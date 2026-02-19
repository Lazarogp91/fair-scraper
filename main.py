import re
from typing import Any, Dict, List, Tuple

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from playwright.sync_api import sync_playwright

app = FastAPI(title="Fair Scraper API", version="3.0.0")


class ScrapeRequest(BaseModel):
    url: str
    countries: list[str] = Field(default_factory=lambda: ["Spain", "Portugal"])
    max_pages: int = 50
    max_exhibitors: int = 2000


# -------------------------------------------------
# UTILIDADES
# -------------------------------------------------

def clean_text(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "").strip())


def is_manufacturer(text: str) -> Tuple[bool, str]:
    """
    Devuelve (True/False, confianza)
    """
    t = (text or "").lower()

    strong_keywords = [
        "fabricante",
        "fabricación",
        "manufactur",
        "desarrolla y fabrica",
        "diseña y fabrica",
        "produce",
        "production",
        "oem"
    ]

    weak_keywords = [
        "diseña",
        "design",
        "industrial solutions",
        "engineering company"
    ]

    negative_keywords = [
        "integrador",
        "consultoría",
        "consulting",
        "software provider",
        "servicios",
        "service provider"
    ]

    if any(n in t for n in negative_keywords):
        return False, "Baja"

    if any(k in t for k in strong_keywords):
        return True, "Alta"

    if any(k in t for k in weak_keywords):
        return True, "Media"

    return False, "Baja"


def want_country(country: str, allowed: list[str]) -> bool:
    if not country:
        return False
    return country.lower() in [c.lower() for c in allowed]


# -------------------------------------------------
# ALGOLIA (Easyfairs)
# -------------------------------------------------

def extract_algolia_config(html: str) -> Dict[str, Any]:
    app_id = re.search(r'algoliaApplicationId["\']?\s*[:=]\s*["\']([A-Z0-9]+)["\']', html)
    api_key = re.search(r'algoliaApiKey["\']?\s*[:=]\s*["\']([^"\']+)["\']', html)
    index_name = re.search(r'indexName["\']?\s*[:=]\s*["\']([^"\']+)["\']', html)
    container_id = re.search(r'containerId["\']?\s*[:=]\s*(\d+)', html)

    if not (app_id and api_key and index_name and container_id):
        return {}

    return {
        "appId": app_id.group(1),
        "apiKey": api_key.group(1),
        "indexName": index_name.group(1),
        "containerId": int(container_id.group(1))
    }


def algolia_query(app_id: str, api_key: str, index: str, payload: dict):
    url = f"https://{app_id}-dsn.algolia.net/1/indexes/{index}/query"
    headers = {
        "X-Algolia-Application-Id": app_id,
        "X-Algolia-API-Key": api_key,
        "Content-Type": "application/json",
    }

    r = requests.post(url, headers=headers, json=payload, timeout=30)

    if r.status_code >= 400:
        raise HTTPException(status_code=500, detail=f"Algolia error {r.status_code}")

    return r.json()


# -------------------------------------------------
# ENDPOINT PRINCIPAL
# -------------------------------------------------

@app.post("/scrape")
def scrape(req: ScrapeRequest):

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(req.url, wait_until="domcontentloaded")
        html = page.content()
        browser.close()

    config = extract_algolia_config(html)

    if not config:
        raise HTTPException(status_code=500, detail="Algolia config not found")

    exhibitors = []
    page_number = 0

    while page_number < req.max_pages:

        payload = {
            "query": "",
            "page": page_number,
            "hitsPerPage": 20,
            "facets": ["country"],
            "filters": f"(containerId: {config['containerId']})"
        }

        data = algolia_query(
            config["appId"],
            config["apiKey"],
            config["indexName"],
            payload
        )

        hits = data.get("hits", [])
        if not hits:
            break

        for h in hits:

            country = clean_text(h.get("country") or "")
            if not want_country(country, req.countries):
                continue

            desc = ""
            d = h.get("description") or {}
            if isinstance(d, dict):
                desc = clean_text(d.get("es") or d.get("en") or "")
            else:
                desc = clean_text(str(d))

            manufacturer, confidence = is_manufacturer(desc)

            if not manufacturer:
                continue

            exhibitors.append({
                "company_name": clean_text(h.get("name") or ""),
                "country": country,
                "description": desc[:500],
                "standNumber": clean_text(h.get("standNumber") or ""),
                "manufacturer_confidence": confidence
            })

            if len(exhibitors) >= req.max_exhibitors:
                break

        if len(exhibitors) >= req.max_exhibitors:
            break

        page_number += 1

    return {
        "source_url": req.url,
        "mode": "algolia_manufacturers_only",
        "total_manufacturers": len(exhibitors),
        "exhibitors": exhibitors
    }
