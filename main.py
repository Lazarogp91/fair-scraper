import json
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

app = FastAPI(title="Fair Scraper API", version="3.2.0")


class ScrapeRequest(BaseModel):
    url: str
    countries: list[str] = Field(default_factory=lambda: ["Spain", "Portugal"])
    manufacturers_only: bool = True
    max_pages: int = 50
    max_exhibitors: int = 2000


# -------------------------
# Helpers
# -------------------------

def clean_text(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "").strip())


def want_country(country: str, allow: list[str]) -> bool:
    if not country:
        return False
    allow_norm = {a.strip().lower() for a in allow}
    return country.strip().lower() in allow_norm


def is_manufacturer_strict(text: str) -> Tuple[bool, str, str]:
    """
    Devuelve: (is_manufacturer, confidence, evidence_snippet)
    Estricto: si no hay evidencia explícita -> NO fabricante.
    """
    raw = text or ""
    t = raw.lower()

    positive_patterns = [
        r"\bfabricante\b",
        r"\bfabricación\b",
        r"\bfabricacion\b",
        r"\bmanufacturer\b",
        r"\bmanufacturing\b",
        r"\bproducción\b",
        r"\bproduccion\b",
        r"\bproduction\b",
        r"\boem\b",
        r"\bdiseñamos y producimos\b",
        r"\bdesarrolla y fabrica\b",
        r"\bdiseña y fabrica\b",
        r"\bdesigns? and manufactures?\b",
        r"\bwe manufacture\b",
        r"\bown production\b",
        r"\bplanta de producción\b",
        r"\bplanta de produccion\b",
    ]

    negative_patterns = [
        r"\bintegrador(?:es)?\b",
        r"\bintegración\b",
        r"\bintegracion\b",
        r"\bconsultor(?:ía|ia)\b",
        r"\bconsulting\b",
        r"\bdistribuidor(?:es)?\b",
        r"\bdistribución\b",
        r"\bdistribucion\b",
        r"\breseller\b",
        r"\bservices?\b",
        r"\bservicios\b",
        r"\bpartner\b",
    ]

    # Si se declara integrador/servicios/distribuidor -> NO fabricante
    if any(re.search(p, t) for p in negative_patterns):
        return False, "Baja", ""

    # Evidencia positiva explícita -> fabricante
    for p in positive_patterns:
        m = re.search(p, t)
        if m:
            start = max(0, m.start() - 70)
            end = min(len(raw), m.end() + 140)
            snippet = clean_text(raw[start:end])
            return True, "Alta", snippet

    return False, "Baja", ""


def extract_container_id_from_str(s: str) -> Optional[int]:
    if not s:
        return None
    m = re.search(r"containerId\s*:\s*(\d+)", s)
    if m:
        return int(m.group(1))
    m = re.search(r"containerId%3A\+(\d+)", s)  # URL-encoded
    if m:
        return int(m.group(1))
    return None


# -------------------------
# Algolia detection (support /query and /queries)
# -------------------------

def parse_algolia_multiqueries_body(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Multi-queries típico:
    {
      "requests":[
        {"indexName":"stands","params":"...filters=(containerId:+2653)...&page=0..."}
      ]
    }
    """
    requests_arr = body.get("requests")
    if not isinstance(requests_arr, list) or not requests_arr:
        return {}

    best = None
    best_score = -1
    for req in requests_arr:
        if not isinstance(req, dict):
            continue
        idx = req.get("indexName") or ""
        params = req.get("params") or ""
        score = 0
        if isinstance(idx, str) and "stands" in idx.lower():
            score += 10
        if isinstance(params, str) and ("containerId" in params):
            score += 5
        if score > best_score:
            best_score = score
            best = req

    if not best:
        return {}

    idx = best.get("indexName")
    params = best.get("params", "")
    container_id = extract_container_id_from_str(params if isinstance(params, str) else "")

    if not idx or not container_id:
        return {}
    return {"indexName": idx, "containerId": container_id}


def detect_algolia_from_network(url: str, timeout_ms: int = 20000) -> Dict[str, Any]:
    """
    Abre la web y captura requests a Algolia:
      - /1/indexes/<index>/query
      - /1/indexes/*/queries  (multi-queries, que es el caso de esta feria)
    """
    result: Dict[str, Any] = {}
    captured_score = -1

    def consider(app_id: str, api_key: str, index_name: str, container_id: Optional[int], score: int):
        nonlocal result, captured_score
        if not app_id or not api_key or not index_name or not container_id:
            return
        if score > captured_score:
            captured_score = score
            result = {
                "appId": app_id,
                "apiKey": api_key,
                "indexName": index_name,
                "containerId": int(container_id),
            }

    def on_request(req):
        try:
            if req.method != "POST":
                return
            u = req.url
            if "algolia.net/1/indexes/" not in u:
                return

            headers = {k.lower(): v for k, v in (req.headers or {}).items()}
            api_key = headers.get("x-algolia-api-key", "")
            app_id = headers.get("x-algolia-application-id", "")

            # appId fallback desde host si no viene en header
            host = urlparse(u).netloc
            if not app_id and "-" in host:
                app_id = host.split("-")[0].upper()

            post_data = req.post_data or ""
            body = {}
            try:
                body = json.loads(post_data) if post_data else {}
            except Exception:
                body = {}

            # Caso 1: /query (indexName está en la URL)
            if u.endswith("/query"):
                m = re.search(r"/1/indexes/([^/]+)/query$", u)
                if not m:
                    return
                index_name = m.group(1)
                container_id = None

                # En /query puede venir filters o params
                if isinstance(body, dict):
                    container_id = extract_container_id_from_str(str(body.get("filters") or "")) or \
                                   extract_container_id_from_str(str(body.get("params") or ""))

                score = 5
                if "stands" in index_name.lower():
                    score += 10
                if container_id:
                    score += 5
                if api_key:
                    score += 2

                consider(app_id, api_key, index_name, container_id, score)
                return

            # Caso 2: /queries (multi-queries). indexName va dentro del body.requests[]
            if u.endswith("/queries"):
                parsed = parse_algolia_multiqueries_body(body if isinstance(body, dict) else {})
                if not parsed:
                    return

                index_name = parsed["indexName"]
                container_id = parsed["containerId"]

                score = 8
                if "stands" in index_name.lower():
                    score += 10
                if container_id:
                    score += 5
                if api_key:
                    score += 2

                consider(app_id, api_key, index_name, container_id, score)
                return

        except Exception:
            return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-dev-shm-usage"])
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="es-ES",
        )
        page = context.new_page()
        page.on("request", on_request)

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            # damos tiempo a que dispare el JS del catálogo
            page.wait_for_timeout(timeout_ms)
        except PWTimeoutError:
            pass
        finally:
            try:
                browser.close()
            except Exception:
                pass

    # validación mínima
    if not result.get("appId") or not result.get("apiKey") or not result.get("indexName") or not result.get("containerId"):
        return {}
    return result


def algolia_query(app_id: str, api_key: str, index_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"https://{app_id}-dsn.algolia.net/1/indexes/{index_name}/query"
    headers = {
        "X-Algolia-Application-Id": app_id,
        "X-Algolia-API-Key": api_key,
        "Content-Type": "application/json",
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Algolia error {r.status_code}: {r.text[:300]}")
    return r.json()


def scrape_via_algolia(req: ScrapeRequest, conf: Dict[str, Any]) -> Dict[str, Any]:
    app_id = conf["appId"]
    api_key = conf["apiKey"]
    index_name = conf["indexName"]
    container_id = conf["containerId"]

    allow = req.countries[:]
    exhibitors: List[Dict[str, Any]] = []

    page_num = 0
    nb_pages = 1
    hits_per_page = 15

    while page_num < nb_pages and page_num < req.max_pages:
        payload: Dict[str, Any] = {
            "query": "",
            "page": page_num,
            "hitsPerPage": hits_per_page,
            "facets": ["country", "categories.name"],
            "filters": f"(containerId: {container_id})",
        }

        # Filtrado por ES/PT en Algolia si el campo country está disponible
        if allow:
            payload["facetFilters"] = [[f"country:{c}" for c in allow]]

        data = algolia_query(app_id, api_key, index_name, payload)
        nb_pages = int(data.get("nbPages", 0) or 0)
        hits = data.get("hits", []) or []
        if not hits:
            break

        for h in hits:
            name = clean_text(h.get("name") or "")
            if not name:
                continue

            country = clean_text(h.get("country") or "")
            if allow and not want_country(country, allow):
                continue

            # description
            desc = ""
            d = h.get("description") or {}
            if isinstance(d, dict):
                desc = clean_text(d.get("es") or d.get("en") or "")
            else:
                desc = clean_text(str(d))

            # categorías
            cats = []
            for c in (h.get("categories") or []):
                nm = c.get("name") if isinstance(c, dict) else None
                if isinstance(nm, dict):
                    cats.append(clean_text(nm.get("es") or nm.get("en") or ""))
                elif isinstance(nm, str):
                    cats.append(clean_text(nm))
            cats = [x for x in cats if x]

            # fabricante estricto
            is_manu, conf_lvl, evidence = is_manufacturer_strict(desc)
            if req.manufacturers_only and not is_manu:
                continue

            exhibitors.append(
                {
                    "company_name": name,
                    "country": country,
                    "standNumber": clean_text(h.get("standNumber") or ""),
                    "categories": cats[:10],
                    "description": desc[:700],
                    "manufacturer_confidence": conf_lvl,
                    "manufacturer_evidence": evidence,
                }
            )

            if len(exhibitors) >= req.max_exhibitors:
                break

        if len(exhibitors) >= req.max_exhibitors:
            break

        page_num += 1

    return {
        "mode": "algolia",
        "algolia": {"indexName": index_name, "containerId": container_id},
        "total": len(exhibitors),
        "exhibitors": exhibitors,
    }


@app.post("/scrape")
def scrape(req: ScrapeRequest):
    if not req.url.startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="Invalid url")

    alg_conf = detect_algolia_from_network(req.url)
    if not alg_conf:
        raise HTTPException(
            status_code=422,
            detail=(
                "No se ha detectado un catálogo tipo Algolia/Easyfairs desde la URL. "
                "La web puede requerir login, bloquear scraping, o usar una API privada distinta."
            ),
        )

    out = scrape_via_algolia(req, alg_conf)
    out["source_url"] = req.url
    return out
