import re
import json
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from playwright.sync_api import sync_playwright


# -----------------------------
# FastAPI
# -----------------------------
app = FastAPI(title="Fair Scraper API", version="3.2.0")


# -----------------------------
# Request/Response models
# -----------------------------
class ScrapeRequest(BaseModel):
    url: str = Field(..., description="URL del catálogo/listado de expositores")
    countries: List[str] = Field(default_factory=lambda: ["Spain", "Portugal"])
    manufacturers_only: bool = True
    max_pages: int = 20
    timeout_ms: int = 25000
    deep_profile: bool = False  # reservado para ampliar (visitar fichas individuales)
    debug: bool = False


class CompanyRow(BaseModel):
    company: str
    country: str
    fair_profile_url: str
    web: Optional[str] = None
    what_they_make: str
    category: str
    evidence_manufacturer: str
    confidence: str
    sensors_potential: str


class ScrapeResponse(BaseModel):
    status: str
    mode: str
    total_detected: int
    total_espt: int
    total_manufacturers: int
    results: List[CompanyRow]
    debug: Dict[str, Any] = Field(default_factory=dict)


# -----------------------------
# Helpers
# -----------------------------
MANUFACTURER_TERMS = [
    r"\bfabricante\b",
    r"\bmanufacturer\b",
    r"\bfabricaci[oó]n\b",
    r"\bproducci[oó]n\b",
    r"\bOEM\b",
    r"\bdiseñamos y producimos\b",
    r"\bdesign(?:s|ed)? and manufacture(?:s|d)?\b",
    r"\bin-house production\b",
    r"\bwe manufacture\b",
    r"\bwe design\b.*\bmanufacture\b",
]

NON_MANUFACTURER_HINTS = [
    r"\bdistribuidor\b",
    r"\bdistributor\b",
    r"\bintegrador\b",
    r"\bintegrator\b",
    r"\bconsultor[ií]a\b",
    r"\bconsulting\b",
    r"\bservicios\b",
    r"\bservices\b",
]


def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def looks_like_manufacturer(text: str) -> Tuple[bool, str, str]:
    """
    Returns: (is_manufacturer, evidence, confidence)
    """
    t = (text or "").lower()

    # If explicit non-manufacturer signals are strong and no manufacturer terms, treat as not confirmed
    has_non = any(re.search(p, t, flags=re.IGNORECASE) for p in NON_MANUFACTURER_HINTS)
    hits = [p for p in MANUFACTURER_TERMS if re.search(p, t, flags=re.IGNORECASE)]

    if hits:
        # Evidence is the first matched pattern (humanized a bit)
        ev = "Coincide con términos de fabricación (p.ej. 'fabricante/production/OEM')."
        conf = "Alta" if len(hits) >= 2 else "Media"
        return True, ev, conf

    if has_non:
        return False, "Se detectan señales de distribuidor/integrador/servicios sin fabricación explícita.", "Media"

    return False, "No hay evidencia textual suficiente de fabricación.", "Baja"


def sensors_potential_from_categories(categories: List[str], description: str) -> str:
    blob = " ".join(categories + [description or ""]).lower()
    high_terms = ["rfid", "vision", "cámara", "camera", "sensor", "sensórica", "i o t", "iot", "automatización", "robot", "agv", "conveyor", "transportador"]
    medium_terms = ["almacenaje", "estanter", "intralog", "mantenimiento", "seguridad", "protección", "barrier", "barrera"]

    if any(t in blob for t in high_terms):
        return "Alto"
    if any(t in blob for t in medium_terms):
        return "Medio"
    return "Bajo"


def category_from_categories(categories: List[str]) -> str:
    if not categories:
        return "No confirmado"
    # Return the most informative (first) category
    return categories[0]


def extract_what_they_make(description: str, products: List[str]) -> str:
    if products:
        # Return up to 3 products
        return "; ".join(products[:3])
    if description:
        d = normalize_space(description)
        return d[:180] + ("…" if len(d) > 180 else "")
    return "No visible"


def absolutize_profile_url(base_url: str, maybe_relative: str) -> str:
    if not maybe_relative:
        return base_url
    if maybe_relative.startswith("http://") or maybe_relative.startswith("https://"):
        return maybe_relative
    return urllib.parse.urljoin(base_url, maybe_relative)


# -----------------------------
# Algolia capture via Playwright
# -----------------------------
def capture_algolia_queries(url: str, timeout_ms: int, debug: bool = False) -> Dict[str, Any]:
    """
    Launches a browser, navigates to URL, and captures Algolia /queries call details.
    Returns dict with {app_id, api_key, endpoint, requests_payload_sample}
    """
    captured: Dict[str, Any] = {"calls": []}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1365, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            locale="es-ES",
        )
        page = context.new_page()

        def on_request(req):
            try:
                u = req.url
                if "/1/indexes/" in u and u.endswith("/queries"):
                    headers = {k.lower(): v for k, v in req.headers.items()}
                    post_data = req.post_data or ""
                    captured["calls"].append(
                        {
                            "url": u,
                            "headers": {
                                "x-algolia-application-id": headers.get("x-algolia-application-id"),
                                "x-algolia-api-key": headers.get("x-algolia-api-key"),
                                "content-type": headers.get("content-type"),
                            },
                            "post_data": post_data,
                        }
                    )
            except Exception:
                # never fail capture on handler
                pass

        page.on("request", on_request)

        # Navigate + wait for network activity
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

        # Many exhibitor pages trigger Algolia after some JS; wait a bit for XHR
        page.wait_for_timeout(min(8000, timeout_ms))

        # Also try to provoke lazy loads / filters
        try:
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(1500)
        except Exception:
            pass

        context.close()
        browser.close()

    if not captured["calls"]:
        raise HTTPException(
            status_code=422,
            detail=(
                "Algolia/Easyfairs config no detectada desde HTML/Network. "
                "Puede haber bloqueo anti-bot, login requerido, o el catálogo usa otra API."
            ),
        )

    # Pick the first valid call that has app_id + api_key
    for c in captured["calls"]:
        h = c.get("headers", {})
        if h.get("x-algolia-application-id") and h.get("x-algolia-api-key"):
            endpoint = c["url"]
            app_id = h["x-algolia-application-id"]
            api_key = h["x-algolia-api-key"]

            # Parse payload as JSON
            try:
                payload = json.loads(c.get("post_data") or "{}")
            except Exception:
                payload = {}

            return {
                "endpoint": endpoint,
                "app_id": app_id,
                "api_key": api_key,
                "payload": payload,
                "all_calls_count": len(captured["calls"]),
                "debug_calls": captured["calls"] if debug else [],
            }

    raise HTTPException(
        status_code=422,
        detail="Se detectó /queries pero no se pudieron extraer x-algolia-application-id / x-algolia-api-key.",
    )


def algolia_paginate(
    endpoint: str,
    app_id: str,
    api_key: str,
    base_payload: Dict[str, Any],
    max_pages: int,
    timeout_s: int,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Replays Algolia queries, paginating by modifying `params` (page=...).
    Returns all hits aggregated.
    """
    headers = {
        "x-algolia-application-id": app_id,
        "x-algolia-api-key": api_key,
        "content-type": "application/json",
        "accept": "application/json",
    }

    requests_list = (base_payload or {}).get("requests") or []
    if not requests_list:
        raise HTTPException(status_code=422, detail="Payload Algolia sin 'requests'.")

    # We'll paginate the first request that contains 'params'
    first_req = requests_list[0]
    index_name = first_req.get("indexName")
    params = first_req.get("params")

    if not index_name or not params:
        raise HTTPException(status_code=422, detail="No se detectan indexName/params en la request Algolia.")

    # Find nbPages from page 0
    all_hits: List[Dict[str, Any]] = []
    nb_pages: Optional[int] = None

    def set_page_in_params(params_str: str, page_num: int) -> str:
        # params is an URL-encoded querystring
        q = urllib.parse.parse_qs(params_str, keep_blank_values=True)
        q["page"] = [str(page_num)]
        # reconstruct (doseq)
        return urllib.parse.urlencode(q, doseq=True)

    for page_num in range(0, max_pages):
        new_params = set_page_in_params(params, page_num)
        payload = {"requests": [{"indexName": index_name, "params": new_params}]}

        r = requests.post(endpoint, headers=headers, json=payload, timeout=timeout_s)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Algolia HTTP {r.status_code}: {r.text[:300]}")

        data = r.json()
        results = (data.get("results") or [])
        if not results:
            break

        res0 = results[0]
        hits = res0.get("hits") or []
        all_hits.extend(hits)

        if nb_pages is None:
            nb_pages = res0.get("nbPages")

        # Stop if we've reached last page per Algolia
        if nb_pages is not None and (page_num + 1) >= int(nb_pages):
            break

        # gentle pacing
        time.sleep(0.2)

    if debug:
        return all_hits
    return all_hits


# -----------------------------
# Main endpoint
# -----------------------------
@app.post("/scrape", response_model=ScrapeResponse)
def scrape(req: ScrapeRequest) -> ScrapeResponse:
    try:
        # 1) Capture algolia config by sniffing real network calls
        cap = capture_algolia_queries(req.url, timeout_ms=req.timeout_ms, debug=req.debug)

        # 2) Replay algolia with pagination
        hits = algolia_paginate(
            endpoint=cap["endpoint"],
            app_id=cap["app_id"],
            api_key=cap["api_key"],
            base_payload=cap["payload"],
            max_pages=req.max_pages,
            timeout_s=max(10, req.timeout_ms // 1000),
            debug=req.debug,
        )

        # 3) Normalize + filter ES/PT + manufacturer
        countries_norm = set([c.strip().lower() for c in req.countries])
        results: List[CompanyRow] = []

        for h in hits:
            name = h.get("name") or ""
            country = h.get("country") or ""
            country_l = str(country).strip().lower()

            # Filter countries
            if country_l and country_l not in countries_norm:
                continue

            # Description can be localized dict: {en:..., es:...}
            desc = h.get("description")
            if isinstance(desc, dict):
                description = desc.get("es") or desc.get("en") or ""
            else:
                description = desc or ""

            categories_raw = h.get("categories") or []
            categories: List[str] = []
            for c in categories_raw:
                nm = (c.get("name") or {})
                if isinstance(nm, dict):
                    categories.append(nm.get("es") or nm.get("en") or "")
                else:
                    categories.append(str(nm))
            categories = [normalize_space(x) for x in categories if normalize_space(x)]

            products_raw = h.get("products") or []
            products: List[str] = []
            for p in products_raw:
                nm = (p.get("name") or {})
                if isinstance(nm, dict):
                    products.append(nm.get("es") or nm.get("en") or "")
                else:
                    products.append(str(nm))
            products = [normalize_space(x) for x in products if normalize_space(x)]

            # Build a blob for manufacturer detection
            blob = " ".join([name, description] + categories + products)
            is_manu, evidence, confidence = looks_like_manufacturer(blob)

            if req.manufacturers_only and not is_manu:
                continue

            # Profile URL: some sites expose a relative path; if not, keep empty
            fair_profile_url = h.get("url") or h.get("profileUrl") or ""
            if not fair_profile_url:
                # If we only have objectID, some Easyfairs builds url elsewhere; keep empty but not invent
                fair_profile_url = ""

            fair_profile_url = absolutize_profile_url(req.url, fair_profile_url)

            row = CompanyRow(
                company=normalize_space(name) or "Sin nombre",
                country=country or "No confirmado",
                fair_profile_url=fair_profile_url or req.url,
                web=None,  # no inventar: si no viene, no se rellena
                what_they_make=extract_what_they_make(description, products),
                category=category_from_categories(categories),
                evidence_manufacturer=evidence,
                confidence=confidence,
                sensors_potential=sensors_potential_from_categories(categories, description),
            )
            results.append(row)

        total_detected = len(hits)
        total_espt = len(results) if req.manufacturers_only else len([h for h in hits if (h.get("country") or "").lower() in countries_norm])
        total_manufacturers = len(results)

        dbg = {}
        if req.debug:
            dbg = {
                "algolia_endpoint": cap["endpoint"],
                "algolia_app_id_present": bool(cap.get("app_id")),
                "algolia_calls_seen": cap.get("all_calls_count"),
                "sample_payload_keys": list((cap.get("payload") or {}).keys()),
            }

        return ScrapeResponse(
            status="ok",
            mode="algolia",
            total_detected=total_detected,
            total_espt=total_espt,
            total_manufacturers=total_manufacturers,
            results=results,
            debug=dbg,
        )

    except HTTPException:
        raise
    except Exception as e:
        # IMPORTANT: no 500 silencioso sin explicación
        raise HTTPException(status_code=500, detail=f"Internal error: {type(e).__name__}: {str(e)[:250]}")
