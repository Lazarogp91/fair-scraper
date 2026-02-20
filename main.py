from __future__ import annotations

import re
import json
import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, HttpUrl
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

app = FastAPI(title="Fair Scraper API", version="3.2.0")


# -----------------------------
# Models
# -----------------------------
class ScrapeRequest(BaseModel):
    url: HttpUrl
    countries: List[str] = Field(default_factory=lambda: ["Spain", "Portugal"])
    manufacturers_only: bool = True
    max_pages: int = 50
    deep_profile: bool = True  # kept for compatibility; in Algolia mode, hits already contain rich fields
    sniff_timeout_ms: int = 25000  # time to wait for Algolia network call


class CompanyOut(BaseModel):
    company: str
    country: Optional[str] = None
    fair_profile_url: Optional[str] = None
    website: Optional[str] = None
    what_they_make: Optional[str] = None
    category: Optional[str] = None
    evidence: Optional[str] = None
    confidence: str = "Baja"
    sensor_potential: str = "Medio"


class ScrapeResponse(BaseModel):
    status: str
    mode: str
    total_detected: int
    total_espt: int
    total_manufacturers: int
    results: List[CompanyOut]
    debug: Dict[str, Any] = Field(default_factory=dict)


# -----------------------------
# Helpers
# -----------------------------
MANUFACTURER_POSITIVE = [
    "fabricante", "manufacturer", "manufacturing", "fabricación", "producción",
    "oem", "diseñamos y producimos", "diseña y fabrica", "designs and manufactures",
    "develops and manufactures", "in-house production", "producción propia",
]
MANUFACTURER_NEGATIVE = [
    "distribuidor", "distribución", "reseller", "dealer",
    "consultoría", "consulting", "servicios", "services",
    "integrador", "system integrator", "integration", "implementación", "implementation",
]

SENSOR_HIGH = [
    "rfid", "vision", "visión", "sensor", "sensores", "safety", "seguridad",
    "automatización", "robot", "agv", "amr", "conveyor", "transportador",
    "iot", "iiot", "track", "tracking", "trazabilidad",
]
SENSOR_LOW = ["consultoría", "consulting", "formación", "training", "marketing"]


def normalize_exhibitors_url(url: str) -> str:
    # Many Easyfairs exhibitor listings work better with sorting/pagination params.
    # Keep original if it already has query.
    if "?" in url:
        return url
    # If it's exactly /exhibitors/ without query, add a common sort param
    if url.rstrip("/").endswith("/exhibitors"):
        return url.rstrip("/") + "/?stands%5BsortBy%5D=stands_relevance"
    return url


def looks_like_manufacturer(text: str) -> Tuple[bool, str, str]:
    """
    Returns: (is_manu, evidence, confidence)
    """
    t = (text or "").lower()

    pos_hits = [k for k in MANUFACTURER_POSITIVE if k in t]
    neg_hits = [k for k in MANUFACTURER_NEGATIVE if k in t]

    if pos_hits and not neg_hits:
        return True, f"Indicadores fabricante: {', '.join(pos_hits[:3])}", "Alta"
    if pos_hits and neg_hits:
        # conflicting signals
        return True, f"Mixto (fabricante + servicios): {pos_hits[0]} / {neg_hits[0]}", "Media"
    if not pos_hits and neg_hits:
        return False, f"Indicadores NO fabricante: {', '.join(neg_hits[:2])}", "Alta"
    return False, "Sin evidencia textual suficiente", "Baja"


def sensor_potential(text: str) -> str:
    t = (text or "").lower()
    if any(k in t for k in SENSOR_LOW):
        return "Bajo"
    if any(k in t for k in SENSOR_HIGH):
        return "Alto"
    return "Medio"


def pick_lang(d: Any, preferred=("es", "en")) -> Optional[str]:
    # Easyfairs often uses { "es": "...", "en": "..." }
    if isinstance(d, dict):
        for p in preferred:
            v = d.get(p)
            if isinstance(v, str) and v.strip():
                return v.strip()
        # fallback any string
        for v in d.values():
            if isinstance(v, str) and v.strip():
                return v.strip()
    if isinstance(d, str) and d.strip():
        return d.strip()
    return None


def safe_join(*parts: Optional[str], sep: str = " | ") -> str:
    return sep.join([p.strip() for p in parts if isinstance(p, str) and p.strip()])


# -----------------------------
# Algolia sniffing
# -----------------------------
@dataclass
class AlgoliaCapture:
    app_id: Optional[str] = None
    api_key: Optional[str] = None
    index_name: Optional[str] = None
    params: Optional[str] = None
    container_id: Optional[int] = None
    source_request_url: Optional[str] = None


ALGOLIA_QUERIES_RE = re.compile(r"algolia\.(net|io)/1/indexes/.*/queries", re.IGNORECASE)


def extract_container_id_from_params(params: str) -> Optional[int]:
    # params contain filters=%28containerId%3A+2653%29 ...
    m = re.search(r"containerId%3A\+(\d+)", params or "")
    if m:
        return int(m.group(1))
    return None


async def sniff_algolia_from_network(target_url: str, timeout_ms: int) -> AlgoliaCapture:
    cap = AlgoliaCapture()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        async def on_request(req):
            try:
                url = req.url
                if not ALGOLIA_QUERIES_RE.search(url):
                    return

                headers = {k.lower(): v for k, v in req.headers.items()}
                app_id = headers.get("x-algolia-application-id")
                api_key = headers.get("x-algolia-api-key")
                if not app_id or not api_key:
                    return

                post = req.post_data
                if not post:
                    return
                data = json.loads(post)

                # Algolia "queries" uses {"requests":[{"indexName":"...","params":"..."}]}
                requests = data.get("requests") or []
                if not requests:
                    return
                first = requests[0]
                index_name = first.get("indexName")
                params = first.get("params")

                if app_id and api_key and index_name and params:
                    cap.app_id = app_id
                    cap.api_key = api_key
                    cap.index_name = index_name
                    cap.params = params
                    cap.container_id = extract_container_id_from_params(params)
                    cap.source_request_url = url
            except Exception:
                return

        page.on("request", on_request)

        try:
            await page.goto(target_url, wait_until="networkidle", timeout=timeout_ms)
        except PWTimeoutError:
            # still may have captured something
            pass

        await context.close()
        await browser.close()

    return cap


# -----------------------------
# Algolia querying
# -----------------------------
def patch_params_for_countries(params: str, countries: List[str]) -> str:
    """
    Ensure facetFilters contains the requested countries (country:Spain, country:Portugal).
    We replace/insert facetFilters=... keeping the rest.
    """
    # Remove existing facetFilters=... (if any)
    params_no_ff = re.sub(r"(?:^|&)facetFilters=[^&]*", "", params or "").strip("&")

    # Build new facetFilters. Algolia expects URL-encoded. We'll insert already encoded.
    # For OR across countries: facetFilters=country%3ASpain%2Ccountry%3APortugal
    encoded = "%2C".join([f"country%3A{httpx.QueryParams({'x': c})['x']}".replace("x=", "") for c in countries])
    # Above is a safe-ish encode for spaces; but countries here are simple.
    ff = f"facetFilters={encoded}"

    # If params already has "facets=" keep.
    if params_no_ff:
        return f"{ff}&{params_no_ff}"
    return ff


async def algolia_fetch_all(
    app_id: str,
    api_key: str,
    index_name: str,
    base_params: str,
    countries: List[str],
    max_pages: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    params = patch_params_for_countries(base_params, countries)

    endpoint = f"https://{app_id}-dsn.algolia.net/1/indexes/*/queries"
    headers = {
        "x-algolia-application-id": app_id,
        "x-algolia-api-key": api_key,
        "content-type": "application/json",
    }

    all_hits: List[Dict[str, Any]] = []
    debug: Dict[str, Any] = {"algolia_endpoint": endpoint, "index_name": index_name}

    async with httpx.AsyncClient(timeout=30) as client:
        page = 0
        nb_pages = None

        while True:
            if page >= max_pages:
                break

            # overwrite page param inside params
            # remove any existing &page=...
            p2 = re.sub(r"(?:^|&)page=\d+", "", params).strip("&")
            p2 = f"{p2}&page={page}"

            body = {"requests": [{"indexName": index_name, "params": p2}]}
            r = await client.post(endpoint, headers=headers, json=body)
            r.raise_for_status()
            js = r.json()

            # Algolia multi-queries returns {"results":[{...}]}
            results = js.get("results") or []
            if not results:
                break
            res0 = results[0]
            hits = res0.get("hits") or []
            all_hits.extend(hits)

            nb_pages = res0.get("nbPages", nb_pages)
            page += 1
            if nb_pages is not None and page >= nb_pages:
                break

    debug["pages_fetched"] = page
    debug["hits_total"] = len(all_hits)
    return all_hits, debug


# -----------------------------
# Normalization + filtering
# -----------------------------
def normalize_hit_to_company(hit: Dict[str, Any], base_domain: str) -> CompanyOut:
    name = hit.get("name") or hit.get("company") or hit.get("title") or "Sin nombre"
    country = hit.get("country") or hit.get("countryCode")  # Easyfairs Algolia often has "country"
    stand_number = hit.get("standNumber")

    desc = pick_lang(hit.get("description")) or ""
    cats = hit.get("categories") or []
    cat_names = []
    for c in cats:
        n = pick_lang((c or {}).get("name"))
        if n:
            cat_names.append(n)
    category = ", ".join(cat_names[:3]) if cat_names else None

    products = hit.get("products") or []
    prod_names = []
    for p in products:
        pn = pick_lang((p or {}).get("name"))
        if pn:
            prod_names.append(pn)

    what_make = None
    if prod_names:
        what_make = ", ".join(prod_names[:8])
    elif desc:
        what_make = (desc[:180] + "…") if len(desc) > 180 else desc

    # profile URL on Easyfairs websites is often /es/exhibitors/<slug>-<id>
    obj_id = hit.get("objectID")
    fair_profile_url = None
    if obj_id:
        # best effort: sometimes also "url" exists
        if isinstance(hit.get("url"), str) and hit["url"].startswith("http"):
            fair_profile_url = hit["url"]
        else:
            fair_profile_url = f"{base_domain.rstrip('/')}/es/exhibitors/{obj_id}"

    # manufacturer inference
    text_for_rules = safe_join(name, desc, category, what_make, stand_number)
    is_manu, evidence, conf = looks_like_manufacturer(text_for_rules)

    return CompanyOut(
        company=name,
        country=country,
        fair_profile_url=fair_profile_url,
        website=hit.get("website") if isinstance(hit.get("website"), str) else None,
        what_they_make=what_make,
        category=category,
        evidence=evidence,
        confidence=conf,
        sensor_potential=sensor_potential(text_for_rules),
    ), is_manu


def country_allowed(country: Optional[str], allowed: List[str]) -> bool:
    if not country:
        return False
    return country.strip().lower() in {a.strip().lower() for a in allowed}


# -----------------------------
# Endpoint
# -----------------------------
@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/scrape", response_model=ScrapeResponse)
async def scrape(req: ScrapeRequest):
    url = normalize_exhibitors_url(str(req.url))

    # derive base domain (scheme + host)
    m = re.match(r"^(https?://[^/]+)", url)
    base_domain = m.group(1) if m else url

    # 1) Try Algolia via network sniffing
    cap = await sniff_algolia_from_network(url, req.sniff_timeout_ms)

    if cap.app_id and cap.api_key and cap.index_name and cap.params:
        hits, dbg = await algolia_fetch_all(
            app_id=cap.app_id,
            api_key=cap.api_key,
            index_name=cap.index_name,
            base_params=cap.params,
            countries=req.countries,
            max_pages=req.max_pages,
        )

        results: List[CompanyOut] = []
        total_espt = 0
        total_manu = 0

        for h in hits:
            co, is_manu = normalize_hit_to_company(h, base_domain)
            if not country_allowed(co.country, req.countries):
                continue
            total_espt += 1
            if req.manufacturers_only and not is_manu:
                continue
            if is_manu:
                total_manu += 1
            results.append(co)

        return ScrapeResponse(
            status="ok",
            mode="algolia_network_sniff",
            total_detected=len(hits),
            total_espt=total_espt,
            total_manufacturers=total_manu if req.manufacturers_only else total_manu,
            results=results,
            debug={
                "sniffed": {
                    "app_id": cap.app_id,
                    "index_name": cap.index_name,
                    "container_id": cap.container_id,
                    "source_request_url": cap.source_request_url,
                },
                **dbg,
            },
        )

    # 2) Fallback: couldn't sniff Algolia
    raise HTTPException(
        status_code=422,
        detail={
            "error": "Algolia/Easyfairs config not detected from network. Site may block bots or use another API.",
            "detected": {
                "index_name": cap.index_name,
                "container_id": cap.container_id,
                "app_id": cap.app_id,
                "api_key": ("present" if cap.api_key else None),
            },
            "hint": "Si en tu navegador ves respuestas tipo Algolia (hits/nbPages/facets), aumenta sniff_timeout_ms o revisa bloqueos anti-bot en Render.",
        },
    )
