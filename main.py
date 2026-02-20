from __future__ import annotations

import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# ============================
# FASTAPI APP (CRITICAL)
# ============================
app = FastAPI(title="Fair Scraper API", version="3.3.0")


# ============================
# Models
# ============================
class ScrapeRequest(BaseModel):
    url: str = Field(..., description="URL of the fair exhibitor catalog")
    countries: List[str] = Field(default_factory=lambda: ["Spain", "Portugal"])
    manufacturers_only: bool = True
    max_pages: int = Field(default=50, ge=1, le=500)
    hits_per_page: int = Field(default=100, ge=10, le=200)
    timeout_ms: int = Field(default=30000, ge=5000, le=120000)


class ExhibitorOut(BaseModel):
    company: str
    country: Optional[str] = None
    fair_profile_url: Optional[str] = None
    what_they_make: Optional[str] = None
    category: Optional[str] = None
    evidence_manufacturer: str
    confidence: str
    sensors_potential: str


class ScrapeResponse(BaseModel):
    status: str
    mode: str
    total_detected: int
    total_espt: int
    total_manufacturers: int
    results: List[ExhibitorOut]
    debug: Dict[str, Any] = Field(default_factory=dict)


# ============================
# Minimal endpoints for sanity
# ============================
@app.get("/health")
def health():
    return {"ok": True}


# ============================
# Helpers
# ============================
def _base(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}".rstrip("/")


def _pick_text(desc: Any) -> str:
    if desc is None:
        return ""
    if isinstance(desc, str):
        return desc
    if isinstance(desc, dict):
        for k in ("es", "en", "pt"):
            v = desc.get(k)
            if isinstance(v, str) and v.strip():
                return v
        for v in desc.values():
            if isinstance(v, str) and v.strip():
                return v
    return ""


def _manufacturer_evidence(text: str) -> tuple[bool, str, str]:
    """
    Returns: (is_manu, evidence, confidence)
    """
    t = (text or "").lower()

    pos = [
        r"\bfabricante(s)?\b",
        r"\bfabricaci[oó]n\b",
        r"\bproducci[oó]n\b",
        r"\bmanufacturer(s)?\b",
        r"\bmanufacturing\b",
        r"\bin[-\s]?house\s+production\b",
        r"\bown\s+production\b",
        r"\boem\b",
        r"\bdiseñ(amos|a|an)\s+y\s+(fabricamos|producimos)\b",
        r"\bwe\s+design\s+and\s+(manufacture|produce)\b",
        r"\bwe\s+manufacture\b",
    ]
    neg = [
        r"\bdistribuidor(a|es)?\b",
        r"\breseller\b",
        r"\bintegrador(a|es)?\b",
        r"\bconsultor[ií]a\b",
        r"\bservicios\b",
    ]

    pos_hit = next((p for p in pos if re.search(p, t, re.IGNORECASE)), None)
    neg_hit = next((n for n in neg if re.search(n, t, re.IGNORECASE)), None)

    if pos_hit and not neg_hit:
        return True, f"Keyword match: {pos_hit}", "Alta"
    if pos_hit and neg_hit:
        return False, f"Mixed signals (pos:{pos_hit}, neg:{neg_hit})", "Baja"
    return False, "No explicit manufacturing evidence", "Baja"


def _sensors_potential(categories: List[str], text: str) -> str:
    blob = (" ".join(categories) + " " + (text or "")).lower()
    high = ["rfid", "vision", "sensor", "automat", "robot", "agv", "intralog", "track", "trazab", "iot"]
    if any(k in blob for k in high):
        return "Alto"
    mid = ["software", "wms", "tms", "scm", "erp"]
    if any(k in blob for k in mid):
        return "Medio"
    return "Bajo"


# ============================
# Core scrape (ALGOLIA DIRECT)
# ============================
ALGOLIA_HOST_RE = re.compile(r"https://([a-z0-9\-]+)\.algolia\.net", re.IGNORECASE)
ALGOLIA_APPID_RE = re.compile(r"\b([A-Z0-9]{8,12})\b")
ALGOLIA_APIKEY_RE = re.compile(r"\b([a-zA-Z0-9]{24,64})\b")
INDEX_RE = re.compile(r"\bstands_\d{8,12}\b")
CONTAINERID_RE = re.compile(r"\bcontainerId\b[^0-9]{0,10}(\d{3,6})\b", re.IGNORECASE)


async def _fetch_html(url: str, timeout_ms: int) -> str:
    async with httpx.AsyncClient(timeout=timeout_ms / 1000.0, follow_redirects=True) as client:
        r = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        return r.text


def _extract_algolia_config(html: str) -> dict:
    """
    Best-effort extraction from HTML/inline scripts.
    """
    # indexName
    idx = INDEX_RE.search(html)
    index_name = idx.group(0) if idx else None

    # containerId
    m = CONTAINERID_RE.search(html)
    container_id = int(m.group(1)) if m else None

    # AppId + ApiKey (best-effort; often present in inline config)
    # We do a slightly targeted search near "algolia" words
    chunk = html
    if "algolia" in html.lower():
        # take a window around the first occurrence to reduce false positives
        pos = html.lower().find("algolia")
        chunk = html[max(0, pos - 20000): pos + 20000]

    app_id = None
    api_key = None

    # Heuristic: look for patterns like appId:"XXXX", apiKey:"YYYY"
    m_app = re.search(r'appId["\']?\s*[:=]\s*["\']([A-Z0-9]{8,12})["\']', chunk)
    if m_app:
        app_id = m_app.group(1)
    m_key = re.search(r'apiKey["\']?\s*[:=]\s*["\']([a-zA-Z0-9]{24,64})["\']', chunk)
    if m_key:
        api_key = m_key.group(1)

    # fallback: broad regex (can false positive; only if targeted search failed)
    if not app_id:
        cand = ALGOLIA_APPID_RE.findall(chunk)
        # app id is usually uppercase+digits; take first
        app_id = cand[0] if cand else None
    if not api_key:
        cand = ALGOLIA_APIKEY_RE.findall(chunk)
        api_key = cand[0] if cand else None

    return {
        "index_name": index_name,
        "container_id": container_id,
        "app_id": app_id,
        "api_key": api_key,
    }


async def _algolia_query(
    app_id: str,
    api_key: str,
    index_name: str,
    container_id: int,
    countries: List[str],
    page: int,
    hits_per_page: int,
    timeout_ms: int,
) -> dict:
    url = f"https://{app_id}-dsn.algolia.net/1/indexes/*/queries"
    headers = {
        "X-Algolia-API-Key": api_key,
        "X-Algolia-Application-Id": app_id,
        "Content-Type": "application/json",
    }

    # facetFilters: country:Spain OR country:Portugal
    facet_filters = [f"country:{c}" for c in countries]

    body = {
        "requests": [
            {
                "indexName": index_name,
                "params": httpx.QueryParams(
                    {
                        "query": "",
                        "page": str(page),
                        "hitsPerPage": str(hits_per_page),
                        "facets": "categories.name,country",
                        "maxValuesPerFacet": "100",
                        "filters": f"(containerId: {container_id})",
                        # OR filter for countries -> Algolia expects nested arrays for OR,
                        # but many Easyfairs configs accept comma-separated facetFilters.
                        "facetFilters": ",".join(facet_filters),
                    }
                ).__str__(),
            }
        ]
    }

    async with httpx.AsyncClient(timeout=timeout_ms / 1000.0, follow_redirects=True) as client:
        r = await client.post(url, headers=headers, json=body)
        r.raise_for_status()
        return r.json()


def _normalize_hit(hit: dict, base: str) -> ExhibitorOut:
    name = hit.get("name") or "Unknown"
    country = hit.get("country")  # often present in Algolia hits
    desc = _pick_text(hit.get("description"))
    categories = []
    for c in hit.get("categories") or []:
        nm = c.get("name")
        categories.append(_pick_text(nm))

    # profile url in Easyfairs often present as "slug" somewhere; if not, build from objectID is impossible
    # We will try to detect a likely path in hit if available
    fair_profile_url = None
    for k in ("url", "publicUrl", "profileUrl", "standUrl", "link"):
        v = hit.get(k)
        if isinstance(v, str) and v.startswith("http"):
            fair_profile_url = v
            break
    if not fair_profile_url:
        # sometimes it's relative
        v = hit.get("slug")
        if isinstance(v, str) and v:
            fair_profile_url = f"{base}/es/exhibitors/{v}"

    is_manu, evidence, conf = _manufacturer_evidence(desc)
    pot = _sensors_potential(categories, desc)

    # what they make: a compact extraction from products if any, else first sentence of desc
    products = hit.get("products") or []
    prod_names = []
    for p in products[:5]:
        nm = p.get("name")
        prod_names.append(_pick_text(nm))
    what = ", ".join([x for x in prod_names if x]) if prod_names else (desc.split("\n")[0][:180] if desc else None)

    cat = categories[0] if categories else None

    return ExhibitorOut(
        company=name,
        country=country,
        fair_profile_url=fair_profile_url,
        what_they_make=what,
        category=cat,
        evidence_manufacturer=evidence,
        confidence=conf,
        sensors_potential=pot,
    )


# ============================
# API endpoint
# ============================
@app.post("/scrape", response_model=ScrapeResponse)
async def scrape(req: ScrapeRequest):
    base = _base(req.url)

    # 1) Fetch HTML and extract config
    try:
        html = await _fetch_html(req.url, req.timeout_ms)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to fetch URL: {e}")

    cfg = _extract_algolia_config(html)
    debug: Dict[str, Any] = {"base": base, "detected": cfg}

    # If no Algolia config found, fail clearly (don’t pretend success)
    if not cfg.get("index_name") or not cfg.get("container_id") or not cfg.get("app_id") or not cfg.get("api_key"):
        raise HTTPException(
            status_code=422,
            detail={
                "error": "Algolia/Easyfairs config not detected from HTML. Site may load config via JS bundles or block bots.",
                "detected": cfg,
                "hint": "Try the full exhibitors URL with query params (page/sort) or allow the site in Render (bot protections).",
            },
        )

    # 2) Iterate pages
    all_hits: List[dict] = []
    total_pages_seen: Optional[int] = None

    for page in range(req.max_pages):
        data = await _algolia_query(
            app_id=cfg["app_id"],
            api_key=cfg["api_key"],
            index_name=cfg["index_name"],
            container_id=cfg["container_id"],
            countries=req.countries,
            page=page,
            hits_per_page=req.hits_per_page,
            timeout_ms=req.timeout_ms,
        )
        results = data.get("results") or []
        if not results:
            break

        r0 = results[0]
        hits = r0.get("hits") or []
        if total_pages_seen is None:
            total_pages_seen = r0.get("nbPages")

        if not hits:
            break

        all_hits.extend(hits)

        # stop when last page reached
        nb_pages = r0.get("nbPages")
        if isinstance(nb_pages, int) and page >= nb_pages - 1:
            break

    # 3) Normalize + filter manufacturers if requested
    normalized: List[ExhibitorOut] = []
    manu_count = 0

    for h in all_hits:
        item = _normalize_hit(h, base)
        is_manu = item.confidence == "Alta" and "No explicit" not in item.evidence_manufacturer
        if req.manufacturers_only:
            if is_manu:
                normalized.append(item)
                manu_count += 1
        else:
            normalized.append(item)
            if is_manu:
                manu_count += 1

    # total_espt = hits already filtered by country facetFilters, but keep metric
    total_espt = len(all_hits)

    return ScrapeResponse(
        status="ok",
        mode="algolia",
        total_detected=len(all_hits),
        total_espt=total_espt,
        total_manufacturers=manu_count,
        results=normalized,
        debug=debug,
    )
