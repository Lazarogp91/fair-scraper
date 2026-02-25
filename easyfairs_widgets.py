# easyfairs_widgets.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
import re
import requests

# =========================================================
# 1) Mapping dominio -> containerId
#    Añade aquí más ferias Easyfairs si las necesitas
# =========================================================
EASYFAIRS_CONTAINER_MAP: Dict[str, int] = {
    "www.logisticsautomationmadrid.com": 2653,
    "logisticsautomationmadrid.com": 2653,
}

# =========================================================
# 2) Detección y helpers
# =========================================================
def is_easyfairs_supported(event_url: str) -> bool:
    """
    Devuelve True si el dominio está en el mapping.
    """
    try:
        host = (urlparse(event_url).netloc or "").lower()
    except Exception:
        return False
    if not host:
        return False
    return host in EASYFAIRS_CONTAINER_MAP


def get_container_id_for_url(event_url: str) -> Optional[int]:
    try:
        host = (urlparse(event_url).netloc or "").lower()
    except Exception:
        return None
    return EASYFAIRS_CONTAINER_MAP.get(host)

# =========================================================
# 3) Scrape vía endpoint widgets (my.easyfairs.com)
# =========================================================
EASYFAIRS_WIDGETS_ENDPOINT = "https://my.easyfairs.com/widgets/api/stands/?language={lang}"

DEFAULT_HEADERS = {
    "accept": "*/*",
    "content-type": "application/json",
    "origin": "https://www.logisticsautomationmadrid.com",
    "referer": "https://www.logisticsautomationmadrid.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

_url_re = re.compile(r"(https?://[^\s)]+|www\.[^\s)]+)", re.IGNORECASE)


def _extract_website_from_text(text: str) -> str:
    if not text:
        return ""
    m = _url_re.search(text)
    if not m:
        return ""
    u = m.group(0).strip().rstrip(".,;")
    if u.lower().startswith("www."):
        u = "https://" + u
    return u


def _activity_from_hit(hit: Dict[str, Any], lang: str) -> str:
    cats = hit.get("categories") or []
    names: List[str] = []
    for c in cats:
        nm = (c.get("name") or {}).get(lang) or (c.get("name") or {}).get("en") or ""
        nm = (nm or "").strip()
        if nm:
            names.append(nm)

    # dedupe manteniendo orden
    seen = set()
    out: List[str] = []
    for n in names:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return ", ".join(out)


def _build_algolia_filters(container_id: int, country: Optional[str]) -> str:
    base = f"(containerId: {container_id})"
    if country:
        base += f' AND country:"{country}"'
    return base


def fetch_easyfairs_stands_by_countries(
    event_url: str,
    container_id: int,
    countries: List[str],
    lang: str = "es",
    query_seed: str = "a",
    hits_per_page: int = 100,
    timeout_s: int = 25,
    max_pages: Optional[int] = 20,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not countries:
        countries = ["Spain", "Portugal"]

    endpoint = EASYFAIRS_WIDGETS_ENDPOINT.format(lang=lang)

    rows: List[Dict[str, Any]] = []
    hits_reported_by_country: Dict[str, int] = {}
    pages_fetched_by_country: Dict[str, int] = {}

    session = requests.Session()

    for country in countries:
        page = 0
        fetched_pages = 0

        while True:
            if max_pages is not None and fetched_pages >= max_pages:
                break

            payload = [
                {
                    "indexName": "stands_relevance",
                    "params": {
                        "facets": ["categories.name", "country"],
                        "filters": _build_algolia_filters(container_id, country),
                        "highlightPostTag": "__/ais-highlight__",
                        "highlightPreTag": "__ais-highlight__",
                        "hitsPerPage": int(hits_per_page),
                        "maxValuesPerFacet": 100,
                        "page": int(page),
                        "query": str(query_seed),
                    },
                }
            ]

            r = session.post(
                endpoint,
                json=payload,
                headers=DEFAULT_HEADERS,
                timeout=timeout_s,
            )

            if r.status_code >= 400:
                body_preview = (r.text or "")[:800]
                raise RuntimeError(f"Easyfairs widgets HTTP {r.status_code}. Body: {body_preview}")

            data = r.json() or {}
            results = data.get("results") or []
            if not results:
                break

            block = results[0]
            nb_hits = int(block.get("nbHits") or 0)
            nb_pages = int(block.get("nbPages") or 0)
            hits_reported_by_country[country] = nb_hits

            hits = block.get("hits") or []
            for hit in hits:
                name = (hit.get("name") or "").strip()
                ctry = (hit.get("country") or "").strip() or country
                activity = _activity_from_hit(hit, lang=lang)
                website = (hit.get("website") or "").strip()

                if not website:
                    desc = hit.get("description") or {}
                    desc_text = (desc.get(lang) or desc.get("en") or "").strip()
                    website = _extract_website_from_text(desc_text)

                rows.append(
                    {
                        "objectID": hit.get("objectID"),
                        "name": name,
                        "activity": activity,
                        "website": website,
                        "country": ctry,
                    }
                )

            fetched_pages += 1
            pages_fetched_by_country[country] = fetched_pages

            page += 1
            if nb_pages <= 0 or page >= nb_pages:
                break

    # dedupe por objectID manteniendo orden
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for r in rows:
        oid = r.get("objectID")
        key = oid if oid is not None else (r.get("name"), r.get("country"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    meta = {
        "source": "easyfairs_widgets",
        "container_id": container_id,
        "language": lang,
        "query_seed": query_seed,
        "countries": countries,
        "hits_reported_by_country": hits_reported_by_country,
        "pages_fetched_by_country": pages_fetched_by_country,
        "dedupe_by_objectID": True,
    }
    return deduped, meta
