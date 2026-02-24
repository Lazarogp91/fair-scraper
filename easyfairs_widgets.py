# easyfairs_widgets.py
import re
import time
from typing import Any, Dict, List, Optional, Tuple
import requests

EASYFAIRS_WIDGETS_STANDS_URL = "https://my.easyfairs.com/widgets/api/stands/?language={lang}"

# Mapeo rápido por dominio (puedes ir añadiendo más ferias)
EASYFAIRS_CONTAINER_MAP: Dict[str, int] = {
    "logisticsautomationmadrid.com": 2653,
}

# Países que queremos
ALLOW_COUNTRIES = {"Spain", "Portugal"}


def _default_headers(origin: str, referer: str) -> Dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": origin,
        "Referer": referer,
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }


def _build_payload(
    *,
    container_id: int,
    query: str,
    page: int,
    hits_per_page: int,
    index_name: str = "stands_relevance",
    facets: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    if facets is None:
        facets = ["categories.name", "country"]
    return [
        {
            "indexName": index_name,
            "params": {
                "facets": facets,
                "filters": f"(containerId: {container_id})",
                "highlightPostTag": "__/ais-highlight__",
                "highlightPreTag": "__ais-highlight__",
                "hitsPerPage": hits_per_page,
                "maxValuesPerFacet": 100,
                "page": page,
                "query": query,
            },
        }
    ]


def _extract_activity(hit: Dict[str, Any], lang: str) -> str:
    cats = hit.get("categories") or []
    out: List[str] = []
    for c in cats:
        name = c.get("name")
        if isinstance(name, dict):
            v = name.get(lang) or name.get("en")
            if isinstance(v, str) and v.strip():
                out.append(v.strip())
        elif isinstance(name, str) and name.strip():
            out.append(name.strip())

    # dedupe manteniendo orden
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return ", ".join(uniq)


def _extract_country(hit: Dict[str, Any]) -> str:
    c = hit.get("country")
    if isinstance(c, str) and c.strip():
        return c.strip()
    if isinstance(c, dict):
        for k in ("name", "value", "en", "es"):
            v = c.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return ""


def _extract_website(hit: Dict[str, Any]) -> str:
    # Campos típicos si vienen “bien”
    for key in ("website", "url", "web", "companyUrl", "standUrl"):
        v = hit.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()

    # Fallback: intentar detectar URL en la descripción (no siempre fiable)
    desc = hit.get("description")
    text = ""
    if isinstance(desc, dict):
        text = desc.get("es") or desc.get("en") or ""
    elif isinstance(desc, str):
        text = desc

    if isinstance(text, str) and text:
        m = re.search(r"(https?://[^\s]+|www\.[^\s]+)", text)
        if m:
            return m.group(1).rstrip(").,;")
    return ""


def is_easyfairs_supported(url: str) -> bool:
    u = (url or "").lower()
    return any(domain in u for domain in EASYFAIRS_CONTAINER_MAP.keys())


def get_container_id_for_url(url: str) -> Optional[int]:
    u = (url or "").lower()
    for domain, cid in EASYFAIRS_CONTAINER_MAP.items():
        if domain in u:
            return cid
    return None


def fetch_easyfairs_stands(
    *,
    event_url: str,
    container_id: int,
    lang: str = "es",
    query_seed: str = "a",
    hits_per_page: int = 100,
    timeout_s: int = 30,
    polite_delay_s: float = 0.15,
    max_pages: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Devuelve (rows, meta)
    rows: lista de dicts normalizados con keys:
      name, activity, website, country, object_id, stand_number, event_name
    """
    origin = _origin_from_url(event_url)
    referer = _referer_from_url(event_url)

    url = EASYFAIRS_WIDGETS_STANDS_URL.format(lang=lang)
    headers = _default_headers(origin=origin, referer=referer)

    # 1) primera página para saber nbPages
    payload0 = _build_payload(
        container_id=container_id,
        query=query_seed,
        page=0,
        hits_per_page=hits_per_page,
    )
    r0 = requests.post(url, json=payload0, headers=headers, timeout=timeout_s)
    r0.raise_for_status()
    data0 = r0.json()

    results0 = data0.get("results") or []
    if not results0:
        raise RuntimeError("Respuesta inesperada: no hay 'results' en el JSON de Easyfairs.")
    block0 = results0[0]

    nb_pages = int(block0.get("nbPages") or 1)
    total_hits = int(block0.get("nbHits") or 0)

    if max_pages is not None:
        nb_pages = min(nb_pages, max_pages)

    rows: List[Dict[str, Any]] = []

    def consume_hits(hits: List[Dict[str, Any]]) -> None:
        for hit in hits:
            country = _extract_country(hit)
            if country and country not in ALLOW_COUNTRIES:
                continue
            rows.append(
                {
                    "name": (hit.get("name") or "").strip(),
                    "activity": _extract_activity(hit, lang),
                    "website": _extract_website(hit),
                    "country": country,
                    "object_id": str(hit.get("objectID") or ""),
                    "stand_number": str(hit.get("standNumber") or ""),
                    "event_name": str(hit.get("eventName") or ""),
                }
            )

    consume_hits(block0.get("hits") or [])

    # 2) resto de páginas
    for page in range(1, nb_pages):
        time.sleep(polite_delay_s)
        payload = _build_payload(
            container_id=container_id,
            query=query_seed,
            page=page,
            hits_per_page=hits_per_page,
        )
        rp = requests.post(url, json=payload, headers=headers, timeout=timeout_s)
        rp.raise_for_status()
        dp = rp.json()
        rsp = dp.get("results") or []
        if not rsp:
            break
        consume_hits((rsp[0].get("hits") or []))

    meta = {
        "source": "easyfairs_widgets",
        "container_id": container_id,
        "total_hits_reported": total_hits,
        "pages_fetched": nb_pages,
        "language": lang,
        "query_seed": query_seed,
    }
    return rows, meta


def _origin_from_url(event_url: str) -> str:
    # Origin = esquema + host
    # Ej: https://www.logisticsautomationmadrid.com
    m = re.match(r"^(https?://[^/]+)", event_url.strip())
    return m.group(1) if m else ""


def _referer_from_url(event_url: str) -> str:
    # Referer: muchos sites usan la home como referer
    origin = _origin_from_url(event_url)
    return origin + "/"
