import re
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

EASYFAIRS_WIDGETS_STANDS_URL = "https://my.easyfairs.com/widgets/api/stands/?language={lang}"

# Mapping dominio -> containerId (añade más ferias aquí)
EASYFAIRS_CONTAINER_MAP: Dict[str, int] = {
    "logisticsautomationmadrid.com": 2653,
}

DEFAULT_ALLOW_COUNTRIES = ["Spain", "Portugal"]


def _default_headers(origin: str, referer: str) -> Dict[str, str]:
    return {
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Origin": origin,
        "Referer": referer,
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }


def _origin_from_url(event_url: str) -> str:
    m = re.match(r"^(https?://[^/]+)", (event_url or "").strip())
    return m.group(1) if m else ""


def _referer_from_url(event_url: str) -> str:
    origin = _origin_from_url(event_url)
    return origin + "/" if origin else ""


def get_container_id_for_url(url: str) -> Optional[int]:
    u = (url or "").lower()
    for domain, cid in EASYFAIRS_CONTAINER_MAP.items():
        if domain in u:
            return cid
    return None


def _build_payload(
    *,
    container_id: int,
    query: str,
    page: int,
    hits_per_page: int,
    filters_extra: Optional[str] = None,
    index_name: str = "stands_relevance",
    facets: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    if facets is None:
        facets = ["categories.name", "country"]

    base_filters = f"(containerId: {container_id})"
    final_filters = f"{base_filters} AND {filters_extra}" if filters_extra else base_filters

    return [
        {
            "indexName": index_name,
            "params": {
                "facets": facets,
                "filters": final_filters,
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


def _extract_website(hit: Dict[str, Any]) -> str:
    # campos posibles si vienen
    for key in ("website", "url", "web", "companyUrl", "standUrl"):
        v = hit.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()

    # fallback: buscar URL en la descripción
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


def fetch_easyfairs_stands_by_countries(
    *,
    event_url: str,
    container_id: int,
    countries: Optional[List[str]] = None,
    lang: str = "es",
    query_seed: str = "a",
    hits_per_page: int = 100,
    timeout_s: int = 30,
    polite_delay_s: float = 0.15,
    max_pages: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Estrategia robusta para 'pais':
    - Los hits no siempre incluyen el campo country.
    - Pero el índice sí permite filtrar por country como facet.
    - Hacemos queries separadas con filtro country:"Spain"/"Portugal" y etiquetamos el país
      porque el filtro lo garantiza.

    Devuelve filas normalizadas:
      name, activity, website, country, object_id, stand_number, event_name
    """
    if not countries:
        countries = DEFAULT_ALLOW_COUNTRIES

    origin = _origin_from_url(event_url)
    referer = _referer_from_url(event_url)
    headers = _default_headers(origin=origin, referer=referer)
    api_url = EASYFAIRS_WIDGETS_STANDS_URL.format(lang=lang)

    all_rows: List[Dict[str, Any]] = []
    seen_object_ids: set = set()

    pages_fetched_by_country: Dict[str, int] = {}
    hits_reported_by_country: Dict[str, int] = {}

    for country in countries:
        filters_extra = f'country:"{country}"'

        # página 0
        payload0 = _build_payload(
            container_id=container_id,
            query=query_seed,
            page=0,
            hits_per_page=hits_per_page,
            filters_extra=filters_extra,
        )
        r0 = requests.post(api_url, json=payload0, headers=headers, timeout=timeout_s)
        r0.raise_for_status()
        data0 = r0.json()

        results0 = data0.get("results") or []
        if not results0:
            pages_fetched_by_country[country] = 0
            hits_reported_by_country[country] = 0
            continue

        block0 = results0[0]
        nb_pages = int(block0.get("nbPages") or 1)
        nb_hits = int(block0.get("nbHits") or 0)
        if max_pages is not None:
            nb_pages = min(nb_pages, max_pages)

        hits_reported_by_country[country] = nb_hits

        def consume(hits: List[Dict[str, Any]]) -> None:
            for hit in hits:
                oid = str(hit.get("objectID") or "")
                if oid and oid in seen_object_ids:
                    continue
                if oid:
                    seen_object_ids.add(oid)

                all_rows.append(
                    {
                        "name": (hit.get("name") or "").strip(),
                        "activity": _extract_activity(hit, lang),
                        "website": _extract_website(hit),
                        "country": country,  # garantizado por el filtro
                        "object_id": oid,
                        "stand_number": str(hit.get("standNumber") or ""),
                        "event_name": str(hit.get("eventName") or ""),
                    }
                )

        consume(block0.get("hits") or [])

        pages_fetched = 1

        # resto de páginas
        for page in range(1, nb_pages):
            time.sleep(polite_delay_s)
            payload = _build_payload(
                container_id=container_id,
                query=query_seed,
                page=page,
                hits_per_page=hits_per_page,
                filters_extra=filters_extra,
            )
            rp = requests.post(api_url, json=payload, headers=headers, timeout=timeout_s)
            rp.raise_for_status()
            dp = rp.json()
            rsp = dp.get("results") or []
            if not rsp:
                break
            consume((rsp[0].get("hits") or []))
            pages_fetched += 1

        pages_fetched_by_country[country] = pages_fetched

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

    return all_rows, meta
