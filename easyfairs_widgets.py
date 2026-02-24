# easyfairs_widgets.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

# ------------------------------------------------------------
# Ajusta aquí el mapping dominio -> containerId
# (este dato lo sacas del payload en Network: filters "(containerId: 2653)")
# ------------------------------------------------------------
EASYFAIRS_CONTAINER_MAP: Dict[str, int] = {
    "www.logisticsautomationmadrid.com": 2653,
    "logisticsautomationmadrid.com": 2653,
}

# Endpoint que has capturado en Network
EASYFAIRS_WIDGETS_API = "https://my.easyfairs.com/widgets/api/stands/"


def is_easyfairs_supported(url: str) -> bool:
    """
    Devuelve True si el dominio está en el mapping.
    """
    host = (urlparse(url).netloc or "").lower()
    return host in EASYFAIRS_CONTAINER_MAP


def get_container_id_for_url(url: str) -> Optional[int]:
    """
    Devuelve containerId según el dominio.
    """
    host = (urlparse(url).netloc or "").lower()
    return EASYFAIRS_CONTAINER_MAP.get(host)


def _post_widgets_query(
    *,
    container_id: int,
    lang: str,
    query: str,
    hits_per_page: int,
    page: int,
    timeout_s: int,
    country: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Llama al endpoint /widgets/api/stands/?language=es con el formato de "multiple queries"
    que has visto en DevTools.

    Nota:
    - El backend parece compatible con estilo Algolia.
    - Para filtrar país, intentamos facetFilters si lo acepta.
      Si no lo aceptara en algún evento, se puede filtrar en cliente por hits[].country.
    """
    params_obj: Dict[str, Any] = {
        "facets": ["categories.name", "country"],
        "filters": f"(containerId: {container_id})",
        "highlightPostTag": "__/ais-highlight__",
        "highlightPreTag": "__ais-highlight__",
        "hitsPerPage": hits_per_page,
        "maxValuesPerFacet": 100,
        "page": page,
        "query": query,
    }

    # Intento de filtro por país (si el backend lo soporta)
    if country:
        # Algolia suele aceptar facetFilters como lista
        params_obj["facetFilters"] = [f"country:{country}"]

    payload = [{"indexName": "stands_relevance", "params": params_obj}]

    r = requests.post(
        f"{EASYFAIRS_WIDGETS_API}?language={lang}",
        json=payload,
        timeout=timeout_s,
        headers={
            "Accept": "*/*",
            "Content-Type": "application/json",
            # "Origin" y "Referer" no suelen ser necesarios desde servidor,
            # pero si algún evento lo exige, se pueden añadir.
        },
    )
    r.raise_for_status()
    return r.json()


def fetch_easyfairs_stands_by_countries(
    *,
    event_url: str,
    container_id: int,
    countries: List[str],
    lang: str = "es",
    query_seed: str = "a",
    hits_per_page: int = 100,
    timeout_s: int = 25,
    max_pages: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Devuelve lista de stands (deduplicada por objectID) filtrada por países.

    Retorna:
      rows: [{objectID, name, activity, website, country, ...}]
      meta: info de depuración
    """
    all_rows: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()

    hits_reported_by_country: Dict[str, int] = {}
    pages_fetched_by_country: Dict[str, int] = {}

    for country in countries:
        page = 0
        fetched_pages = 0
        total_hits_reported: Optional[int] = None

        while True:
            if max_pages is not None and fetched_pages >= max_pages:
                break

            data = _post_widgets_query(
                container_id=container_id,
                lang=lang,
                query=query_seed,
                hits_per_page=hits_per_page,
                page=page,
                timeout_s=timeout_s,
                country=country,
            )

            results = data.get("results") or []
            if not results:
                break

            first = results[0]
            hits = first.get("hits") or []

            # Guardamos nbHits si aparece
            if total_hits_reported is None:
                nb_hits = first.get("nbHits")
                if isinstance(nb_hits, int):
                    total_hits_reported = nb_hits

            # Si el filtro por país no funcionó en servidor, filtramos aquí por seguridad
            filtered_hits = []
            for h in hits:
                h_country = h.get("country") or ""
                if not country or h_country == country:
                    filtered_hits.append(h)

            for h in filtered_hits:
                oid = str(h.get("objectID") or "")
                if not oid:
                    continue
                if oid in seen_ids:
                    continue
                seen_ids.add(oid)

                name = h.get("name") or ""
                cats = h.get("categories") or []
                cat_names: List[str] = []
                for c in cats:
                    # c["name"] puede ser dict con "es"/"en"
                    nm = (c.get("name") or {})
                    if isinstance(nm, dict):
                        cat_names.append(nm.get(lang) or nm.get("en") or "")
                    elif isinstance(nm, str):
                        cat_names.append(nm)
                activity = ", ".join([x for x in cat_names if x])

                # Website: a veces viene en description o no viene.
                website = h.get("website") or ""
                # Country:
                h_country = h.get("country") or country or ""

                all_rows.append(
                    {
                        "objectID": oid,
                        "name": name,
                        "activity": activity,
                        "website": website,
                        "country": h_country,
                        "raw": h,
                    }
                )

            fetched_pages += 1
            page += 1

            # Cortes por fin de paginación
            nb_pages = first.get("nbPages")
            if isinstance(nb_pages, int) and page >= nb_pages:
                break

            # Si no hay hits, fin
            if not hits:
                break

        # Meta por país
        if total_hits_reported is None:
            # Intento alternativo: facets['country'][country]
            try:
                facets = (results[0].get("facets") or {}).get("country") or {}
                if isinstance(facets, dict) and country in facets:
                    total_hits_reported = int(facets[country])
            except Exception:
                pass

        hits_reported_by_country[country] = int(total_hits_reported or 0)
        pages_fetched_by_country[country] = fetched_pages

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

    # quitamos raw para devolver limpio si quieres (pero lo dejamos aquí por si haces debug)
    # Si prefieres sin raw:
    cleaned: List[Dict[str, Any]] = []
    for r in all_rows:
        cleaned.append(
            {
                "objectID": r["objectID"],
                "name": r["name"],
                "activity": r["activity"],
                "website": r["website"],
                "country": r["country"],
            }
        )

    return cleaned, meta
