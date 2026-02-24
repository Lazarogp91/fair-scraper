# easyfairs_widgets.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
import re
import time

import requests

# =========================================================
# Easyfairs containerId mapping (añade dominios aquí)
# =========================================================
EASYFAIRS_CONTAINER_MAP: Dict[str, int] = {
    "www.logisticsautomationmadrid.com": 2653,
    "logisticsautomationmadrid.com": 2653,
}

EASYFAIRS_WIDGETS_BASE = "https://my.easyfairs.com"
EASYFAIRS_STANDS_API = f"{EASYFAIRS_WIDGETS_BASE}/widgets/api/stands/?language={{lang}}"

# Regex para detectar URLs en texto (incluye www.)
_URL_RE = re.compile(
    r"(?:(https?://)|\bwww\.)[a-zA-Z0-9\-._~%]+(?:\.[a-zA-Z]{2,})(?::\d+)?(?:/[^\s\"'<>)]*)?"
)

# Algunos falsos positivos típicos que conviene evitar
_BAD_URL_ENDINGS = (".jpeg", ".jpg", ".png", ".gif", ".webp", ".svg", ".pdf")


def get_container_id_for_url(event_url: str) -> Optional[int]:
    """
    Devuelve el containerId de una URL de feria Easyfairs según el dominio.
    """
    try:
        host = urlparse(event_url).netloc.lower()
    except Exception:
        return None
    return EASYFAIRS_CONTAINER_MAP.get(host)


def _extract_first_url(text: str) -> str:
    """
    Extrae la primera URL útil de un texto. Devuelve "" si no encuentra.
    """
    if not text:
        return ""
    for m in _URL_RE.finditer(text):
        u = m.group(0).strip().rstrip(".,;:)")
        # normaliza
        if u.lower().endswith(_BAD_URL_ENDINGS):
            continue
        if u.startswith("www."):
            u = "https://" + u
        return u
    return ""


def _categories_to_activity(hit: Dict[str, Any], lang: str) -> str:
    cats = hit.get("categories") or []
    out: List[str] = []
    for c in cats:
        name = (c.get("name") or {}).get(lang)
        if name and name not in out:
            out.append(name)
    return ", ".join(out)


def _guess_website_from_hit(hit: Dict[str, Any], lang: str) -> str:
    """
    Best-effort para encontrar la web dentro del hit.
    """
    # campos típicos (dependen de implementación)
    for k in ("website", "webSite", "url", "standUrl", "companyWebsite"):
        v = hit.get(k)
        if isinstance(v, str) and v.strip():
            u = v.strip()
            if u.startswith("www."):
                u = "https://" + u
            return u

    # a veces está en description
    desc = hit.get("description") or {}
    if isinstance(desc, dict):
        d = desc.get(lang) or desc.get("en") or ""
    else:
        d = str(desc)
    u = _extract_first_url(d)
    if u:
        return u

    return ""


def _post_algolia_widget(
    session: requests.Session,
    *,
    container_id: int,
    lang: str,
    query_seed: str,
    page: int,
    hits_per_page: int,
    country_filter: Optional[str],
    timeout_s: int,
) -> Dict[str, Any]:
    """
    Llama al endpoint widget de Easyfairs, que funciona como wrapper de Algolia.
    """
    url = EASYFAIRS_STANDS_API.format(lang=lang)

    # Nota: el "filters" es sintaxis Algolia.
    # En tu captura original era "(containerId: 2653)"
    # Aquí añadimos AND country:"Spain" / "Portugal" cuando aplique.
    filt = f"(containerId: {container_id})"
    if country_filter:
        # country facet usa valores como "Spain", "Portugal"
        filt = f'{filt} AND country:"{country_filter}"'

    payload = [
        {
            "indexName": "stands_relevance",
            "params": {
                "facets": ["categories.name", "country"],
                "filters": filt,
                "highlightPostTag": "__/ais-highlight__",
                "highlightPreTag": "__ais-highlight__",
                "hitsPerPage": hits_per_page,
                "maxValuesPerFacet": 100,
                "page": page,
                "query": query_seed,
            },
        }
    ]

    r = session.post(url, json=payload, timeout=timeout_s)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict) or "results" not in data or not data["results"]:
        raise RuntimeError("Respuesta inesperada del endpoint Easyfairs widgets (sin 'results').")
    return data["results"][0]


def _try_fetch_stand_detail(
    session: requests.Session,
    *,
    object_id: str,
    lang: str,
    timeout_s: int,
) -> Optional[Dict[str, Any]]:
    """
    Deep profile: intenta obtener más datos del expositor con varios patrones.
    Si no existe endpoint compatible, devuelve None sin romper.
    """
    # Estos patrones NO están garantizados; por eso es best-effort.
    # Se prueban varios candidatos típicos.
    candidates = [
        f"{EASYFAIRS_WIDGETS_BASE}/widgets/api/stands/{object_id}/?language={lang}",
        f"{EASYFAIRS_WIDGETS_BASE}/widgets/api/stands/{object_id}?language={lang}",
        f"{EASYFAIRS_WIDGETS_BASE}/backend/api/stands/{object_id}?language={lang}",
        f"{EASYFAIRS_WIDGETS_BASE}/backend/api/stands/{object_id}/?language={lang}",
    ]

    headers = {"Accept": "application/json"}
    for u in candidates:
        try:
            rr = session.get(u, headers=headers, timeout=timeout_s)
            if rr.status_code == 404:
                continue
            rr.raise_for_status()
            j = rr.json()
            if isinstance(j, dict):
                return j
        except Exception:
            continue
    return None


def fetch_easyfairs_stands_by_countries(
    *,
    event_url: str,
    container_id: int,
    countries: List[str],
    lang: str = "es",
    query_seed: str = "a",
    hits_per_page: int = 100,
    timeout_s: int = 25,
    max_pages: Optional[int] = 5,
    deep_profile: bool = False,
    polite_delay_s: float = 0.0,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Devuelve (rows, meta)
    rows: lista de dict con keys: objectID, name, activity, website, country
    """
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; FairScraper/1.0; +https://example.com)",
            "Origin": urlparse(event_url).scheme + "://" + urlparse(event_url).netloc,
            "Referer": event_url,
            "Accept": "application/json, text/plain, */*",
        }
    )

    all_rows: List[Dict[str, Any]] = []
    seen_object_ids = set()

    hits_reported_by_country: Dict[str, int] = {}
    pages_fetched_by_country: Dict[str, int] = {}

    for country in countries:
        page = 0
        fetched_pages = 0
        total_hits_reported = None

        while True:
            if max_pages is not None and fetched_pages >= max_pages:
                break

            res0 = _post_algolia_widget(
                session,
                container_id=container_id,
                lang=lang,
                query_seed=query_seed,
                page=page,
                hits_per_page=hits_per_page,
                country_filter=country,
                timeout_s=timeout_s,
            )

            if total_hits_reported is None:
                # nbHits suele venir en el wrapper
                nb_hits = res0.get("nbHits")
                if isinstance(nb_hits, int):
                    total_hits_reported = nb_hits
                    hits_reported_by_country[country] = nb_hits

            hits = res0.get("hits") or []
            if not hits:
                break

            for h in hits:
                oid = str(h.get("objectID") or "").strip()
                if not oid:
                    continue
                if oid in seen_object_ids:
                    continue
                seen_object_ids.add(oid)

                name = (h.get("name") or "").strip()
                activity = _categories_to_activity(h, lang=lang)

                website = _guess_website_from_hit(h, lang=lang)

                # Deep profile: si falta web, intenta detalle
                if deep_profile and not website:
                    detail = _try_fetch_stand_detail(session, object_id=oid, lang=lang, timeout_s=timeout_s)
                    if detail:
                        # intenta campos típicos y fallback a texto
                        for k in ("website", "webSite", "url", "companyWebsite"):
                            v = detail.get(k)
                            if isinstance(v, str) and v.strip():
                                website = v.strip()
                                if website.startswith("www."):
                                    website = "https://" + website
                                break
                        if not website:
                            # a veces el detalle lleva description también
                            ddesc = detail.get("description")
                            if isinstance(ddesc, dict):
                                dd = ddesc.get(lang) or ddesc.get("en") or ""
                            else:
                                dd = str(ddesc or "")
                            website = _extract_first_url(dd)

                all_rows.append(
                    {
                        "objectID": oid,
                        "name": name,
                        "activity": activity,
                        "website": website,
                        "country": country,
                    }
                )

            fetched_pages += 1
            page += 1

            # si ya cubrimos todas las páginas, paramos
            nb_pages = res0.get("nbPages")
            if isinstance(nb_pages, int) and page >= nb_pages:
                break

            if polite_delay_s > 0:
                time.sleep(polite_delay_s)

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
        "deep_profile": bool(deep_profile),
    }

    return all_rows, meta
