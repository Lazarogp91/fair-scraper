# easyfairs_widgets.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
import re
import time
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
    return bool(host) and host in EASYFAIRS_CONTAINER_MAP


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

# User-Agent razonable (mejor no usar uno ultra viejo)
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

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
    out = []
    for n in names:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return ", ".join(out)


def _build_algolia_filters(container_id: int, country: Optional[str]) -> str:
    """
    Base: (containerId: 2653)
    Con país: (containerId: 2653) AND country:"Spain"
    """
    base = f"(containerId: {container_id})"
    if country:
        base += f' AND country:"{country}"'
    return base


def _build_headers_for_event(event_url: str) -> Dict[str, str]:
    """
    IMPORTANTÍSIMO: Origin/Referer dinámicos según la feria.
    Evita 403 cuando cambias de dominio o cuando Easyfairs valida cabeceras.
    """
    try:
        p = urlparse(event_url)
        host = (p.netloc or "").strip()
        if not host:
            host = "www.logisticsautomationmadrid.com"
    except Exception:
        host = "www.logisticsautomationmadrid.com"

    origin = f"https://{host}"
    referer = origin + "/"

    return {
        "accept": "*/*",
        "content-type": "application/json",
        "origin": origin,
        "referer": referer,
        "user-agent": DEFAULT_UA,
    }


def _post_with_retries(
    session: requests.Session,
    url: str,
    json_payload: Any,
    headers: Dict[str, str],
    timeout_s: int,
    max_retries: int = 5,
    backoff_s: float = 1.2,
) -> requests.Response:
    """
    Reintentos para 429/403/5xx y errores de red.
    """
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            r = session.post(url, json=json_payload, headers=headers, timeout=timeout_s)
            # Si va bien, devuelve
            if r.status_code < 400:
                return r

            # Reintentar en códigos típicos de bloqueo/rate limit/errores temporales
            if r.status_code in (403, 429, 500, 502, 503, 504):
                # Respeta Retry-After si existe
                retry_after = r.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep_s = float(retry_after)
                    except Exception:
                        sleep_s = backoff_s * attempt
                else:
                    sleep_s = backoff_s * attempt

                if attempt < max_retries:
                    time.sleep(sleep_s)
                    continue

            # Si no se reintenta o es último intento -> error explícito
            body_preview = (r.text or "")[:800]
            raise RuntimeError(f"Easyfairs widgets HTTP {r.status_code}. Body: {body_preview}")

        except (requests.Timeout, requests.ConnectionError, requests.ChunkedEncodingError) as e:
            last_exc = e
            if attempt < max_retries:
                time.sleep(backoff_s * attempt)
                continue
            raise RuntimeError(f"Network error calling Easyfairs widgets after retries: {e}") from e
        except Exception as e:
            last_exc = e
            if attempt < max_retries:
                time.sleep(backoff_s * attempt)
                continue
            raise

    # No debería llegar aquí
    raise RuntimeError(f"Easyfairs widgets failed after retries: {last_exc}")


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
    """
    Devuelve:
      rows: lista dicts con name/activity/website/country/objectID
      meta: info paginación, hits reportados, etc.
    """
    if not countries:
        countries = ["Spain", "Portugal"]

    endpoint = EASYFAIRS_WIDGETS_ENDPOINT.format(lang=lang)
    headers = _build_headers_for_event(event_url)

    rows: List[Dict[str, Any]] = []
    hits_reported_by_country: Dict[str, int] = {}
    pages_fetched_by_country: Dict[str, int] = {}

    session = requests.Session()

    # (Opcional) Mantén conexiones vivas
    session.headers.update({"connection": "keep-alive"})

    for country in countries:
        page = 0
        fetched_pages = 0
        nb_pages_last = None

        while True:
            if max_pages is not None and fetched_pages >= max_pages:
                break

            payload = [
                {
                    "indexName": "stands_relevance",
                    "params": {
                        "facets": ["categories.name", "country"],
                        "filters": _build_algolia_filters(container_id, country),
                        "highlightPostTag": "__ais-highlight__",
                        "highlightPreTag": "__ais-highlight__",
                        "hitsPerPage": int(hits_per_page),
                        "maxValuesPerFacet": 100,
                        "page": int(page),
                        "query": str(query_seed),
                    },
                }
            ]

            r = _post_with_retries(
                session=session,
                url=endpoint,
                json_payload=payload,
                headers=headers,
                timeout_s=timeout_s,
                max_retries=5,
                backoff_s=1.3,
            )

            try:
                data = r.json() or {}
            except Exception:
                body_preview = (r.text or "")[:800]
                raise RuntimeError(f"Easyfairs widgets returned non-JSON. Body: {body_preview}")

            results = data.get("results") or []
            if not results:
                break

            block = results[0]
            nb_hits = int(block.get("nbHits") or 0)
            nb_pages = int(block.get("nbPages") or 0)
            nb_pages_last = nb_pages
            hits_reported_by_country[country] = nb_hits

            hits = block.get("hits") or []
            if not hits:
                break

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

        # Si Easyfairs dijo nbPages pero no hemos podido llegar, al menos lo registramos
        if nb_pages_last is not None and country not in pages_fetched_by_country:
            pages_fetched_by_country[country] = 0

    # dedupe por objectID manteniendo orden
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for rr in rows:
        oid = rr.get("objectID")
        key = oid if oid is not None else (rr.get("name"), rr.get("country"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(rr)

    meta = {
        "source": "easyfairs_widgets",
        "container_id": container_id,
        "language": lang,
        "query_seed": query_seed,
        "countries": countries,
        "hits_reported_by_country": hits_reported_by_country,
        "pages_fetched_by_country": pages_fetched_by_country,
        "dedupe_by_objectID": True,
        "endpoint": endpoint,
    }
    return deduped, meta
