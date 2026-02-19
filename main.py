import json
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

app = FastAPI(title="Fair Scraper API", version="2.0.0")


class ScrapeRequest(BaseModel):
    url: str
    countries: list[str] = Field(default_factory=lambda: ["Spain", "Portugal"])
    deep_profile: bool = True  # en modo Algolia no hace falta, pero lo dejamos por compatibilidad
    max_pages: int = 50        # para Algolia: máximo de páginas a recorrer
    max_exhibitors: int = 2000 # máximo de expositores a devolver


def clean_text(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "").strip())


def want_country(country: str, allow: list[str]) -> bool:
    if not allow:
        return True
    allow_norm = {a.strip().lower() for a in allow}
    return country.strip().lower() in allow_norm


def dedupe(items: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for it in items:
        key = (
            (it.get("company_name") or "").strip().lower(),
            (it.get("profile_url") or "").strip().lower(),
            (it.get("country") or "").strip().lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


# --------------------------
# ALGOLIA (Easyfairs) DETECT
# --------------------------

ALGOLIA_APPID_RE = re.compile(r'algolia(?:AppId|ApplicationId)["\']?\s*[:=]\s*["\']([A-Z0-9]+)["\']', re.IGNORECASE)
ALGOLIA_KEY_RE = re.compile(r'algolia(?:ApiKey|SearchApiKey|ApiKeySearch)["\']?\s*[:=]\s*["\']([A-Za-z0-9\-\_]+)["\']', re.IGNORECASE)
ALGOLIA_INDEX_RE = re.compile(r'indexName["\']?\s*[:=]\s*["\']([^"\']+)["\']', re.IGNORECASE)
CONTAINER_ID_RE = re.compile(r'containerId["\']?\s*[:=]\s*(\d+)', re.IGNORECASE)

# Alternativa: algunos Easyfairs inyectan config en window.__INITIAL_STATE__ / __NUXT__
WINDOW_JSON_HINTS = ["__INITIAL_STATE__", "__NUXT__", "__NEXT_DATA__", "easyfairs", "algolia"]


def extract_algolia_config_from_html(html: str, url: str) -> Dict[str, Any]:
    """
    Intenta extraer appId/apiKey/indexName/containerId desde el HTML (keys públicas de búsqueda).
    Devuelve dict con claves, o {} si no encuentra suficiente.
    """
    if not html:
        return {}

    app_id = None
    api_key = None
    index_name = None
    container_id = None

    m = ALGOLIA_APPID_RE.search(html)
    if m:
        app_id = m.group(1)

    m = ALGOLIA_KEY_RE.search(html)
    if m:
        api_key = m.group(1)

    # indexName puede aparecer varias veces; preferimos el que contenga "stands"
    idxs = ALGOLIA_INDEX_RE.findall(html) or []
    if idxs:
        # heurística: prioriza "stands" y/o algo tipo stands_*
        idxs_sorted = sorted(
            set(idxs),
            key=lambda s: (("stands" not in s.lower()), len(s)),
        )
        index_name = idxs_sorted[0]

    m = CONTAINER_ID_RE.search(html)
    if m:
        try:
            container_id = int(m.group(1))
        except Exception:
            container_id = None

    # A veces el containerId viene en la URL como containerId=XXXX
    try:
        qs = parse_qs(urlparse(url).query)
        if "containerId" in qs and qs["containerId"]:
            container_id = int(qs["containerId"][0])
    except Exception:
        pass

    # Si falta containerId, a menudo se ve dentro de scripts JSON; intentamos un fallback barato
    if container_id is None:
        # busca patrón "containerId": 1234 cerca de "stands"
        near = re.search(r'containerId"\s*:\s*(\d+)', html)
        if near:
            try:
                container_id = int(near.group(1))
            except Exception:
                container_id = None

    # Validación mínima: necesitamos app_id, api_key, index_name y container_id
    if app_id and api_key and index_name and container_id:
        return {
            "appId": app_id,
            "apiKey": api_key,
            "indexName": index_name,
            "containerId": container_id,
        }

    return {}


def algolia_query(
    app_id: str,
    api_key: str,
    index_name: str,
    payload: Dict[str, Any],
    timeout_s: int = 30,
) -> Dict[str, Any]:
    """
    Llama a Algolia usando keys públicas de búsqueda.
    """
    url = f"https://{app_id}-dsn.algolia.net/1/indexes/{index_name}/query"
    headers = {
        "X-Algolia-Application-Id": app_id,
        "X-Algolia-API-Key": api_key,
        "Content-Type": "application/json",
    }
    r = requests.post(url, headers=headers, json=payload, timeout=timeout_s)
    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Algolia error {r.status_code}: {r.text[:300]}")
    return r.json()


def scrape_easyfairs_via_algolia(req: ScrapeRequest, config: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Extrae expositores (stands) vía Algolia para Easyfairs. Devuelve (exhibitors, errors).
    """
    errors: List[str] = []
    exhibitors: List[Dict[str, Any]] = []

    app_id = config["appId"]
    api_key = config["apiKey"]
    index_name = config["indexName"]
    container_id = config["containerId"]

    allow = [a.strip() for a in req.countries]

    # Filtros Algolia (lo que tú ya viste):
    # filters=(containerId: 2653)
    # facetFilters=country:Portugal,country:Spain
    facet_filters = []
    if allow:
        # Algolia facetFilters admite OR en un array: ["country:Spain","country:Portugal"]
        facet_filters = [[f"country:{c}" for c in allow]]

    page = 0
    nb_pages = 1
    hits_per_page = 15

    while page < nb_pages and page < req.max_pages:
        payload = {
            "query": "",
            "page": page,
            "hitsPerPage": hits_per_page,
            "facets": ["categories.name", "country"],
            "filters": f"(containerId: {container_id})",
        }
        if facet_filters:
            payload["facetFilters"] = facet_filters

        data = algolia_query(app_id, api_key, index_name, payload)
        nb_pages = int(data.get("nbPages", 0) or 0)
        hits = data.get("hits", []) or []

        for h in hits:
            name = clean_text(h.get("name") or "")
            if not name:
                continue

            country = clean_text(h.get("country") or "")
            # En algunos índices, el país solo está en facets; si falta, lo dejamos vacío
            if country and allow and not want_country(country, allow):
                continue

            desc = ""
            d = h.get("description") or {}
            # prioriza ES si existe
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

            # perfil: en Easyfairs suele existir una URL de ficha, pero a veces no viene en el hit.
            # Construimos un "profile_url" razonable si hay objectID.
            obj_id = str(h.get("objectID") or "").strip()
            profile_url = ""
            if obj_id:
                # Muchas ferias tienen ruta /es/exhibitors/<slug>-<id>. Si no tenemos slug, devolvemos link genérico a la búsqueda.
                profile_url = req.url  # fallback seguro

            exhibitors.append(
                {
                    "company_name": name[:200],
                    "country": country,
                    "profile_url": profile_url,
                    "description": desc[:600],
                    "categories": cats[:10],
                    "standNumber": clean_text(h.get("standNumber") or ""),
                    "raw": h,  # opcional: útil para tu GPT (si no lo quieres, lo quitamos)
                }
            )

            if len(exhibitors) >= req.max_exhibitors:
                break

        if len(exhibitors) >= req.max_exhibitors:
            break

        page += 1

    exhibitors = dedupe(exhibitors)
    return exhibitors, errors


# --------------------------
# HTML FALLBACK (simple)
# --------------------------

EXHIBITOR_PATH_RE = re.compile(r"/es/exhibitors/[^/?#]+-\d+")


def is_exhibitor_profile(href_abs: str) -> bool:
    try:
        path = urlparse(href_abs).path
    except Exception:
        return False
    return bool(EXHIBITOR_PATH_RE.search(path))


def scrape_fallback_html(req: ScrapeRequest) -> Tuple[List[Dict[str, Any]], List[str]]:
    exhibitors: List[Dict[str, Any]] = []
    errors: List[str] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1400, "height": 900})

        try:
            page.goto(req.url, wait_until="networkidle", timeout=60000)
            page.wait_for_timeout(800)
        except Exception as e:
            browser.close()
            raise HTTPException(status_code=500, detail=f"Navigation failed: {e}")

        base = req.url
        links = page.locator("a")
        count = min(links.count(), 3000)

        for i in range(count):
            try:
                link = links.nth(i)
                text = clean_text(link.inner_text() or "")
                href = link.get_attribute("href") or ""
                if not href or len(text) < 2:
                    continue
                href_abs = href if href.startswith("http") else (req.url.rstrip("/") + "/" + href.lstrip("/"))
                if not is_exhibitor_profile(href_abs):
                    continue

                exhibitors.append(
                    {
                        "company_name": text[:200],
                        "country": "",
                        "profile_url": href_abs,
                        "description": "",
                        "categories": [],
                        "standNumber": "",
                        "raw_text": text[:500],
                    }
                )
                if len(exhibitors) >= req.max_exhibitors:
                    break
            except Exception:
                continue

        browser.close()

    exhibitors = dedupe(exhibitors)
    return exhibitors, errors


@app.post("/scrape")
def scrape(req: ScrapeRequest):
    if not req.url.startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="Invalid url")

    # 1) Cargamos HTML y tratamos de extraer config Algolia
    html = ""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1400, "height": 900})
        try:
            page.goto(req.url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(600)
            html = page.content()
        except PWTimeoutError:
            pass
        except Exception:
            pass
        finally:
            try:
                browser.close()
            except Exception:
                pass

    config = extract_algolia_config_from_html(html, req.url)

    # 2) Si hay Algolia config, usamos Algolia (modo correcto para Easyfairs)
    if config:
        exhibitors, errors = scrape_easyfairs_via_algolia(req, config)
        return {
            "source_url": req.url,
            "mode": "algolia",
            "algolia": {"indexName": config["indexName"], "containerId": config["containerId"]},
            "exhibitors": exhibitors,
            "errors": errors,
        }

    # 3) Si no, fallback HTML
    exhibitors, errors = scrape_fallback_html(req)
    return {
        "source_url": req.url,
        "mode": "html_fallback",
        "exhibitors": exhibitors,
        "errors": errors,
    }
