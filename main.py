import asyncio
import json
import re
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from playwright.async_api import async_playwright

app = FastAPI(title="Fair Scraper API", version="3.2.1")


# =========================
# Models
# =========================
class ScrapeRequest(BaseModel):
    url: str
    countries: List[str] = Field(default_factory=lambda: ["Spain", "Portugal"])
    manufacturers_only: bool = True

    # límites prácticos
    max_pages: int = 50
    max_exhibitors: int = 2000

    # tiempos
    sniff_timeout_ms: int = 25000  # tiempo máximo esperando detectar el backend

    # si quieres enriquecer después, lo dejamos preparado
    deep_profile: bool = False


# =========================
# Manufacturer heuristic (tu criterio)
# =========================
MANUFACTURER_KEYWORDS = [
    r"\bfabricante\b",
    r"\bmanufacturer\b",
    r"\bmanufacturing\b",
    r"\bfabricación\b",
    r"\bproducción\b",
    r"\bOEM\b",
    r"\bdiseñamos\b",
    r"\bdiseña\b",
    r"\bdesign\b",
    r"\bdesarrolla\b",
    r"\bproduce\b",
    r"\bproducimos\b",
    r"\bfabrica\b",
    r"\bmanufactures\b",
]

NON_MANUFACTURER_HINTS = [
    r"\bdistribuidor\b",
    r"\bdistribución\b",
    r"\bdistributor\b",
    r"\bintegrador\b",
    r"\bintegración\b",
    r"\bintegrator\b",
    r"\bconsultor\b",
    r"\bconsultoría\b",
    r"\bservices?\b",
    r"\bservicios\b",
]


def is_manufacturer(text: str) -> Tuple[bool, str, str]:
    """
    Devuelve (is_manu, evidencia, confianza)
    """
    t = (text or "").lower()

    # señales negativas fuertes
    for pat in NON_MANUFACTURER_HINTS:
        if re.search(pat, t, flags=re.IGNORECASE):
            # no descartamos si hay señales positivas muy fuertes, pero bajamos confianza
            break

    positives = [pat for pat in MANUFACTURER_KEYWORDS if re.search(pat, t, flags=re.IGNORECASE)]
    negatives = [pat for pat in NON_MANUFACTURER_HINTS if re.search(pat, t, flags=re.IGNORECASE)]

    if positives:
        # si hay positivos y no hay muchos negativos, alta
        confidence = "Alta" if len(negatives) == 0 else "Media"
        evidence = " + ".join([re.sub(r"\\b", "", p).strip() for p in positives[:3]])
        return True, evidence, confidence

    # sin positivos: no confirmado
    return False, "Sin evidencia textual explícita", "Baja"


def best_lang_text(obj: Any) -> str:
    """
    Easyfairs suele traer description como {en: "...", es: "..."}.
    """
    if obj is None:
        return ""
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        # prioriza es, luego en, luego cualquier
        for k in ("es", "en"):
            if obj.get(k):
                return obj[k]
        for _, v in obj.items():
            if isinstance(v, str) and v.strip():
                return v
    return ""


# =========================
# Algolia utilities
# =========================
ALGOLIA_QUERIES_RE = re.compile(r"/1/indexes/.+/queries", re.IGNORECASE)


def parse_algolia_headers(headers: Dict[str, str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Busca credenciales típicas en headers.
    """
    app_id = None
    api_key = None
    for k, v in headers.items():
        lk = k.lower()
        if lk == "x-algolia-application-id":
            app_id = v
        if lk == "x-algolia-api-key":
            api_key = v
    return app_id, api_key


def find_container_id(payload: Dict[str, Any]) -> Optional[int]:
    """
    En tu JSON aparece filters: (containerId: 2653)
    A veces viene en params / filters.
    """
    try:
        results = payload.get("results") or []
        for r in results:
            params = r.get("params", "")
            m = re.search(r"containerId%3A\+(\d+)", params)
            if m:
                return int(m.group(1))
            # a veces viene como containerId: 2653 sin URL-encode
            m2 = re.search(r"containerId:\s*(\d+)", params)
            if m2:
                return int(m2.group(1))
    except Exception:
        pass
    return None


def find_index_name_from_hits(payload: Dict[str, Any]) -> Optional[str]:
    """
    En tu JSON: hits[0]._index = stands_1764744851, y "index": "stands".
    Para queries, necesitamos el index real. Normalmente es el _index.
    """
    try:
        results = payload.get("results") or []
        for r in results:
            hits = r.get("hits") or []
            if hits and isinstance(hits, list):
                idx = hits[0].get("_index")
                if idx:
                    return idx
        # fallback: a veces viene directamente
        for r in results:
            if r.get("index"):
                return r["index"]
    except Exception:
        pass
    return None


def algolia_post_queries(
    app_id: str,
    api_key: str,
    index_name: str,
    container_id: Optional[int],
    page: int,
    hits_per_page: int = 100,
    countries: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Llama directamente a Algolia /queries.
    """
    url = f"https://{app_id}-dsn.algolia.net/1/indexes/*/queries"
    headers = {
        "Content-Type": "application/json",
        "X-Algolia-Application-Id": app_id,
        "X-Algolia-API-Key": api_key,
    }

    # filtros típicos
    filters = []
    if container_id is not None:
        filters.append(f"(containerId: {container_id})")

    facet_filters = []
    if countries:
        # Algolia facetFilters: ["country:Spain", "country:Portugal"] (OR en el mismo array)
        facet_filters.append([f"country:{c}" for c in countries])

    params_parts = [
        f"hitsPerPage={hits_per_page}",
        f"page={page}",
        "query=",
        "facets=country,categories.name",
        "maxValuesPerFacet=100",
        "highlightPreTag=__ais-highlight__",
        "highlightPostTag=__/ais-highlight__",
    ]

    if filters:
        # Algolia usa filters=... con encoding; aquí lo pasamos “raw”, Algolia lo acepta
        params_parts.append("filters=" + " AND ".join(filters))

    if facet_filters:
        # en /queries va como JSON, no como querystring: se mete en request body con params
        pass

    params = "&".join(params_parts)

    body = {
        "requests": [
            {
                "indexName": index_name,
                "params": params,
            }
        ]
    }

    # facetFilters debe ir dentro de params como querystring encoded si quieres ser purista,
    # pero Algolia acepta facetFilters en el cuerpo en algunas integraciones.
    # Para máxima compatibilidad, lo metemos en params url-encoded simple.
    if facet_filters:
        # country%3ASpain%2Ccountry%3APortugal -> lo hacemos simple:
        # Algolia entiende facetFilters=[["country:Spain","country:Portugal"]] en JSON,
        # pero aquí vamos a “params” con encoding mínimo.
        ff = ",".join([f"country%3A{c}" for c in countries or []])
        params += f"&facetFilters={ff}"
        body["requests"][0]["params"] = params

    resp = requests.post(url, headers=headers, data=json.dumps(body), timeout=60)
    if resp.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Algolia error {resp.status_code}: {resp.text[:500]}")
    return resp.json()


# =========================
# Playwright sniffing
# =========================
async def sniff_algolia_from_url(url: str, sniff_timeout_ms: int) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[int]]:
    """
    Abre la URL y escucha respuestas de red para encontrar:
    - x-algolia-application-id
    - x-algolia-api-key
    - index_name (desde payload)
    - container_id (desde payload/params)
    """
    found = {
        "app_id": None,
        "api_key": None,
        "index_name": None,
        "container_id": None,
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        async def on_response(response):
            try:
                rurl = response.url
                if ("algolia" in rurl.lower()) or ALGOLIA_QUERIES_RE.search(rurl):
                    headers = await response.all_headers()
                    app_id, api_key = parse_algolia_headers(headers)

                    if app_id and api_key:
                        found["app_id"] = app_id
                        found["api_key"] = api_key

                    # intentar leer JSON del body (a veces no es JSON o está comprimido)
                    try:
                        if "application/json" in (headers.get("content-type", "")).lower():
                            payload = await response.json()
                            if not found["container_id"]:
                                cid = find_container_id(payload)
                                if cid:
                                    found["container_id"] = cid
                            if not found["index_name"]:
                                idx = find_index_name_from_hits(payload)
                                if idx:
                                    found["index_name"] = idx
                    except Exception:
                        pass
            except Exception:
                pass

        page.on("response", on_response)

        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        # deja tiempo para que cargue el catálogo
        await page.wait_for_timeout(sniff_timeout_ms)

        await browser.close()

    return found["app_id"], found["api_key"], found["index_name"], found["container_id"]


# =========================
# Main endpoint
# =========================
@app.post("/scrape")
async def scrape_fair(request: ScrapeRequest):
    """
    Devuelve SOLO fabricantes ES/PT si manufacturers_only=true.
    """
    try:
        # 1) sniff Algolia via red (robusto)
        app_id, api_key, index_name, container_id = await sniff_algolia_from_url(
            request.url, request.sniff_timeout_ms
        )

        exhibitors: List[Dict[str, Any]] = []

        if app_id and api_key and index_name:
            # 2) Paginar en Algolia
            page = 0
            hits_per_page = 100

            while page < request.max_pages and len(exhibitors) < request.max_exhibitors:
                data = algolia_post_queries(
                    app_id=app_id,
                    api_key=api_key,
                    index_name=index_name,
                    container_id=container_id,
                    page=page,
                    hits_per_page=hits_per_page,
                    countries=request.countries,
                )

                results = data.get("results") or []
                if not results:
                    break

                block = results[0]
                hits = block.get("hits") or []
                if not hits:
                    break

                for h in hits:
                    name = h.get("name") or ""
                    country = h.get("country") or ""  # muchas implementaciones lo traen directo
                    desc = best_lang_text((h.get("description") or {}))
                    cats = h.get("categories") or []
                    cat_names = []
                    for c in cats:
                        nm = c.get("name") if isinstance(c, dict) else None
                        cat_names.append(best_lang_text(nm))
                    category = ", ".join([x for x in cat_names if x])[:200]

                    products = h.get("products") or []
                    prod_names = []
                    for pr in products:
                        nm = pr.get("name") if isinstance(pr, dict) else None
                        prod_names.append(best_lang_text(nm))
                    products_txt = ", ".join([x for x in prod_names if x])[:400]

                    full_text = " ".join([name, country, desc, category, products_txt]).strip()

                    manu, evidence, confidence = is_manufacturer(full_text)

                    if request.manufacturers_only and not manu:
                        continue

                    exhibitors.append({
                        "empresa": name,
                        "pais": country or "No confirmado",
                        "que_fabrican": products_txt or "",
                        "categoria": category or "",
                        "evidencia_fabricante": evidence if manu else "Fabricante no confirmado",
                        "confianza": confidence if manu else "Baja",
                        "source": "algolia",
                    })

                    if len(exhibitors) >= request.max_exhibitors:
                        break

                # nbPages viene en bloque
                nb_pages = block.get("nbPages")
                page += 1
                if isinstance(nb_pages, int) and page >= nb_pages:
                    break

            return {
                "status": "ok",
                "mode": "algolia",
                "app_id_detected": True,
                "index_name": index_name,
                "container_id": container_id,
                "total_detected": len(exhibitors),
                "results": exhibitors
            }

        # 3) Fallback básico HTML (por si la feria no es Algolia/Easyfairs)
        # (No será perfecto para Easyfairs, pero te salva otras ferias)
        # ---------------------------------------------
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(request.url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(4000)

            # intenta sacar links / cards visibles
            cards = await page.query_selector_all("a, article, .card, .exhibitor, .stand")
            seen = set()

            for c in cards:
                try:
                    txt = (await c.inner_text()) or ""
                    txt_low = txt.lower()
                    if len(txt.strip()) < 10:
                        continue

                    # filtro país por texto (fallback)
                    if request.countries and not any(cc.lower() in txt_low for cc in request.countries):
                        continue

                    manu, evidence, confidence = is_manufacturer(txt)
                    if request.manufacturers_only and not manu:
                        continue

                    key = txt.strip()[:120]
                    if key in seen:
                        continue
                    seen.add(key)

                    exhibitors.append({
                        "empresa": txt.strip().split("\n")[0][:120],
                        "pais": "No confirmado",
                        "que_fabrican": "",
                        "categoria": "",
                        "evidencia_fabricante": evidence if manu else "Fabricante no confirmado",
                        "confianza": confidence if manu else "Baja",
                        "source": "html_fallback",
                    })

                    if len(exhibitors) >= request.max_exhibitors:
                        break
                except Exception:
                    continue

            await browser.close()

        return {
            "status": "ok",
            "mode": "html_fallback",
            "total_detected": len(exhibitors),
            "results": exhibitors
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
