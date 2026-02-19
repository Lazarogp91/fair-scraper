import json
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

app = FastAPI(title="Fair Scraper API", version="3.1.0")


class ScrapeRequest(BaseModel):
    url: str
    countries: list[str] = Field(default_factory=lambda: ["Spain", "Portugal"])
    manufacturers_only: bool = True
    max_pages: int = 50
    max_exhibitors: int = 2000


# -------------------------
# Helpers
# -------------------------

def clean_text(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "").strip())


def want_country(country: str, allow: list[str]) -> bool:
    if not country:
        return False
    allow_norm = {a.strip().lower() for a in allow}
    return country.strip().lower() in allow_norm


def is_manufacturer_strict(text: str) -> Tuple[bool, str, str]:
    """
    Devuelve: (is_manufacturer, confidence, evidence_snippet)
    Estricto: si no hay evidencia, NO se considera fabricante.
    """
    t = (text or "").lower()

    positive_patterns = [
        r"\bfabricante\b",
        r"\bfabricación\b",
        r"\bfabricacion\b",
        r"\bmanufacturer\b",
        r"\bmanufacturing\b",
        r"\bproducción\b",
        r"\bproduccion\b",
        r"\bproduction\b",
        r"\boem\b",
        r"\bdiseñamos y producimos\b",
        r"\bdesarrolla y fabrica\b",
        r"\bdiseña y fabrica\b",
        r"\bdesigns? and manufactures?\b",
        r"\bwe manufacture\b",
        r"\bown production\b",
        r"\bplanta de producción\b",
        r"\bplanta de produccion\b",
    ]

    negative_patterns = [
        r"\bintegrador(?:es)?\b",
        r"\bintegración\b",
        r"\bintegracion\b",
        r"\bconsultor(?:ía|ia)\b",
        r"\bconsulting\b",
        r"\bdistribuidor(?:es)?\b",
        r"\bdistribución\b",
        r"\bdistribucion\b",
        r"\breseller\b",
        r"\bservices?\b",
        r"\bservicios\b",
        r"\bpartner\b",
    ]

    # Si se declara explícitamente como distribuidor/integrador/servicios -> NO
    if any(re.search(p, t) for p in negative_patterns):
        return False, "Baja", ""

    # Evidencia positiva explícita -> SÍ
    for p in positive_patterns:
        m = re.search(p, t)
        if m:
            start = max(0, m.start() - 60)
            end = min(len(text), m.end() + 120)
            snippet = clean_text(text[start:end])
            return True, "Alta", snippet

    return False, "Baja", ""


def extract_container_id(payload: dict) -> Optional[int]:
    """
    Busca containerId en filtros tipo "(containerId: 2653)" o en params.
    """
    # caso directo
    f = payload.get("filters")
    if isinstance(f, str):
        m = re.search(r"containerId\s*:\s*(\d+)", f)
        if m:
            return int(m.group(1))

    # caso en params (string URL-encoded)
    params = payload.get("params")
    if isinstance(params, str):
        m = re.search(r"containerId%3A\+(\d+)", params) or re.search(r"containerId:\+(\d+)", params)
        if m:
            return int(m.group(1))

    return None


# -------------------------
# Algolia detection by network sniffing (robusto)
# -------------------------

ALGOLIA_QUERY_RE = re.compile(r"/1/indexes/([^/]+)/query")


def detect_algolia_from_network(url: str, timeout_ms: int = 15000) -> Dict[str, Any]:
    """
    Abre la web y captura la primera request POST a Algolia /query.
    Devuelve {appId, apiKey, indexName, containerId, templatePayload}
    """
    captured = {"found": False}
    result: Dict[str, Any] = {}

    def on_request(req):
        try:
            if req.method != "POST":
                return
            u = req.url
            if "algolia.net/1/indexes/" not in u or not u.endswith("/query"):
                return

            m = ALGOLIA_QUERY_RE.search(u)
            if not m:
                return
            index_name = m.group(1)

            # appId desde el host: <APPID>-dsn.algolia.net
            host = urlparse(u).netloc
            app_id = host.split("-")[0].upper() if "-" in host else ""

            headers = {k.lower(): v for k, v in (req.headers or {}).items()}
            api_key = headers.get("x-algolia-api-key", "")
            hdr_app = headers.get("x-algolia-application-id", "")
            if hdr_app:
                app_id = hdr_app

            post_data = req.post_data or ""
            payload = {}
            try:
                payload = json.loads(post_data) if post_data else {}
            except Exception:
                payload = {}

            container_id = extract_container_id(payload)

            # guardamos la primera que parezca la buena (stands suele contener "stands")
            score = 0
            if "stands" in index_name.lower():
                score += 10
            if container_id:
                score += 5
            if api_key:
                score += 2

            # si no hemos guardado ninguna o esta es mejor
            prev_score = captured.get("score", -1)
            if score > prev_score:
                captured["score"] = score
                captured["found"] = True
                result.update(
                    {
                        "appId": app_id,
                        "apiKey": api_key,
                        "indexName": index_name,
                        "containerId": container_id,
                        "templatePayload": payload,
                    }
                )
        except Exception:
            return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.on("request", on_request)

        try:
            page.goto(url, wait_until="networkidle", timeout=60000)
            page.wait_for_timeout(timeout_ms)
        except PWTimeoutError:
            pass
        except Exception:
            pass
        finally:
            try:
                browser.close()
            except Exception:
                pass

    # validación mínima
    if not result.get("appId") or not result.get("apiKey") or not result.get("indexName") or not result.get("containerId"):
        return {}

    return result


def algolia_query(app_id: str, api_key: str, index_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"https://{app_id}-dsn.algolia.net/1/indexes/{index_name}/query"
    headers = {
        "X-Algolia-Application-Id": app_id,
        "X-Algolia-API-Key": api_key,
        "Content-Type": "application/json",
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Algolia error {r.status_code}: {r.text[:300]}")
    return r.json()


def scrape_via_algolia(req: ScrapeRequest, conf: Dict[str, Any]) -> Dict[str, Any]:
    app_id = conf["appId"]
    api_key = conf["apiKey"]
    index_name = conf["indexName"]
    container_id = conf["containerId"]

    allow = req.countries[:]
    exhibitors: List[Dict[str, Any]] = []
    errors: List[str] = []

    page_num = 0
    nb_pages = 1
    hits_per_page = 15

    while page_num < nb_pages and page_num < req.max_pages:
        payload = {
            "query": "",
            "page": page_num,
            "hitsPerPage": hits_per_page,
            "facets": ["country", "categories.name"],
            "filters": f"(containerId: {container_id})",
        }
        # filtro ES/PT directamente en Algolia (si el campo country existe)
        if allow:
            payload["facetFilters"] = [[f"country:{c}" for c in allow]]

        data = algolia_query(app_id, api_key, index_name, payload)

        nb_pages = int(data.get("nbPages", 0) or 0)
        hits = data.get("hits", []) or []

        if not hits:
            break

        for h in hits:
            name = clean_text(h.get("name") or "")
            if not name:
                continue

            country = clean_text(h.get("country") or "")
            if allow and not want_country(country, allow):
                continue

            # description (prioriza ES)
            desc = ""
            d = h.get("description") or {}
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

            # fabricante estricto
            is_manu, conf_lvl, evidence = is_manufacturer_strict(desc)
            if req.manufacturers_only and not is_manu:
                continue

            exhibitors.append(
                {
                    "company_name": name,
                    "country": country,
                    "description": desc[:700],
                    "categories": cats[:10],
                    "standNumber": clean_text(h.get("standNumber") or ""),
                    "manufacturer_confidence": conf_lvl,
                    "manufacturer_evidence": evidence,
                }
            )

            if len(exhibitors) >= req.max_exhibitors:
                break

        if len(exhibitors) >= req.max_exhibitors:
            break

        page_num += 1

    return {
        "mode": "algolia",
        "algolia": {"indexName": index_name, "containerId": container_id},
        "exhibitors": exhibitors,
        "errors": errors,
    }


# -------------------------
# Endpoint
# -------------------------

@app.post("/scrape")
def scrape(req: ScrapeRequest):
    if not req.url.startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="Invalid url")

    # 1) Detectar Algolia por red (robusto)
    alg_conf = detect_algolia_from_network(req.url)
    if alg_conf:
        out = scrape_via_algolia(req, alg_conf)
        out["source_url"] = req.url
        out["total"] = len(out["exhibitors"])
        return out

    # 2) Si no detecta Algolia, devolvemos mensaje claro
    # (Aquí podrías añadir más “drivers” para otros formatos: CSV, PDF, APIs, etc.)
    raise HTTPException(
        status_code=422,
        detail="No se ha detectado un catálogo tipo Algolia/Easyfairs desde la URL. "
               "La web puede requerir login, bloquear scraping, o usar una API privada distinta.",
    )
