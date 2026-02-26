from __future__ import annotations
# scrapers.py
from playwright_scraper import scrape_with_playwright
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import re
import requests

from easyfairs_widgets import (
    is_easyfairs_supported,
    get_container_id_for_url,
    fetch_easyfairs_stands_by_countries,
)

# -----------------------------
# Tipos
# -----------------------------
Row = Dict[str, Any]
Meta = Dict[str, Any]


@dataclass
class ScrapeConfig:
    countries: List[str]
    lang: str = "es"
    timeout_s: int = 25
    max_pages: int = 20
    query_seed: str = "a"
    hits_per_page: int = 100
    debug: bool = False


# -----------------------------
# Helpers genéricos
# -----------------------------
def normalize_countries(countries: List[str]) -> List[str]:
    # Normaliza a nombres típicos en directorios internacionales
    if not countries:
        return ["Spain", "Portugal"]

    out: List[str] = []
    for c in countries:
        cc = (c or "").strip()
        if not cc:
            continue
        u = cc.upper()
        if u in ("ES", "ESP", "ESPAÑA", "SPAIN"):
            out.append("Spain")
        elif u in ("PT", "PRT", "PORTUGAL"):
            out.append("Portugal")
        else:
            out.append(cc)

    # dedupe
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def _host(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""


# -----------------------------
# Driver 1: Easyfairs
# -----------------------------
def scrape_easyfairs(url: str, cfg: ScrapeConfig) -> Tuple[List[Row], Meta]:
    if not is_easyfairs_supported(url):
        return [], {"supported": False, "driver": "easyfairs"}

    container_id = get_container_id_for_url(url)
    if not container_id:
        return [], {
            "supported": False,
            "driver": "easyfairs",
            "reason": "domain supported pero sin containerId mapping",
        }

    countries_norm = normalize_countries(cfg.countries)

    rows, meta = fetch_easyfairs_stands_by_countries(
        event_url=url,
        container_id=container_id,
        countries=countries_norm,
        lang=cfg.lang,
        query_seed=cfg.query_seed,
        hits_per_page=cfg.hits_per_page,
        timeout_s=cfg.timeout_s,
        max_pages=cfg.max_pages if cfg.max_pages > 0 else None,
    )

    # Normalizamos al “modelo” de tu API
    out: List[Row] = []
    for r in rows:
        out.append(
            {
                "fabricante": r.get("name", "") or "",
                "actividad": r.get("activity", "") or "",
                "enlace_web": r.get("website", "") or "",
                "pais": r.get("country", "") or "",
            }
        )

    meta2: Meta = {"driver": "easyfairs", "supported": True}
    meta2.update(meta or {})
    return out, meta2


# -----------------------------
# Driver 2: HTML estático (muy básico)
# - Útil para directorios sencillos que tienen cards con nombre/país/web
# - No es universal, pero sirve como “segundo intento”
# -----------------------------
_NAME_PATTERNS = [
    re.compile(r'exhibitor|exhibitors|expositor|expositores|companies|empresas', re.I),
]

def _looks_like_exhibitors_page(html: str) -> bool:
    if not html:
        return False
    for p in _NAME_PATTERNS:
        if p.search(html):
            return True
    return False


def scrape_static_html(url: str, cfg: ScrapeConfig) -> Tuple[List[Row], Meta]:
    """
    Scraper HTML simple: intenta encontrar bloques típicos:
    - <a ...>Nombre</a> en cards
    - o listas con items
    Este driver NO garantiza éxito; es fallback.
    """
    headers = {
        "user-agent": "Mozilla/5.0",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        r = requests.get(url, headers=headers, timeout=cfg.timeout_s)
    except Exception as e:
        return [], {"driver": "static_html", "supported": False, "error": str(e)}

    if r.status_code >= 400:
        return [], {
            "driver": "static_html",
            "supported": False,
            "http_status": r.status_code,
            "body_preview": (r.text or "")[:400],
        }

    html = r.text or ""
    if not _looks_like_exhibitors_page(html):
        return [], {"driver": "static_html", "supported": False, "reason": "no parece directorio"}

    # Heurística MUY conservadora: buscar anchors con texto “largo” como posible nombre
    # y quedarnos con un conjunto único.
    candidates = set()
    for m in re.finditer(r"<a[^>]*>([^<]{2,120})</a>", html, flags=re.I):
        text = re.sub(r"\s+", " ", (m.group(1) or "").strip())
        if 3 <= len(text) <= 80:
            # evitamos menús típicos
            if any(x in text.lower() for x in ["home", "inicio", "about", "contact", "privacy", "cookies"]):
                continue
            candidates.add(text)

    # Convertimos a rows sin país (no siempre hay)
    results: List[Row] = []
    for name in sorted(candidates)[:2000]:
        results.append({"fabricante": name, "actividad": "", "enlace_web": "", "pais": ""})

    return results, {
        "driver": "static_html",
        "supported": True,
        "note": "fallback heurístico; puede incluir falsos positivos",
        "count_candidates": len(results),
    }


# -----------------------------
# Autodetección: orden de preferencia
# -----------------------------
def scrape_any(url: str, cfg: ScrapeConfig) -> Tuple[List[Row], Meta]:
    # 1) Easyfairs (rápido y preciso)
    res, meta = scrape_easyfairs(url, cfg)
    if meta.get("supported") and res:
        return res, meta

    # 2) HTML estático (fallback)
    res2, meta2 = scrape_static_html(url, cfg)
    if meta2.get("supported") and res2:
        # Añadimos info del intento anterior si debug
        if cfg.debug:
            meta2["previous_driver"] = meta
        return res2, meta2

    # 3) Si quieres: aquí es donde meterías un driver Playwright (JS)
    #    Lo dejo preparado para que lo añadamos cuando tú quieras.

    meta_out: Meta = {
        "driver": "none",
        "supported": False,
        "reason": "No se detectó un driver soportado o no se obtuvieron resultados",
        "attempts": [meta, meta2],
        "host": _host(url),
    }
    # 3) Playwright fallback (JS dinámico)
    res3, meta3 = scrape_with_playwright(url, timeout_s=cfg.timeout_s)
    if res3:
        return res3, meta3
    return [], meta_out
