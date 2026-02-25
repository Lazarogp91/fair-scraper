# main.py
from typing import List, Any, Dict
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, HttpUrl

from easyfairs_widgets import (
    is_easyfairs_supported,
    get_container_id_for_url,
    fetch_easyfairs_stands_by_countries,
)

app = FastAPI(title="Fair Scraper API", version="1.0.2")


class ScrapeRequest(BaseModel):
    url: HttpUrl
    countries: List[str] = Field(default_factory=list)
    manufacturers_only: bool = True
    max_pages: int = 20
    timeout_ms: int = 25000
    deep_profile: bool = False
    debug: bool = False


class ScrapeResponse(BaseModel):
    url: str
    total: int
    results: List[Dict[str, Any]]
    meta: Dict[str, Any] = Field(default_factory=dict)


@app.get("/health")
def health():
    return {"status": "ok"}


def _normalize_countries(countries: List[str]) -> List[str]:
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

    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


@app.post("/scrape", response_model=ScrapeResponse)
def scrape(req: ScrapeRequest):
    url = str(req.url)

    if not is_easyfairs_supported(url):
        raise HTTPException(
            status_code=400,
            detail="URL no soportada por el scraper Easyfairs (dominio no está en EASYFAIRS_CONTAINER_MAP).",
        )

    container_id = get_container_id_for_url(url)
    if not container_id:
        raise HTTPException(
            status_code=400,
            detail="URL parece Easyfairs, pero no hay containerId en el mapping. Añádelo a EASYFAIRS_CONTAINER_MAP.",
        )

    countries_norm = _normalize_countries(req.countries)
    timeout_s = max(5, int(req.timeout_ms / 1000))

    try:
        rows, meta = fetch_easyfairs_stands_by_countries(
            event_url=url,
            container_id=container_id,
            countries=countries_norm,
            lang="es",
            query_seed="a",
            hits_per_page=100,
            timeout_s=timeout_s,
            max_pages=req.max_pages if req.max_pages > 0 else None,
        )
    except Exception as e:
        # Aquí vas a ver el HTTP code y body si era 403/429/etc.
        raise HTTPException(status_code=502, detail=f"Easyfairs scrape failed: {e}")

    out = []
    for r in rows:
        out.append(
            {
                "fabricante": r.get("name", ""),
                "actividad": r.get("activity", ""),
                "enlace_web": r.get("website", ""),
                "pais": r.get("country", ""),
            }
        )

    return ScrapeResponse(url=url, total=len(out), results=out, meta=meta)
