# main.py
from typing import List, Any, Dict
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, HttpUrl

from easyfairs_widgets import (
    get_container_id_for_url,
    fetch_easyfairs_stands_by_countries,
)

app = FastAPI(title="Fair Scraper API", version="1.0.2")


class ScrapeRequest(BaseModel):
    url: HttpUrl
    countries: List[str] = Field(default_factory=list)  # admite ["Spain","Portugal"] o ["ES","PT"]
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

    # dedupe
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

    # Detecta Easyfairs por mapping dominio->containerId
    container_id = get_container_id_for_url(url)
    if container_id is None:
        raise HTTPException(
            status_code=400,
            detail=(
                "Esta URL no está en el mapping Easyfairs. "
                "Si es otra feria Easyfairs, añade el dominio->containerId en EASYFAIRS_CONTAINER_MAP."
            ),
        )

    countries_norm = _normalize_countries(req.countries)

    try:
        rows, meta = fetch_easyfairs_stands_by_countries(
            event_url=url,
            container_id=container_id,
            countries=countries_norm,
            lang="es",
            query_seed="a",
            hits_per_page=100,
            timeout_s=max(5, int(req.timeout_ms / 1000)),
            max_pages=req.max_pages if req.max_pages and req.max_pages > 0 else None,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Easyfairs widgets scrape failed: {e}")

    results = [
        {
            "fabricante": r.get("name", ""),
            "actividad": r.get("activity", ""),
            "enlace_web": r.get("website", ""),
            "pais": r.get("country", ""),  # <- aquí ya debe venir Spain/Portugal por el filtro
        }
        for r in rows
    ]

    return ScrapeResponse(url=url, total=len(results), results=results, meta=meta)
