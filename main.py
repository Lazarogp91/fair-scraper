# main.py
from typing import List, Optional, Any, Dict
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, HttpUrl

from easyfairs_widgets import (
    is_easyfairs_supported,
    get_container_id_for_url,
    fetch_easyfairs_stands,
)

app = FastAPI(title="Fair Scraper API", version="1.0.0")


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


@app.post("/scrape", response_model=ScrapeResponse)
def scrape(req: ScrapeRequest):
    url = str(req.url)

    # 1) RUTA EASYFAIRS (la que acabamos de confirmar)
    if is_easyfairs_supported(url):
        container_id = get_container_id_for_url(url)
        if not container_id:
            raise HTTPException(
                status_code=400,
                detail="URL parece Easyfairs, pero no hay containerId en el mapping. Añádelo a EASYFAIRS_CONTAINER_MAP.",
            )

        try:
            rows, meta = fetch_easyfairs_stands(
                event_url=url,
                container_id=container_id,
                lang="es",
                # query_seed: se usa para “activar” resultados; puede ser "a" o vacío.
                query_seed="a",
                hits_per_page=100,
                timeout_s=max(5, int(req.timeout_ms / 1000)),
                max_pages=req.max_pages if req.max_pages > 0 else None,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Easyfairs widgets scrape failed: {e}")

        # Normalizar salida EXACTA a lo que quieres (fabricante, actividad, web, país)
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

    # 2) FALLBACK (si tienes otros scrapers, aquí enganchas tu lógica actual)
    # Si no tienes nada más implementado aún, devolvemos error claro.
    raise HTTPException(
        status_code=400,
        detail=(
            "Esta URL no está en el mapping Easyfairs. "
            "Si es otra feria Easyfairs, añade el dominio->containerId. "
            "Si es otro tipo de directorio, implementa el scraper correspondiente."
        ),
    )
