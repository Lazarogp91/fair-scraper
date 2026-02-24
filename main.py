# main.py
from __future__ import annotations

from typing import List, Optional, Any, Dict, Tuple
from io import BytesIO

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, HttpUrl

from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from easyfairs_widgets import (
    is_easyfairs_supported,
    get_container_id_for_url,
    fetch_easyfairs_stands_by_countries,
)

app = FastAPI(title="Fair Scraper API", version="1.2.0")


class ScrapeRequest(BaseModel):
    url: HttpUrl
    countries: List[str] = Field(default_factory=list)  # ["Spain","Portugal"] o ["ES","PT"]
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
    """
    Normaliza códigos típicos a nombres que usa el facet de Easyfairs.
    """
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

    # dedupe preservando orden
    seen = set()
    uniq: List[str] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def _rows_to_excel_bytes(url: str, rows: List[Dict[str, Any]]) -> bytes:
    """
    Genera un .xlsx en memoria con columnas:
    Fabricante | Actividad | Enlace Web | País
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "Fabricantes"

    headers = ["Fabricante", "Actividad", "Enlace Web", "País"]
    ws.append(headers)

    for r in rows:
        ws.append(
            [
                r.get("fabricante", "") or "",
                r.get("actividad", "") or "",
                r.get("enlace_web", "") or "",
                r.get("pais", "") or "",
            ]
        )

    # Formato mínimo: ancho de columnas
    for col_idx, h in enumerate(headers, start=1):
        max_len = len(h)
        for row_idx in range(2, ws.max_row + 1):
            val = ws.cell(row=row_idx, column=col_idx).value
            if val is None:
                continue
            max_len = max(max_len, len(str(val)))
        ws.column_dimensions[get_column_letter(col_idx)].width = min(60, max(12, max_len + 2))

    # Guardar a bytes
    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()


def _scrape_logic(req: ScrapeRequest) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    url = str(req.url)

    if not is_easyfairs_supported(url):
        raise HTTPException(
            status_code=400,
            detail=(
                "Esta URL no está soportada como Easyfairs Widgets. "
                "Si es otra feria Easyfairs, añade el dominio->containerId en el mapping."
            ),
        )

    container_id = get_container_id_for_url(url)
    if not container_id:
        raise HTTPException(
            status_code=400,
            detail="URL parece Easyfairs, pero no hay containerId en el mapping. Añádelo a EASYFAIRS_CONTAINER_MAP.",
        )

    countries_norm = _normalize_countries(req.countries)

    try:
        rows_raw, meta = fetch_easyfairs_stands_by_countries(
            event_url=url,
            container_id=container_id,
            countries=countries_norm,
            lang="es",
            query_seed="a",
            hits_per_page=100,
            timeout_s=max(5, int(req.timeout_ms / 1000)),
            max_pages=req.max_pages if req.max_pages > 0 else None,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Easyfairs widgets scrape failed: {e}")

    # Transformación a tu formato
    out: List[Dict[str, Any]] = []
    for r in rows_raw:
        out.append(
            {
                "fabricante": r.get("name", "") or "",
                "actividad": r.get("activity", "") or "",
                "enlace_web": r.get("website", "") or "",
                "pais": r.get("country", "") or "",
            }
        )

    return out, meta


@app.post("/scrape")
def scrape_excel(req: ScrapeRequest):
    """
    Devuelve SIEMPRE Excel binario (.xlsx)
    """
    rows, meta = _scrape_logic(req)

    xlsx_bytes = _rows_to_excel_bytes(str(req.url), rows)
    filename = "fabricantes_es_pt.xlsx"

    return StreamingResponse(
        BytesIO(xlsx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/scrape_json", response_model=ScrapeResponse)
def scrape_json(req: ScrapeRequest):
    """
    Endpoint opcional que devuelve JSON
    """
    rows, meta = _scrape_logic(req)
    return ScrapeResponse(url=str(req.url), total=len(rows), results=rows, meta=meta)
