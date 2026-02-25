# main.py
from __future__ import annotations

from typing import List, Any, Dict, Tuple
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
    countries: List[str] = Field(default_factory=list)
    manufacturers_only: bool = True
    max_pages: int = 20
    timeout_ms: int = 25000
    deep_profile: bool = False
    debug: bool = False


class Company(BaseModel):
    fabricante: str = ""
    actividad: str = ""
    enlace_web: str = ""
    pais: str = ""


class ScrapeResponse(BaseModel):
    url: str
    total: int
    results: List[Company]
    meta: Dict[str, Any] = Field(default_factory=dict)
    excel_endpoint: str = "/scrape_excel"  # para recordarte el endpoint de exportación


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


def _scrape_easyfairs(req: ScrapeRequest) -> Tuple[str, List[Dict[str, Any]], Dict[str, Any]]:
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
        raise HTTPException(status_code=502, detail=f"Easyfairs scrape failed: {e}")

    results: List[Dict[str, Any]] = []
    for r in rows:
        results.append(
            {
                "fabricante": r.get("name", "") or "",
                "actividad": r.get("activity", "") or "",
                "enlace_web": r.get("website", "") or "",
                "pais": r.get("country", "") or "",
            }
        )

    return url, results, meta


def _build_excel_bytes(url: str, results: List[Dict[str, Any]], meta: Dict[str, Any]) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "Empresas"

    headers = ["Fabricante", "Actividad", "Enlace Web", "País"]
    ws.append(headers)

    for r in results:
        ws.append(
            [
                r.get("fabricante", ""),
                r.get("actividad", ""),
                r.get("enlace_web", ""),
                r.get("pais", ""),
            ]
        )

    # auto-ancho simple
    for col_idx in range(1, len(headers) + 1):
        col_letter = get_column_letter(col_idx)
        max_len = 0
        for cell in ws[col_letter]:
            v = "" if cell.value is None else str(cell.value)
            if len(v) > max_len:
                max_len = len(v)
        ws.column_dimensions[col_letter].width = min(max_len + 2, 80)

    # hoja meta (opcional)
    ws2 = wb.create_sheet("Meta")
    ws2.append(["url", url])
    for k, v in (meta or {}).items():
        ws2.append([str(k), str(v)])

    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()


@app.post("/scrape", response_model=ScrapeResponse, summary="Ver en pantalla (JSON)")
def scrape_json(req: ScrapeRequest):
    """
    Devuelve el listado para verlo en pantalla.
    """
    url, results, meta = _scrape_easyfairs(req)
    companies = [Company(**r) for r in results]
    return ScrapeResponse(url=url, total=len(companies), results=companies, meta=meta)


@app.post(
    "/scrape_excel",
    summary="Descargar Excel (.xlsx)",
    response_class=StreamingResponse,
)
def scrape_excel(req: ScrapeRequest):
    """
    Devuelve SIEMPRE un archivo Excel.
    """
    url, results, meta = _scrape_easyfairs(req)

    xlsx_bytes = _build_excel_bytes(url, results, meta)
    filename = "fabricantes.xlsx"

    return StreamingResponse(
        BytesIO(xlsx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
