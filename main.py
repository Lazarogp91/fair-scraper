# main.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response, JSONResponse
from pydantic import BaseModel, Field, HttpUrl

from io import BytesIO
from openpyxl import Workbook
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.styles import Font, Alignment
from openpyxl.utils import get_column_letter

from scrapers import ScrapeConfig, scrape_any, normalize_countries


app = FastAPI(title="Fair Scraper API", version="2.0.0")


class ScrapeRequest(BaseModel):
    url: HttpUrl
    countries: List[str] = Field(default_factory=list)  # admite ES/PT o Spain/Portugal
    max_pages: int = 20
    timeout_ms: int = 25000
    debug: bool = False


class Company(BaseModel):
    fabricante: str = ""
    actividad: str = ""
    enlace_web: str = ""
    pais: str = ""


class ScrapeResponse(BaseModel):
    url: str
    total: int
    results: List[Dict[str, Any]]
    meta: Dict[str, Any] = Field(default_factory=dict)


@app.get("/health")
def health():
    return {"status": "ok"}


def _build_excel(results: List[Dict[str, Any]], url: str, meta: Dict[str, Any]) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "Empresas"

    # ---- ORDENAR POR PAÍS Y FABRICANTE ----
    sorted_results = sorted(
        results,
        key=lambda x: (
            (x.get("pais") or "").lower(),
            (x.get("fabricante") or "").lower(),
        ),
    )

    headers = ["Fabricante", "Actividad", "Enlace Web", "País"]
    ws.append(headers)

    # ---- Estilo cabecera ----
    header_font = Font(bold=True)
    for col in range(1, len(headers) + 1):
        cell = ws.cell(row=1, column=col)
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    # ---- Insertar datos ----
    for item in sorted_results:
        ws.append(
            [
                item.get("fabricante", ""),
                item.get("actividad", ""),
                item.get("enlace_web", ""),
                item.get("pais", ""),
            ]
        )

    last_row = ws.max_row
    last_col = ws.max_column

    # ---- Convertir en TABLA estructurada ----
    table_ref = f"A1:{get_column_letter(last_col)}{last_row}"
    table = Table(displayName="EmpresasTable", ref=table_ref)

    style = TableStyleInfo(
        name="TableStyleMedium9",
        showFirstColumn=False,
        showLastColumn=False,
        showRowStripes=True,
        showColumnStripes=False,
    )
    table.tableStyleInfo = style
    ws.add_table(table)

    # ---- Freeze panes ----
    ws.freeze_panes = "A2"

    # ---- Auto ancho columnas ----
    for col in range(1, last_col + 1):
        max_len = 0
        col_letter = get_column_letter(col)
        for row in range(1, last_row + 1):
            val = ws.cell(row=row, column=col).value
            if val:
                max_len = max(max_len, len(str(val)))
        ws.column_dimensions[col_letter].width = min(max_len + 3, 60)

    # ---- Hoja meta ----
    ws2 = wb.create_sheet("Meta")
    ws2.append(["Campo", "Valor"])
    ws2["A1"].font = header_font
    ws2["B1"].font = header_font
    ws2.append(["URL", url])
    ws2.append(["Total empresas", str(len(sorted_results))])

    # dump meta (ligero)
    ws2.append(["Driver", str(meta.get("driver", ""))])
    ws2.append(["Supported", str(meta.get("supported", ""))])

    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()


@app.post("/scrape_json", response_model=ScrapeResponse)
def scrape_json(req: ScrapeRequest):
    url = str(req.url)

    countries_norm = normalize_countries(req.countries)
    cfg = ScrapeConfig(
        countries=countries_norm,
        timeout_s=max(5, int(req.timeout_ms / 1000)),
        max_pages=req.max_pages,
        debug=req.debug,
    )

    results, meta = scrape_any(url, cfg)

    if not results:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "No se pudo extraer ningún expositor con los drivers disponibles.",
                "meta": meta,
            },
        )

    return ScrapeResponse(url=url, total=len(results), results=results, meta=meta)


@app.post(
    "/scrape",
    summary="Extrae expositores y devuelve Excel",
    responses={
        200: {"content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}}}
    },
)
def scrape_excel(req: ScrapeRequest):
    url = str(req.url)

    countries_norm = normalize_countries(req.countries)
    cfg = ScrapeConfig(
        countries=countries_norm,
        timeout_s=max(5, int(req.timeout_ms / 1000)),
        max_pages=req.max_pages,
        debug=req.debug,
    )

    results, meta = scrape_any(url, cfg)

    if not results:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "No se pudo extraer ningún expositor con los drivers disponibles.",
                "meta": meta,
            },
        )

    xlsx = _build_excel(results=results, url=url, meta=meta)
    filename = "fabricantes.xlsx"

    return Response(
        content=xlsx,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
