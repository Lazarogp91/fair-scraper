# main.py
from __future__ import annotations

from typing import List, Any, Dict, Optional, Tuple
from io import BytesIO

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, HttpUrl

from openpyxl import Workbook
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.styles import Font, Alignment
from openpyxl.utils import get_column_letter

from easyfairs_widgets import (
    is_easyfairs_supported,
    get_container_id_for_url,
    fetch_easyfairs_stands_by_countries,
)

app = FastAPI(title="Fair Scraper API", version="1.2.1")


class ScrapeRequest(BaseModel):
    url: HttpUrl
    countries: List[str] = Field(default_factory=list)  # ["ES","PT"] o ["Spain","Portugal"]
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
    uniq: List[str] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def _build_excel(results: List[Dict[str, Any]], url: str) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "Empresas"

    # Ordenar por país y fabricante
    sorted_results = sorted(
        results,
        key=lambda x: (
            (x.get("pais") or "").lower(),
            (x.get("fabricante") or "").lower(),
        ),
    )

    headers = ["Fabricante", "Actividad", "Enlace Web", "País"]
    ws.append(headers)

    # Cabecera en negrita y centrada
    header_font = Font(bold=True)
    for col in range(1, len(headers) + 1):
        cell = ws.cell(row=1, column=col)
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    # Datos
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

    # Tabla estructurada
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

    # Freeze pane
    ws.freeze_panes = "A2"

    # Auto ancho columnas
    for col in range(1, last_col + 1):
        max_len = 0
        col_letter = get_column_letter(col)
        for row in range(1, last_row + 1):
            val = ws.cell(row=row, column=col).value
            if val:
                max_len = max(max_len, len(str(val)))
        ws.column_dimensions[col_letter].width = min(max_len + 3, 60)

    # Hoja Meta
    ws2 = wb.create_sheet("Meta")
    ws2.append(["Campo", "Valor"])
    ws2["A1"].font = header_font
    ws2["B1"].font = header_font
    ws2.append(["URL", url])
    ws2.append(["Total empresas", str(len(sorted_results))])

    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()


def _scrape_easyfairs(req: ScrapeRequest) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    url = str(req.url)

    if not is_easyfairs_supported(url):
        raise HTTPException(
            status_code=400,
            detail="Esta URL no está soportada (no está en EASYFAIRS_CONTAINER_MAP).",
        )

    container_id = get_container_id_for_url(url)
    if not container_id:
        raise HTTPException(status_code=400, detail="No hay containerId para este dominio.")

    countries_norm = _normalize_countries(req.countries)
    timeout_s = max(5, int(req.timeout_ms / 1000))

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

    results: List[Dict[str, Any]] = []
    for r in rows:
        results.append(
            {
                "fabricante": r.get("name", ""),
                "actividad": r.get("activity", ""),
                "enlace_web": r.get("website", ""),
                "pais": r.get("country", ""),
            }
        )

    return results, meta


@app.post("/scrape_json", response_model=ScrapeResponse)
def scrape_json(req: ScrapeRequest):
    results, meta = _scrape_easyfairs(req)
    return ScrapeResponse(url=str(req.url), total=len(results), results=results, meta=meta)


@app.post("/scrape")
def scrape_excel(req: ScrapeRequest):
    results, _meta = _scrape_easyfairs(req)
    xlsx_bytes = _build_excel(results, url=str(req.url))

    filename = "fabricantes_es_pt.xlsx"
    return StreamingResponse(
        BytesIO(xlsx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
