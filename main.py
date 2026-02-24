# main.py
from __future__ import annotations

from typing import List, Any, Dict
from io import BytesIO
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, HttpUrl

from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

from easyfairs_widgets import (
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


def _scrape_impl(req: ScrapeRequest) -> ScrapeResponse:
    url = str(req.url)

    container_id = get_container_id_for_url(url)
    if container_id is None:
        raise HTTPException(
            status_code=400,
            detail=(
                "Esta URL no está en el mapping Easyfairs (dominio->containerId). "
                "Añádela en EASYFAIRS_CONTAINER_MAP en easyfairs_widgets.py"
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
            deep_profile=bool(req.deep_profile),
            polite_delay_s=0.0,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Easyfairs widgets scrape failed: {e}")

    results = [
        {
            "fabricante": r.get("name", "") or "",
            "actividad": r.get("activity", "") or "",
            "enlace_web": r.get("website", "") or "",
            "pais": r.get("country", "") or "",
        }
        for r in rows
    ]

    return ScrapeResponse(url=url, total=len(results), results=results, meta=meta)


def _build_excel(results: List[Dict[str, Any]], *, sheet_name: str = "Fabricantes") -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name

    headers = ["Fabricante", "Actividad", "Enlace Web", "País"]
    ws.append(headers)

    for r in results:
        ws.append(
            [
                r.get("fabricante", "") or "",
                r.get("actividad", "") or "",
                r.get("enlace_web", "") or "",
                r.get("pais", "") or "",
            ]
        )

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{ws.max_row}"

    tab = Table(displayName="TablaFabricantes", ref=f"A1:{get_column_letter(len(headers))}{ws.max_row}")
    tab.tableStyleInfo = TableStyleInfo(
        name="TableStyleMedium9",
        showFirstColumn=False,
        showLastColumn=False,
        showRowStripes=True,
        showColumnStripes=False,
    )
    ws.add_table(tab)

    # Hipervínculos + ancho columnas
    col_widths = [len(h) for h in headers]
    for row_idx in range(2, ws.max_row + 1):
        cell = ws.cell(row=row_idx, column=3)
        val = (cell.value or "").strip()
        if val:
            url = val
            if url.startswith("www."):
                url = "https://" + url
            if url.startswith("http://") or url.startswith("https://"):
                cell.hyperlink = url
                cell.style = "Hyperlink"

        for col_idx in range(1, len(headers) + 1):
            v = ws.cell(row=row_idx, column=col_idx).value
            v = "" if v is None else str(v)
            col_widths[col_idx - 1] = max(col_widths[col_idx - 1], min(len(v), 80))

    for i, w in enumerate(col_widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = max(12, min(w + 2, 60))

    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()


def _excel_filename(prefix: str = "fabricantes") -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}.xlsx"


@app.post("/scrape")
def scrape_excel_always(req: ScrapeRequest):
    """
    SIEMPRE devuelve un Excel.
    """
    resp = _scrape_impl(req)
    xlsx_bytes = _build_excel(resp.results)

    filename = _excel_filename("fabricantes")
    return StreamingResponse(
        BytesIO(xlsx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/scrape_json", response_model=ScrapeResponse)
def scrape_json(req: ScrapeRequest):
    """
    Endpoint opcional para depurar: devuelve JSON.
    """
    return _scrape_impl(req)
