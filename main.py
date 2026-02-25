from typing import List, Dict, Any
from io import BytesIO

from openpyxl import Workbook
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.styles import Font, Alignment
from openpyxl.utils import get_column_letter


def _build_excel(results: List[Dict[str, Any]], url: str) -> bytes:
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
        name="TableStyleMedium9",  # Profesional azul/gris
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
            if val is not None:
                max_len = max(max_len, len(str(val)))
        ws.column_dimensions[col_letter].width = min(max_len + 3, 60)

    # ---- Hoja meta profesional ----
    ws2 = wb.create_sheet("Meta")
    ws2.append(["Campo", "Valor"])
    ws2["A1"].font = header_font
    ws2["B1"].font = header_font
    ws2.append(["URL", url])
    ws2.append(["Total empresas", str(len(sorted_results))])

    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()
