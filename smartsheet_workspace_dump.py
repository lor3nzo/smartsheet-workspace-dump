"""
Smartsheet Full Workspace Dump → Excel
Requirements: pip install smartsheet-python-sdk openpyxl pandas python-dotenv
Usage: python smartsheet_workspace_dump.py
"""

from dotenv import load_dotenv
load_dotenv()

import smartsheet
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter
from datetime import datetime
import re
import os
import shutil
import sys

# ── CONFIG ──────────────────────────────────────────────────────────────────
API_TOKEN    = os.environ.get("SMARTSHEET_API_TOKEN", "")
WORKSPACE_ID = os.environ.get("SMARTSHEET_WORKSPACE_ID", None)  # None = ALL workspaces
OUTPUT_FILE  = "smartsheet.xlsx"
# ────────────────────────────────────────────────────────────────────────────

# Fail fast if token is missing
if not API_TOKEN:
    sys.exit("ERROR: SMARTSHEET_API_TOKEN is not set. Add it to your .env file.")

client = smartsheet.Smartsheet(API_TOKEN)
client.errors_as_exceptions(True)

# Validate token immediately
try:
    client.Users.get_current_user()
except smartsheet.exceptions.ApiError as e:
    sys.exit(f"ERROR: Smartsheet authentication failed: {e.message}")


def sanitize_sheet_name(name: str, fallback: str = "Sheet") -> str:
    """Excel tab names: max 31 chars, no special chars. Fallback if result is empty."""
    name = re.sub(r"[\\/*?\[\]:]", "", name).strip()
    return name[:31] if name else fallback[:31]


def get_all_sheet_ids() -> list[tuple[str, int]]:
    """Return [(name, sheet_id)] from target workspace(s) or all workspaces."""
    results = []

    if WORKSPACE_ID:
        workspaces = [client.Workspaces.get_workspace(int(WORKSPACE_ID))]
    else:
        workspaces = client.Workspaces.list_workspaces(include_all=True).data

    for ws in workspaces:
        ws_detail = client.Workspaces.get_workspace(ws.id)
        if ws_detail.sheets:
            for s in ws_detail.sheets:
                results.append((s.name, s.id))

    # Also grab home-level sheets not in any workspace
    if not WORKSPACE_ID:
        home_sheets = client.Sheets.list_sheets(include_all=True).data
        ws_sheet_ids = {sid for _, sid in results}
        for s in home_sheets:
            if s.id not in ws_sheet_ids:
                results.append((s.name, s.id))

    return results


def sheet_to_dataframe(sheet) -> pd.DataFrame:
    """
    Convert a Smartsheet sheet object to a pandas DataFrame.
    Maps cells by column_id (not position) to handle sparse/out-of-order rows.
    """
    col_id_to_title = {col.id: col.title for col in sheet.columns}
    col_titles = [col.title for col in sheet.columns]

    rows = []
    for row in sheet.rows:
        row_data = {title: None for title in col_titles}  # pre-fill all columns
        for cell in row.cells:
            title = col_id_to_title.get(cell.column_id)
            if title:
                row_data[title] = cell.display_value if cell.display_value is not None else cell.value
        rows.append(row_data)

    return pd.DataFrame(rows, columns=col_titles)


def style_header_row(ws):
    """Apply professional header styling to row 1."""
    header_fill  = PatternFill("solid", start_color="1F3864")
    header_font  = Font(bold=True, color="FFFFFF", name="Arial", size=10)
    center_align = Alignment(horizontal="center", vertical="center")

    for cell in ws[1]:
        cell.fill      = header_fill
        cell.font      = header_font
        cell.alignment = center_align

    ws.row_dimensions[1].height = 20


def auto_fit_columns(ws):
    for col in ws.columns:
        max_len = max((len(str(c.value)) if c.value else 0) for c in col)
        ws.column_dimensions[get_column_letter(col[0].column)].width = min(max_len + 4, 60)


def build_index_sheet(wb, sheet_manifest: list[tuple[str, str, int, int]]):
    """First tab: clickable index of all exported sheets."""
    idx = wb.create_sheet("INDEX", 0)
    idx.append(["#", "Sheet Name", "Rows", "Smartsheet ID"])
    style_header_row(idx)

    for i, (tab_name, orig_name, row_count, sheet_id) in enumerate(sheet_manifest, start=1):
        idx.append([i, orig_name, row_count, sheet_id])
        cell = idx.cell(row=i + 1, column=2)
        safe = f"'{tab_name}'" if " " in tab_name else tab_name
        cell.hyperlink = f"#{safe}!A1"
        cell.font = Font(color="0070C0", underline="single", name="Arial", size=10)

    auto_fit_columns(idx)


def archive_existing():
    """Rename smartsheet.xlsx → smartsheet_TIMESTAMP.xlsx using copy+delete for cross-fs safety."""
    if not os.path.exists(OUTPUT_FILE):
        return
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive = f"smartsheet_{ts}.xlsx"
    shutil.copy2(OUTPUT_FILE, archive)
    os.remove(OUTPUT_FILE)
    print(f"Archived existing file → {archive}")


def main():
    archive_existing()

    print("Fetching sheet list...")
    sheet_list = get_all_sheet_ids()
    print(f"Found {len(sheet_list)} sheets.")

    writer = pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl")
    pd.DataFrame().to_excel(writer, sheet_name="INDEX", index=False)

    manifest = []
    seen_names = {}
    errors = []

    for orig_name, sheet_id in sheet_list:
        print(f"  Pulling: {orig_name}")
        try:
            sheet = client.Sheets.get_sheet(sheet_id)
            df = sheet_to_dataframe(sheet)

            tab_name = sanitize_sheet_name(orig_name, fallback=f"Sheet_{sheet_id}")
            if tab_name in seen_names:
                seen_names[tab_name] += 1
                suffix = f"_{seen_names[tab_name]}"
                tab_name = sanitize_sheet_name(orig_name, fallback=f"Sheet_{sheet_id}")[:31 - len(suffix)] + suffix
            else:
                seen_names[tab_name] = 0

            df.to_excel(writer, sheet_name=tab_name, index=False)
            manifest.append((tab_name, orig_name, len(df), sheet_id))

        except smartsheet.exceptions.ApiError as e:
            msg = f"  SKIPPED '{orig_name}': API error {e.message}"
            print(msg)
            errors.append(msg)
        except Exception as e:
            msg = f"  SKIPPED '{orig_name}': {type(e).__name__}: {e}"
            print(msg)
            errors.append(msg)

    writer.close()

    print("Applying formatting...")
    wb = load_workbook(OUTPUT_FILE)

    for tab_name, _, _, _ in manifest:
        if tab_name in wb.sheetnames:
            ws = wb[tab_name]
            style_header_row(ws)
            auto_fit_columns(ws)
            ws.freeze_panes = "A2"

    if "INDEX" in wb.sheetnames:
        del wb["INDEX"]
    build_index_sheet(wb, manifest)

    wb.save(OUTPUT_FILE)

    print(f"\nDone. Exported {len(manifest)} sheets → {OUTPUT_FILE}")
    if errors:
        print(f"\n{len(errors)} sheet(s) skipped:")
        for e in errors:
            print(e)


if __name__ == "__main__":
    main()
