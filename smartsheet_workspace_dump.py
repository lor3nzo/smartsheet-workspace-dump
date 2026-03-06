"""
Smartsheet Full Workspace Dump → Excel
Requirements: pip install smartsheet-python-sdk openpyxl pandas
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

# ── CONFIG ──────────────────────────────────────────────────────────────────
API_TOKEN    = os.environ.get("SMARTSHEET_API_TOKEN", "YOUR_API_TOKEN_HERE")
WORKSPACE_ID = os.environ.get("SMARTSHEET_WORKSPACE_ID", None)  # None = ALL workspaces
OUTPUT_FILE  = "smartsheet.xlsx"
# ────────────────────────────────────────────────────────────────────────────

client = smartsheet.Smartsheet(API_TOKEN)
client.errors_as_exceptions(True)


def sanitize_sheet_name(name: str) -> str:
    """Excel tab names: max 31 chars, no special chars."""
    name = re.sub(r"[\\/*?\[\]:]", "", name)
    return name[:31]


def get_all_sheet_ids() -> list[tuple[str, int]]:
    """Return [(name, sheet_id)] from target workspace(s) or all workspaces."""
    results = []

    if WORKSPACE_ID:
        ws = client.Workspaces.get_workspace(WORKSPACE_ID)
        workspaces = [ws]
    else:
        workspaces = client.Workspaces.list_workspaces(include_all=True).data

    for ws in workspaces:
        ws_detail = client.Workspaces.get_workspace(ws.id)
        if ws_detail.sheets:
            for s in ws_detail.sheets:
                results.append((s.name, s.id))

    # Also grab sheets not in any workspace (Home-level sheets)
    if not WORKSPACE_ID:
        home_sheets = client.Sheets.list_sheets(include_all=True).data
        ws_sheet_ids = {sid for _, sid in results}
        for s in home_sheets:
            if s.id not in ws_sheet_ids:
                results.append((s.name, s.id))

    return results


def sheet_to_dataframe(sheet) -> pd.DataFrame:
    """Convert a Smartsheet sheet object to a pandas DataFrame."""
    col_titles = [col.title for col in sheet.columns]
    rows = []
    for row in sheet.rows:
        row_data = {}
        for i, cell in enumerate(row.cells):
            col_name = col_titles[i] if i < len(col_titles) else f"Col_{i}"
            # Use display_value when available for formatted output
            row_data[col_name] = cell.display_value if cell.display_value is not None else cell.value
        rows.append(row_data)
    return pd.DataFrame(rows, columns=col_titles)


def style_header_row(ws):
    """Apply professional header styling to row 1."""
    header_fill  = PatternFill("solid", start_color="1F3864")  # dark navy
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


def build_index_sheet(wb, sheet_manifest: list[tuple[str, str, int]]):
    """First tab: clickable index of all exported sheets."""
    idx = wb.create_sheet("INDEX", 0)
    idx.append(["#", "Sheet Name", "Rows", "Smartsheet ID"])
    style_header_row(idx)

    for i, (tab_name, orig_name, row_count) in enumerate(sheet_manifest, start=1):
        idx.append([i, orig_name, row_count, ""])
        # Hyperlink to the tab
        cell = idx.cell(row=i + 1, column=2)
        cell.hyperlink = f"#{tab_name}!A1"
        cell.font = Font(color="0070C0", underline="single", name="Arial", size=10)

    auto_fit_columns(idx)


def main():
    if os.path.exists(OUTPUT_FILE):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive = f"smartsheet_{ts}.xlsx"
        os.rename(OUTPUT_FILE, archive)
        print(f"Archived existing file → {archive}")

    print("Fetching sheet list...")
    sheet_list = get_all_sheet_ids()
    print(f"Found {len(sheet_list)} sheets.")

    writer = pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl")
    # Write a dummy sheet so ExcelWriter initialises the workbook
    pd.DataFrame().to_excel(writer, sheet_name="INDEX", index=False)

    manifest = []
    seen_names = {}

    for orig_name, sheet_id in sheet_list:
        print(f"  Pulling: {orig_name}")
        try:
            sheet = client.Sheets.get_sheet(sheet_id)
            df = sheet_to_dataframe(sheet)

            tab_name = sanitize_sheet_name(orig_name)
            # Deduplicate tab names
            if tab_name in seen_names:
                seen_names[tab_name] += 1
                tab_name = sanitize_sheet_name(f"{tab_name}_{seen_names[tab_name]}")
            else:
                seen_names[tab_name] = 0

            df.to_excel(writer, sheet_name=tab_name, index=False)
            manifest.append((tab_name, orig_name, len(df)))

        except Exception as e:
            print(f"    WARNING: Could not export '{orig_name}': {e}")

    writer.close()

    # Post-process: style all sheets
    print("Applying formatting...")
    wb = load_workbook(OUTPUT_FILE)

    for tab_name, _, _ in manifest:
        if tab_name in wb.sheetnames:
            ws = wb[tab_name]
            style_header_row(ws)
            auto_fit_columns(ws)
            ws.freeze_panes = "A2"

    # Build index (replaces blank INDEX sheet)
    if "INDEX" in wb.sheetnames:
        del wb["INDEX"]
    build_index_sheet(wb, manifest)

    wb.save(OUTPUT_FILE)
    print(f"\nDone. Exported {len(manifest)} sheets → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
