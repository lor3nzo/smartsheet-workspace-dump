"""
Smartsheet Full Workspace Dump → Excel

Requirements:
    pip install smartsheet-python-sdk openpyxl pandas python-dotenv

Usage:
    python smartsheet_workspace_dump.py
    python smartsheet_workspace_dump.py --workspace-id 123 --output dump.xlsx
    python smartsheet_workspace_dump.py --values both --log-level DEBUG --dry-run
"""

from dotenv import load_dotenv
load_dotenv()

import argparse
import logging
import math
import os
import re
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional

import pandas as pd
import smartsheet
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

# ── CONSTANTS ────────────────────────────────────────────────────────────────
DEFAULT_OUTPUT   = "smartsheet.xlsx"
ARCHIVE_DIR      = "archive"
LOG_FILE         = "smartsheet_dump.log"
MAX_RETRIES      = 3
RETRY_BASE_DELAY = 2.0
MAX_WORKERS      = 5
INDENT_COL       = "_Indent_Level"
INDEX_TAB        = "INDEX"
# ────────────────────────────────────────────────────────────────────────────


def _normalize(v) -> str:
    """Normalize cell values for write validation: NaN/None → '', whole floats → int, dates → ISO."""
    if v is None:
        return ""
    if isinstance(v, float):
        if math.isnan(v):
            return ""
        if v == int(v):
            return str(int(v))
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, date):
        return v.strftime("%Y-%m-%d")
    return str(v).strip()


# ── CLI ──────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Dump Smartsheet workspaces to Excel")
    p.add_argument("--workspace-id", default=os.environ.get("SMARTSHEET_WORKSPACE_ID"),
                   help="Single workspace ID (default: all workspaces)")
    p.add_argument("--output",    default=DEFAULT_OUTPUT,
                   help="Output .xlsx filename")
    p.add_argument("--values",    choices=["raw", "display", "both"], default="both",
                   help="Cell value export mode (default: both)")
    p.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")
    p.add_argument("--dry-run",   action="store_true",
                   help="Discover and list sheets without writing any file")
    p.add_argument("--log-every", type=int, default=10,
                   help="Log progress every N sheets (default: 10)")
    p.add_argument("--workers",   type=int, default=MAX_WORKERS,
                   help=f"Parallel fetch workers (default: {MAX_WORKERS})")
    return p.parse_args()


# ── LOGGING ──────────────────────────────────────────────────────────────────
def setup_logging(level: str) -> logging.Logger:
    logger = logging.getLogger("ss_dump")
    if logger.handlers:
        return logger  # already configured — prevent duplicate handlers
    logger.setLevel(getattr(logging, level, logging.INFO))
    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%Y-%m-%d %H:%M:%S")
    for h in [logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_FILE, encoding="utf-8")]:
        h.setFormatter(fmt)
        logger.addHandler(h)
    return logger


# ── DATA CLASSES ─────────────────────────────────────────────────────────────
@dataclass
class SheetRecord:
    orig_name:      str
    sheet_id:       int
    workspace_name: str = ""

@dataclass
class FetchedSheet:
    record:   SheetRecord
    df:       pd.DataFrame
    tab_name: str

@dataclass
class ExportStats:
    total_found:    int   = 0
    exported:       int   = 0
    skipped_type:   int   = 0
    skipped_error:  int   = 0
    row_mismatches: list  = field(default_factory=list)
    errors:         list  = field(default_factory=list)
    elapsed:        float = 0.0


# ── RETRY ────────────────────────────────────────────────────────────────────
def with_retry(fn, *args, logger, **kwargs):
    """Call fn(*args, **kwargs) with exponential backoff on transient errors."""
    for attempt in range(1, MAX_RETRIES + 2):
        try:
            return fn(*args, **kwargs)
        except smartsheet.exceptions.ApiError as e:
            code = getattr(getattr(e, "error", None), "status_code", None)
            if attempt <= MAX_RETRIES and code in (429, 503):
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                logger.warning(f"API {code} (attempt {attempt}/{MAX_RETRIES}), retry in {delay:.1f}s")
                time.sleep(delay)
            else:
                raise
        except (ConnectionError, TimeoutError, OSError,
                smartsheet.exceptions.SystemMaintenanceError) as e:
            if attempt <= MAX_RETRIES:
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                logger.warning(f"Network error (attempt {attempt}/{MAX_RETRIES}), retry in {delay:.1f}s: {type(e).__name__}: {e}")
                time.sleep(delay)
            else:
                raise


# ── NAMING ───────────────────────────────────────────────────────────────────
def sanitize_sheet_name(name: str, fallback: str = "Sheet") -> str:
    name = re.sub(r"[\\/*?\[\]:]", "", name).strip()
    return name[:31] if name else fallback[:31]


def unique_tab_name(raw_name: str, sheet_id: int, seen: dict) -> str:
    """Case-insensitive unique tab name, suffix fits within Excel's 31-char limit."""
    base = sanitize_sheet_name(raw_name, fallback=f"Sheet_{sheet_id}")
    key  = base.lower()
    if key not in seen:
        seen[key] = 0
        return base
    seen[key] += 1
    suffix   = f"_{seen[key]}"
    tab_name = base[:31 - len(suffix)] + suffix
    seen[tab_name.lower()] = 0
    return tab_name


def _safe_reserved_name(base: str, seen: dict) -> str:
    """Return a tab name that doesn't collide with real sheets."""
    name = base
    while name.lower() in seen:
        name += "_"
    return name

def safe_index_name(seen: dict) -> str:
    return _safe_reserved_name(INDEX_TAB, seen)

def safe_summary_name(seen: dict) -> str:
    return _safe_reserved_name("RUN_SUMMARY", seen)

def safe_skipped_name(seen: dict) -> str:
    return _safe_reserved_name("SKIPPED", seen)


# ── DISCOVERY ────────────────────────────────────────────────────────────────
def _collect_from_folder(folder, workspace_name: str) -> list:
    records = []
    if getattr(folder, "sheets", None):
        for s in folder.sheets:
            records.append(SheetRecord(s.name, s.id, workspace_name))
    if getattr(folder, "folders", None):
        for sub in folder.folders:
            records.extend(_collect_from_folder(sub, workspace_name))
    return records


def discover_sheets(client, workspace_id: Optional[str], logger) -> list:
    records = []

    if workspace_id:
        workspaces = [with_retry(
            client.Workspaces.get_workspace, int(workspace_id),
            logger=logger, include="sheets,folders"
        )]
    else:
        ws_list    = with_retry(client.Workspaces.list_workspaces, logger=logger, include_all=True)
        workspaces = [
            with_retry(client.Workspaces.get_workspace, ws.id,
                       logger=logger, include="sheets,folders")
            for ws in ws_list.data
        ]

    for ws in workspaces:
        if getattr(ws, "sheets", None):
            for s in ws.sheets:
                records.append(SheetRecord(s.name, s.id, ws.name))
        if getattr(ws, "folders", None):
            for folder in ws.folders:
                records.extend(_collect_from_folder(folder, ws.name))

    if not workspace_id:
        ws_ids = {r.sheet_id for r in records}
        home   = with_retry(client.Sheets.list_sheets, logger=logger, include_all=True)
        for s in home.data:
            if s.id not in ws_ids:
                records.append(SheetRecord(s.name, s.id, "(Home)"))

    logger.info(f"Discovered {len(records)} sheet(s) across {len(workspaces)} workspace(s)")

    # Final dedup pass — guard against duplicates across recursive paths
    seen_ids, deduped = set(), []
    for r in records:
        if r.sheet_id not in seen_ids:
            seen_ids.add(r.sheet_id)
            deduped.append(r)
    if len(deduped) < len(records):
        logger.warning(f"Removed {len(records) - len(deduped)} duplicate sheet(s) after discovery")
    return deduped


# ── EXTRACTION ───────────────────────────────────────────────────────────────
def _resolve_col_titles(columns) -> dict:
    """Map col.id → unique title; append _2, _3 for duplicate column names."""
    seen, result = {}, {}
    for col in columns:
        title = col.title or f"Col_{col.id}"
        if title in seen:
            seen[title] += 1
            title = f"{title}_{seen[title]}"
        else:
            seen[title] = 1
        result[col.id] = title
    return result


def extract_sheet(client, record: SheetRecord, value_mode: str, logger) -> Optional[pd.DataFrame]:
    """
    Fetch one sheet and return a DataFrame.
    Returns None if the object is not a plain sheet (report, dashboard, etc).
    Includes _Indent_Level column to preserve row hierarchy.
    """
    raw      = with_retry(client.Sheets.get_sheet, record.sheet_id, logger=logger)
    obj_type = getattr(raw, "type", None)
    if obj_type and str(obj_type).upper() not in ("SHEET", ""):
        logger.warning(f"Skipping '{record.orig_name}': unsupported object type '{obj_type}'")
        return None

    col_map    = _resolve_col_titles(raw.columns)
    col_titles = list(col_map.values())

    if value_mode == "both":
        data_cols = [c for t in col_titles for c in (t, f"{t}_raw")]
    else:
        data_cols = col_titles

    all_cols = [INDENT_COL] + data_cols

    rows = []
    for row in raw.rows:
        row_data               = dict.fromkeys(all_cols)
        row_data[INDENT_COL]   = getattr(row, "indent", 0) or 0
        for cell in row.cells:
            title = col_map.get(cell.column_id)
            if not title:
                continue
            display = cell.display_value if cell.display_value is not None else cell.value
            if value_mode == "raw":
                row_data[title] = cell.value
            elif value_mode == "display":
                row_data[title] = display
            else:
                row_data[title]          = display
                row_data[f"{title}_raw"] = cell.value
        rows.append(row_data)

    return pd.DataFrame(rows, columns=all_cols)


# ── RENDERING ────────────────────────────────────────────────────────────────
def style_header_row(ws):
    fill  = PatternFill("solid", start_color="1F3864")
    font  = Font(bold=True, color="FFFFFF", name="Arial", size=10)
    align = Alignment(horizontal="center", vertical="center")
    for cell in ws[1]:
        cell.fill, cell.font, cell.alignment = fill, font, align
    ws.row_dimensions[1].height = 20


def auto_fit_columns(ws):
    for col in ws.columns:
        width = max((len(str(c.value)) if c.value else 0) for c in col)
        ws.column_dimensions[get_column_letter(col[0].column)].width = min(width + 4, 60)


def build_index_sheet(wb, manifest: list, index_tab: str):
    idx = wb.create_sheet(index_tab, 0)
    idx.append(["#", "Workspace", "Sheet Name", "Rows", "Smartsheet ID", "Link", "Generated"])
    style_header_row(idx)
    idx.cell(row=1, column=7).value = datetime.now().strftime("%Y-%m-%d %H:%M")
    idx.cell(row=1, column=7).font  = Font(italic=True, name="Arial", size=9, color="FFFFFF")

    for i, fs in enumerate(manifest, start=1):
        url = f"https://app.smartsheet.com/sheets/{fs.record.sheet_id}"
        idx.append([i, fs.record.workspace_name, fs.record.orig_name, len(fs.df), fs.record.sheet_id, "", ""])

        name_cell = idx.cell(row=i + 1, column=3)
        needs_quote = bool(re.search(r"[ '\[\]!]", fs.tab_name))
        escaped     = fs.tab_name.replace("'", "''")
        safe        = f"'{escaped}'" if needs_quote else fs.tab_name
        name_cell.hyperlink = f"#{safe}!A1"
        name_cell.font      = Font(color="0070C0", underline="single", name="Arial", size=10)

        link_cell           = idx.cell(row=i + 1, column=6)
        link_cell.value     = "Open"
        link_cell.hyperlink = url
        link_cell.font      = Font(color="0070C0", underline="single", name="Arial", size=10)

    auto_fit_columns(idx)


# ── SUMMARY / SKIPPED TABS ───────────────────────────────────────────────────
def build_summary_sheet(wb, stats: "ExportStats", args, summary_name: str, skipped_name: str):
    """RUN_SUMMARY tab: operational evidence inside the workbook."""
    ws = wb.create_sheet(summary_name)
    ws.column_dimensions["A"].width = 28
    ws.column_dimensions["B"].width = 60

    rows = [
        ("Run timestamp",    datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        ("Output file",      args.output),
        ("Value mode",       args.values),
        ("Workspace filter", args.workspace_id or "All"),
        ("Elapsed (s)",      f"{stats.elapsed:.1f}"),
        ("",                 ""),
        ("Sheets found",     stats.total_found),
        ("Sheets exported",  stats.exported),
        ("Skipped (type)",   stats.skipped_type),
        ("Skipped (error)",  stats.skipped_error),
        ("Row mismatches",   len(stats.row_mismatches)),
    ]
    for r in rows:
        ws.append(r)

    style_header = Font(bold=True, name="Arial", size=10)
    for row in ws.iter_rows(min_col=1, max_col=1):
        for cell in row:
            if cell.value:
                cell.font = style_header

    if stats.errors or stats.row_mismatches:
        skipped_ws = wb.create_sheet(skipped_name)
        skipped_ws.append(["Sheet Name", "Reason"])
        style_header_row(skipped_ws)
        for msg in stats.errors:
            skipped_ws.append(["", msg])
        for msg in stats.row_mismatches:
            skipped_ws.append(["", f"ROW MISMATCH: {msg}"])
        auto_fit_columns(skipped_ws)


# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    args   = parse_args()
    logger = setup_logging(args.log_level)
    log_n  = max(1, args.log_every)

    # Auth — client created inside main, no module-level side effects
    token = os.environ.get("SMARTSHEET_API_TOKEN", "")
    if not token:
        logger.error("SMARTSHEET_API_TOKEN is not set. Add it to your .env file.")
        sys.exit(1)

    client = smartsheet.Smartsheet(token)
    client.errors_as_exceptions(True)
    try:
        with_retry(client.Users.get_current_user, logger=logger)
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        sys.exit(1)

    # Discovery
    records = discover_sheets(client, args.workspace_id, logger)
    stats   = ExportStats(total_found=len(records))

    # Dry run — list sheets and exit
    if args.dry_run:
        logger.info("DRY RUN — no files will be written")
        for r in records:
            logger.info(f"  [{r.workspace_name}]  {r.orig_name}  (id={r.sheet_id})")
        logger.info(f"Total: {len(records)} sheet(s) found")
        return

    # Parallel extraction
    t_start  = time.time()
    fetched  = {}   # sheet_id → (record, df | None, error | None)

    logger.info(f"Fetching {len(records)} sheet(s) with up to {max(1, args.workers)} parallel workers...")

    def fetch_one(record):
        # Each thread gets its own client instance — avoids shared-state concurrency risk
        thread_client = smartsheet.Smartsheet(token)
        thread_client.errors_as_exceptions(True)
        return record, extract_sheet(thread_client, record, args.values, logger)

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {executor.submit(fetch_one, r): r for r in records}
        done    = 0
        for future in as_completed(futures):
            done  += 1
            record = futures[future]
            try:
                _, df = future.result()
                fetched[record.sheet_id] = (record, df, None)
                if done == 1 or done % log_n == 0 or done == len(records):
                    logger.info(f"  [{done}/{len(records)}] {record.orig_name}")
            except Exception as e:
                msg = f"SKIPPED '{record.orig_name}': {type(e).__name__}: {e}"
                logger.error(msg)
                stats.errors.append(msg)
                stats.skipped_error += 1
                fetched[record.sheet_id] = (record, None, msg)

    # Build manifest in original discovery order
    seen_tabs = {}
    manifest  = []
    for record in records:
        _, df, err = fetched.get(record.sheet_id, (record, None, "not fetched"))
        if err is not None:
            continue
        if df is None:
            stats.skipped_type += 1
            continue
        tab_name = unique_tab_name(record.orig_name, record.sheet_id, seen_tabs)
        manifest.append(FetchedSheet(record=record, df=df, tab_name=tab_name))

    index_tab = safe_index_name(seen_tabs)
    stem, ext = os.path.splitext(args.output)
    tmp_path  = f"{stem}_tmp{ext or '.xlsx'}"

    # Write to temp file (crash-safe: real file untouched until validated)
    with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
        pd.DataFrame().to_excel(writer, sheet_name=index_tab, index=False)
        for fs in manifest:
            fs.df.to_excel(writer, sheet_name=fs.tab_name, index=False)

    # Post-write validation: row counts + header sets
    logger.info("Validating output...")
    wb_check = load_workbook(tmp_path, read_only=True)
    for fs in manifest:
        if fs.tab_name not in wb_check.sheetnames:
            stats.row_mismatches.append(f"Missing tab '{fs.record.orig_name}'")
            continue
        ws_chk  = wb_check[fs.tab_name]
        written = ws_chk.max_row - 1
        if written != len(fs.df):
            msg = f"Row mismatch '{fs.record.orig_name}': expected {len(fs.df)}, got {written}"
            logger.warning(msg)
            stats.row_mismatches.append(msg)
        # Header check: compare written column names against DataFrame columns
        written_headers = [cell.value for cell in next(ws_chk.iter_rows(min_row=1, max_row=1))]
        expected_headers = list(fs.df.columns)
        if written_headers != expected_headers:
            msg = f"Header mismatch '{fs.record.orig_name}': expected {expected_headers}, got {written_headers}"
            logger.warning(msg)
            stats.row_mismatches.append(msg)
        # Sampled cell integrity: hash up to 5 data rows from source vs written
        if len(fs.df) > 0:
            sample_size = min(5, len(fs.df))
            for row_idx in range(sample_size):
                src_hash  = hash(tuple(_normalize(v) for v in fs.df.iloc[row_idx].values))
                xlsx_row  = next(ws_chk.iter_rows(
                    min_row=row_idx + 2, max_row=row_idx + 2, values_only=True
                ))
                xlsx_hash = hash(tuple(_normalize(v) for v in xlsx_row))
                if src_hash != xlsx_hash:
                    msg = f"Cell mismatch '{fs.record.orig_name}' row {row_idx + 1}: data differs after write"
                    logger.warning(msg)
                    stats.row_mismatches.append(msg)
                    break
    wb_check.close()

    # Apply formatting
    logger.info("Applying formatting...")
    wb = load_workbook(tmp_path)
    for fs in manifest:
        if fs.tab_name in wb.sheetnames:
            ws = wb[fs.tab_name]
            style_header_row(ws)
            auto_fit_columns(ws)
            ws.freeze_panes = "A2"
    if index_tab in wb.sheetnames:
        del wb[index_tab]
    build_index_sheet(wb, manifest, index_tab)
    stats.exported = len(manifest)
    stats.elapsed  = time.time() - t_start
    summary_name = safe_summary_name(seen_tabs)
    skipped_name = safe_skipped_name(seen_tabs)
    build_summary_sheet(wb, stats, args, summary_name, skipped_name)
    wb.save(tmp_path)

    # Archive prior output first (copy only), then atomically replace with new file via os.replace()
    if os.path.exists(args.output):
        os.makedirs(ARCHIVE_DIR, exist_ok=True)
        ts         = datetime.now().strftime("%Y%m%d_%H%M%S")
        stem2, _   = os.path.splitext(os.path.basename(args.output))
        old_backup = os.path.join(ARCHIVE_DIR, f"{stem2}_{ts}.xlsx")
        try:
            shutil.copy2(args.output, old_backup)
            logger.info(f"Archived previous output → {old_backup}")
        except Exception as e:
            logger.warning(f"Archive failed ({e}). Continuing — previous file will be overwritten.")

    try:
        os.replace(tmp_path, args.output)
    except Exception as e:
        logger.error(f"Failed to promote temp file: {e}")
        sys.exit(1)

    logger.info("─" * 50)
    logger.info(f"Output:          {args.output}")
    logger.info(f"Elapsed:         {stats.elapsed:.1f}s")
    logger.info(f"Found:           {stats.total_found}")
    logger.info(f"Exported:        {stats.exported}")
    logger.info(f"Skipped (type):  {stats.skipped_type}")
    logger.info(f"Skipped (error): {stats.skipped_error}")
    if stats.row_mismatches:
        logger.warning(f"Row mismatches:  {len(stats.row_mismatches)}")
        for m in stats.row_mismatches:
            logger.warning(f"  {m}")
    if stats.errors:
        logger.error("Errors:")
        for e in stats.errors:
            logger.error(f"  {e}")
    logger.info("─" * 50)


if __name__ == "__main__":
    main()
