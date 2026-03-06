# Smartsheet Workspace Dump

Exports all Smartsheet workspaces and sheets to a formatted Excel workbook, with optional CSV and Parquet output. Each sheet becomes its own tab. Supports incremental export, parallel fetching, post-write validation, and run-specific output directories.

---

## Features

- Dumps all workspaces (or a single workspace) in one run
- One Excel tab per Smartsheet sheet, with a hyperlinked INDEX tab
- Parallel fetching with configurable worker count
- Incremental export: re-fetch only sheets modified since the last run
- Output formats: XLSX, CSV, Parquet, or any combination
- Run-specific subdirectories for CSV/Parquet with a stable `_latest/` pointer
- Post-write validation at three depth levels (basic / standard / deep)
- Automatic archive of the previous XLSX on each run
- Professional formatting: frozen headers, auto-sized columns, styled header row
- Configurable: regex filters, row metadata, formatting mode, validation depth
- API token stored securely in `.env` -- never committed to GitHub

---

## Requirements

- Python 3.8+
- A Smartsheet account with API access

---

## Installation

```bash
pip install smartsheet-python-sdk openpyxl pandas python-dotenv
```

For Parquet output, also install:

```bash
pip install pyarrow
```

---

## Configuration

1. Copy `.env.example` to `.env`:
   ```bash
   copy .env.example .env
   ```

2. Open `.env` and paste your Smartsheet API token:
   ```
   SMARTSHEET_API_TOKEN=your_token_here
   ```

3. To get your token: Smartsheet > Avatar (top-right) > Apps & Integrations > API Access > Generate new access token

---

## Usage

**Option A -- Double-click (Windows):**
Run `run_smartsheet_dump.bat`

**Option B -- Command line:**
```bash
python smartsheet_workspace_dump.py [options]
```

Default output: `smartsheet.xlsx` in the same folder. Previous exports are automatically archived to `archive/smartsheet_YYYYMMDD_HHMMSS.xlsx`.

---

## CLI Reference

### Core

| Flag | Default | Description |
|---|---|---|
| `--workspace-id ID` | all | Export a single workspace by ID. Omit to export all workspaces. |
| `--output FILE` | `smartsheet.xlsx` | Output `.xlsx` filename. |
| `--values` | `both` | Cell value mode: `raw`, `display`, or `both` (adds a second column per cell). |
| `--dry-run` | off | Discover and list sheets without writing any file. Shows FETCH vs CACHE in incremental mode. |
| `--workers N` | `5` | Parallel fetch workers. |
| `--log-level` | `INFO` | `DEBUG`, `INFO`, `WARNING`, or `ERROR`. |
| `--log-every N` | `10` | Log a progress line every N sheets fetched. |

### Filtering

| Flag | Default | Description |
|---|---|---|
| `--include-regex PATTERN` | off | Only export sheets whose name matches this regex (case-insensitive). |
| `--exclude-regex PATTERN` | off | Skip sheets whose name matches this regex (case-insensitive). |
| `--max-sheets N` | off | Cap total sheets exported. Useful for testing. |

### Output Format

| Flag | Default | Description |
|---|---|---|
| `--output-format` | `xlsx` | `xlsx`, `csv`, `parquet`, `both` (xlsx+csv), or `all` (xlsx+csv+parquet). |
| `--row-metadata` | off | Append `_Row_ID`, `_Parent_Row_ID`, `_Row_Number`, `_Created_At`, `_Modified_At` columns to each sheet. |

CSV and Parquet runs write into timestamped subdirectories and maintain a stable `_latest/` pointer:

```
smartsheet_csv/
    2026-03-06_170001/
        Sheet1.csv
        _manifest.csv
    _latest/              <- always points to the most recent run
```

### XLSX Formatting

| Flag | Default | Description |
|---|---|---|
| `--format` | `pretty` | `pretty` (styled headers, autofit columns) or `minimal` (freeze panes only, faster). |
| `--autofit-rows N` | `50` | Rows sampled for column autofit. `0` disables autofit. Ignored under `--format minimal`. |
| `--autofit-max-width N` | `60` | Maximum column width in autofit. Ignored under `--format minimal`. |
| `--no-index` | off | Omit the INDEX tab. |
| `--no-summary` | off | Omit the RUN_SUMMARY and SKIPPED tabs. |

### Validation

| Flag | Default | Description |
|---|---|---|
| `--validation-level` | `standard` | `basic` (row counts + headers), `standard` (+ sampled cell hashes), `deep` (all rows hashed, slow). |
| `--max-validation-issues N` | `10` | Max issues logged per sheet in deep mode. `0` = unlimited. |

### Incremental Export

| Flag | Default | Description |
|---|---|---|
| `--since DATE or last-run` | off | Only re-fetch sheets modified after this date. Use `last-run` to read cutoff from the state sidecar automatically. Requires an XLSX-producing output format. |
| `--state-file PATH` | `{output}.state.json` | Path to the incremental state sidecar JSON. |

**How incremental export works:**

1. On the first run, all sheets are fetched and a state sidecar is written alongside the output file.
2. On subsequent runs with `--since last-run`, each sheet's `modified_at` from the Smartsheet API is compared against the last run timestamp.
3. Unmodified sheets are read from the prior XLSX using the exact tab name stored in the sidecar. Modified sheets are re-fetched from the API.
4. The workbook is rebuilt from both sources and written fresh.
5. If `modified_at` is unavailable for a sheet, it is always re-fetched (safe default).
6. If the sidecar or prior XLSX is missing, a full export runs automatically.

**State sidecar format:**
```json
{
  "C:\\path\\to\\smartsheet.xlsx": {
    "last_run": "2026-03-06T17:19:20",
    "sheet_tabs": {
      "12345678": "355 Court St",
      "87654321": "451 West Broadway - V0"
    }
  }
}
```

---

## Example Commands

```bash
# Full export, all defaults
python smartsheet_workspace_dump.py

# Incremental: re-fetch only sheets changed since last run
python smartsheet_workspace_dump.py --since last-run

# Incremental: re-fetch sheets changed since a specific date
python smartsheet_workspace_dump.py --since 2026-03-01

# Fastest possible run (no styling, no tabs, basic validation only)
python smartsheet_workspace_dump.py --format minimal --no-index --no-summary --validation-level basic

# Full audit run
python smartsheet_workspace_dump.py --validation-level deep --max-validation-issues 0

# Export to CSV only (no XLSX)
python smartsheet_workspace_dump.py --output-format csv

# Export to all formats
python smartsheet_workspace_dump.py --output-format all

# Filter to a subset of sheets
python smartsheet_workspace_dump.py --include-regex "court st|broadway" --dry-run

# Wide columns, more autofit sampling
python smartsheet_workspace_dump.py --autofit-max-width 100 --autofit-rows 200

# Single workspace, custom output file
python smartsheet_workspace_dump.py --workspace-id 1234567890 --output my_workspace.xlsx
```

---

## File Structure

```
smartsheet-workspace-dump/
├── smartsheet_workspace_dump.py    # Main script
├── run_smartsheet_dump.bat         # Windows one-click runner
├── .env.example                    # Token template (safe to commit)
├── .env                            # Your actual token (never committed)
├── .gitignore                      # Excludes .env, output files, archive/
├── README.md
│
├── smartsheet.xlsx                 # Latest XLSX export (gitignored)
├── smartsheet.xlsx.state.json      # Incremental state sidecar (gitignored)
├── archive/                        # Prior XLSX exports with timestamps (gitignored)
├── smartsheet_csv/                 # CSV output runs (gitignored)
│   ├── 2026-03-06_170001/
│   └── _latest/
└── smartsheet_parquet/             # Parquet output runs (gitignored)
    ├── 2026-03-06_170001/
    └── _latest/
```

---

## Security

- `.env` is listed in `.gitignore` and will never be pushed to GitHub
- Use `.env.example` as a template when sharing or cloning this repo
- The API token is read at runtime only and never logged

---

## License

GPL v3 -- see `LICENSE` file.

---

*Built with the [Smartsheet Python SDK](https://github.com/smartsheet/smartsheet-python-sdk)*
