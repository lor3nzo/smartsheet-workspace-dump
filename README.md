# Smartsheet Workspace Dump

Exports all Smartsheet workspaces and sheets to a single formatted Excel workbook. Each sheet becomes its own tab, with a hyperlinked INDEX. Automatically archives the previous export on each run.

---

## Features

- Dumps all workspaces (or a single workspace) in one click
- One Excel tab per Smartsheet sheet
- Tab 1 is a hyperlinked INDEX with row counts
- Auto-archives previous `smartsheet.xlsx` with a timestamp before each new run
- Professional formatting: frozen headers, auto-sized columns, navy header row
- Token stored securely in `.env` — never committed to GitHub

---

## Requirements

- Python 3.8+
- A Smartsheet account with API access

---

## Installation

```bash
pip install smartsheet-python-sdk openpyxl pandas python-dotenv
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

**Option A — Double-click:**
Run `run_smartsheet_dump.bat`

**Option B — Command line:**
```bash
python smartsheet_workspace_dump.py
```

Output: `smartsheet.xlsx` in the same folder.
Previous exports are automatically renamed to `smartsheet_YYYYMMDD_HHMMSS.xlsx`.

---

## File Structure

```
smartsheet-workspace-dump/
├── smartsheet_workspace_dump.py   # Main script
├── run_smartsheet_dump.bat        # Windows one-click runner
├── .env.example                   # Token template (safe to commit)
├── .env                           # Your actual token (never committed)
├── .gitignore                     # Excludes .env and Excel output files
└── README.md
```

---

## Security

- `.env` is listed in `.gitignore` and will never be pushed to GitHub
- Use `.env.example` as a template when sharing or cloning this repo

---

## License

MIT — free to use, modify, and distribute.

---

*Built with the [Smartsheet Python SDK](https://github.com/smartsheet/smartsheet-python-sdk)*
