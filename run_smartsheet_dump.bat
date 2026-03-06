@echo off
cd /d "%~dp0"
echo Running Smartsheet Workspace Dump...
python smartsheet_workspace_dump.py
echo.
echo Done! Check this folder for the Excel file.
pause
