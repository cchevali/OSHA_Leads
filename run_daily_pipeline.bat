@echo off
REM OSHA Daily Pipeline - Run ingestion, export, and update freshness
REM Schedule this via Windows Task Scheduler to run daily at 6am

cd /d "C:\dev\OSHA_Leads"

echo [%date% %time%] Starting OSHA daily pipeline >> out\pipeline.log

REM Run ingestion
python ingest_osha.py --db osha_leads.db --states TX --since-days 30 >> out\pipeline.log 2>&1
if errorlevel 1 (
    echo [%date% %time%] ERROR: Ingestion failed >> out\pipeline.log
    exit /b 1
)

REM Export to daily CSV
python export_daily.py --db osha_leads.db --outdir out >> out\pipeline.log 2>&1

REM Get today's date for the filename
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /format:list') do set datetime=%%I
set TODAY=%datetime:~0,4%-%datetime:~4,2%-%datetime:~6,2%

REM Update latest_leads.csv
copy /Y "out\daily_leads_%TODAY%.csv" "out\latest_leads.csv" >> out\pipeline.log 2>&1

REM Update freshness metadata
python write_latest_run.py >> out\pipeline.log 2>&1

echo [%date% %time%] Pipeline complete >> out\pipeline.log

