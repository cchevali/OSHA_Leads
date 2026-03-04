@echo off
cd /d "%~dp0"
if not exist out mkdir out
set RUN_TMP=out\wally_trial_last_run.log
echo [%date% %time%] Wally trial run start >> out\wally_trial_task.log
echo [%date% %time%] === RUN HEADER === >> out\wally_trial_task.log
echo [%date% %time%] batch=%~f0 >> out\wally_trial_task.log
echo [%date% %time%] cwd=%cd% >> out\wally_trial_task.log
for /f "delims=" %%p in ('where python 2^>nul') do echo [%date% %time%] python=%%p >> out\wally_trial_task.log
if errorlevel 1 echo [%date% %time%] python=NOT_FOUND >> out\wally_trial_task.log
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\run_with_secrets.ps1" python deliver_daily.py --db "data/osha.sqlite" --customer "%~dp0customers\wally_trial_tx_triangle_v1.json" --mode daily --since-days 14 --admin-email "support@microflowops.com" --send-live > "%RUN_TMP%" 2>&1
set RUN_EXIT=%ERRORLEVEL%
type "%RUN_TMP%" >> out\wally_trial_task.log
findstr /C:"CONFIG_ERROR" "%RUN_TMP%" >nul
if %ERRORLEVEL%==0 echo [%date% %time%] CONFIG_ERROR detected >> out\wally_trial_task.log
if %RUN_EXIT% EQU 0 (
  set TRIAL_TS_UTC=
  set TRIAL_RUN_ID=
  for /f "delims=" %%t in ('py -3 -c "from datetime import datetime,timezone;print(datetime.now(timezone.utc).strftime(\"%%Y-%%m-%%dT%%H:%%M:%%SZ\"))"') do set TRIAL_TS_UTC=%%t
  for /f "delims=" %%r in ('py -3 -c "from datetime import datetime,timezone;print(\"scheduler_wally_trial_\" + datetime.now(timezone.utc).strftime(\"%%Y%%m%%dT%%H%%M%%SZ\"))"') do set TRIAL_RUN_ID=%%r
  if "%TRIAL_TS_UTC%"=="" (
    echo [%date% %time%] WARN_TRIAL_TS_CAPTURE_FAILED subscriber_key=wally_trial >> out\wally_trial_task.log
  ) else (
    py -3 run_trial_admin.py append-event --subscriber-key wally_trial --status SENT --variant DAILY --ts-utc "%TRIAL_TS_UTC%" --run-id "%TRIAL_RUN_ID%" >> out\wally_trial_task.log 2>&1
    if errorlevel 1 echo [%date% %time%] WARN_TRIAL_LEDGER_APPEND_FAILED subscriber_key=wally_trial run_id=%TRIAL_RUN_ID% >> out\wally_trial_task.log
  )
)
if %RUN_EXIT% NEQ 0 echo [%date% %time%] ERROR: Wally trial run failed >> out\wally_trial_task.log
if %RUN_EXIT% EQU 0 echo [%date% %time%] SUCCESS: Wally trial run completed >> out\wally_trial_task.log
exit /b %RUN_EXIT%
