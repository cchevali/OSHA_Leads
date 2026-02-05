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
python deliver_daily.py --db "data/osha.sqlite" --customer "%~dp0customers\wally_trial_tx_triangle_v1.json" --mode daily --since-days 14 --admin-email "support@microflowops.com" --send-live > "%RUN_TMP%" 2>&1
set RUN_EXIT=%ERRORLEVEL%
type "%RUN_TMP%" >> out\wally_trial_task.log
findstr /C:"CONFIG_ERROR" "%RUN_TMP%" >nul
if %ERRORLEVEL%==0 echo [%date% %time%] CONFIG_ERROR detected >> out\wally_trial_task.log
if %RUN_EXIT% NEQ 0 echo [%date% %time%] ERROR: Wally trial run failed >> out\wally_trial_task.log
if %RUN_EXIT% EQU 0 echo [%date% %time%] SUCCESS: Wally trial run completed >> out\wally_trial_task.log
exit /b %RUN_EXIT%
