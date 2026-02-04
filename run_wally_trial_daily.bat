@echo off
cd /d "C:\dev\OSHA_Leads"
if not exist out mkdir out
set RUN_TMP=out\wally_trial_last_run.log
echo [%date% %time%] Wally trial run start >> out\wally_trial_task.log
python deliver_daily.py --db "data/osha.sqlite" --customer "C:\dev\OSHA_Leads\customers\wally_trial_tx_triangle_v1.json" --mode daily --since-days 14 --admin-email "support@microflowops.com" > "%RUN_TMP%" 2>&1
set RUN_EXIT=%ERRORLEVEL%
type "%RUN_TMP%" >> out\wally_trial_task.log
findstr /C:"CONFIG_ERROR" "%RUN_TMP%" >nul
if %ERRORLEVEL%==0 echo [%date% %time%] CONFIG_ERROR detected >> out\wally_trial_task.log
if %RUN_EXIT% NEQ 0 echo [%date% %time%] ERROR: Wally trial run failed >> out\wally_trial_task.log
if %RUN_EXIT% EQU 0 echo [%date% %time%] SUCCESS: Wally trial run completed >> out\wally_trial_task.log
exit /b %RUN_EXIT%
