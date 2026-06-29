@echo off
REM Daily NBOHR congestion forecast -> logs a new 7-day forecast to Snowflake
REM (SKYVEST.DBO.CM_CONGEST_FORECAST/_DRIVERS) for the /model dashboard tracker.
REM Registered as Windows Scheduled Task "NBOHR Congestion Forecast" (daily 06:00).
REM Output appended to forecast_log.txt; one Xweather forecast call per run.
cd /d "C:\Users\TylerMartin\ercot-basis-tracker\congestion_model"
echo ============================================================ >> forecast_log.txt
echo RUN %DATE% %TIME% >> forecast_log.txt
"C:\Users\TylerMartin\AppData\Local\Programs\Python\Python312\python.exe" forecast_demo.py --log >> forecast_log.txt 2>&1
echo EXIT forecast %ERRORLEVEL% >> forecast_log.txt
REM Refresh McCrae actual generation (HARALD_UNIT1, ~60-day SCED lag) for the GWA weighting.
"C:\Users\TylerMartin\AppData\Local\Programs\Python\Python312\python.exe" build_gen.py >> forecast_log.txt 2>&1
echo EXIT gen %ERRORLEVEL% >> forecast_log.txt
REM Multi-horizon budget (3mo/3y), ATC + GWA. Monthly-stable; idempotent per month.
"C:\Users\TylerMartin\AppData\Local\Programs\Python\Python312\python.exe" horizon_forecast.py --log >> forecast_log.txt 2>&1
echo EXIT budget %ERRORLEVEL% >> forecast_log.txt
