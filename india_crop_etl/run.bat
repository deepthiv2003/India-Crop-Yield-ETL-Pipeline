@echo off
echo 🌾 Installing India Crop ETL Project...
pip install -r requirements.txt

echo.
echo 🚀 Running COMPLETE ETL Pipeline...
python main.py

echo.
echo ✅ SUCCESS! Check:
echo   📊 Database: india_crops.db  
echo   💾 ML Data: ml_ready_dataset.csv
echo   📈 Logs: etl.log
echo.
echo 🔍 Run SQL: sqlite3 india_crops.db ^< queries.sql
pause
