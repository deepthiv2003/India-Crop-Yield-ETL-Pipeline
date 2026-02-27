-- 🔥 INDIA CROP YIELD ANALYTICS (Run: sqlite3 india_crops.db < queries.sql)

-- 1. NATIONAL SUMMARY
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT State) as states,
    COUNT(DISTINCT Crop) as crops,
    ROUND(AVG(Yield_kg_ha), 1) as avg_yield_kg_ha,
    ROUND(MIN(Yield_kg_ha), 1) as min_yield,
    ROUND(MAX(Yield_kg_ha), 1) as max_yield
FROM crop_yields;

-- 2. TOP 15 CROPS BY YIELD
SELECT Crop, ROUND(AVG(Yield_kg_ha), 1) as avg_yield,
       COUNT(*) as records, COUNT(DISTINCT State) as states
FROM crop_yields GROUP BY Crop 
ORDER BY avg_yield DESC LIMIT 15;

-- 3. STATE RANKING
SELECT State, ROUND(AVG(Yield_kg_ha), 1) as avg_yield,
       COUNT(DISTINCT Crop) as crops, SUM(Production) as total_tons
FROM crop_yields GROUP BY State 
ORDER BY avg_yield DESC LIMIT 20;

-- 4. YEARLY TRENDS
SELECT Crop_Year, ROUND(AVG(Yield_kg_ha), 1) as avg_yield,
       COUNT(*) as records
FROM crop_yields GROUP BY Crop_Year ORDER BY Crop_Year;

-- 5. ML TRAINING DATA (Ready for scikit-learn)
SELECT State_Code, Crop_Code, Season_Code, Area, 
       Annual_Rainfall, Yield_kg_ha
FROM crop_yields WHERE Yield_kg_ha IS NOT NULL;
