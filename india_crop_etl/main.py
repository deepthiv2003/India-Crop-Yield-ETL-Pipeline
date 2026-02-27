#!/usr/bin/env python3

import pandas as pd
import numpy as np
import sqlite3
import kagglehub
import os
from datetime import datetime
import logging
from pathlib import Path

# FIXED: Windows-compatible logging (NO emojis)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('etl.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class IndiaCropETL:
    def __init__(self, db_path='india_crops.db'):
        self.db_path = Path(db_path)
        self.conn = None
        
    def extract(self):
        """EXTRACT: Full India Crop Yield Dataset"""
        logger.info("EXTRACTING Full India Dataset...")
        try:
            dataset_path = kagglehub.dataset_download("akshatgupta7/crop-yield-in-indian-states-dataset")
            df = pd.read_csv(os.path.join(dataset_path, "crop_yield.csv"))
            logger.info(f"SUCCESS: {df.shape[0]:,} rows | {df['State'].nunique()} states")
            logger.info(f"States sample: {df['State'].unique()[:5].tolist()}")
            return df
        except Exception as e:
            logger.error(f"EXTRACT FAILED: {e}")
            raise
    
    def transform(self, df):
        """TRANSFORM: ML-ready features"""
        logger.info("TRANSFORMING dataset...")
        df_clean = df.copy()
        
        # Clean data
        df_clean['State'] = df_clean['State'].str.strip().str.title()
        df_clean = df_clean.dropna(subset=['Area', 'Production'])
        
        # ML Features
        df_clean['Yield_kg_ha'] = (df_clean['Production'] * 1000) / df_clean['Area']
        df_clean['Rainfall_mm_ha'] = df_clean['Annual_Rainfall'] / df_clean['Area']
        
        # Categorical encoding
        df_clean['Season_Code'] = df_clean['Season'].astype('category').cat.codes
        df_clean['Crop_Code'] = df_clean['Crop'].astype('category').cat.codes
        df_clean['State_Code'] = df_clean['State'].astype('category').cat.codes
        
        # Outlier removal
        for col in ['Yield_kg_ha', 'Rainfall_mm_ha']:
            q01, q99 = df_clean[col].quantile([0.01, 0.99])
            df_clean = df_clean[(df_clean[col] >= q01) & (df_clean[col] <= q99)]
        
        df_clean['etl_timestamp'] = datetime.now().isoformat()
        logger.info(f"TRANSFORMED: {df_clean.shape[0]:,} clean records")
        return df_clean
    
    def load(self, df):
        """LOAD: FIXED SQLite (small chunks for Windows)"""
        logger.info("LOADING to SQLite...")
        self.conn = sqlite3.connect(self.db_path)
        
        # FIXED: Smaller chunks for SQLite limit
        chunk_size = 500  # Safe for SQLite
        total_chunks = len(df) // chunk_size + 1
        
        for i in range(total_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, len(df))
            chunk = df.iloc[start_idx:end_idx]
            
            chunk.to_sql('crop_yields', self.conn, if_exists='append' if i > 0 else 'replace',
                        index=False, method='multi')
            
            logger.info(f"Loaded chunk {i+1}/{total_chunks} ({chunk.shape[0]} rows)")
        
        # Indexes
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_state ON crop_yields(State)',
            'CREATE INDEX IF NOT EXISTS idx_crop ON crop_yields(Crop)',
            'CREATE INDEX IF NOT EXISTS idx_year ON crop_yields(Crop_Year)',
            'CREATE INDEX IF NOT EXISTS idx_yield ON crop_yields(Yield_kg_ha)'
        ]
        
        for idx_sql in indexes:
            self.conn.execute(idx_sql)
        
        self.conn.commit()
        logger.info(f"DATABASE READY: {self.db_path.absolute()} ({len(df):,} records)")
    
    def analytics(self):
        """SQL ANALYTICS DASHBOARD"""
        logger.info("GENERATING ANALYTICS...")
        queries = {
            'NATIONAL_OVERVIEW': """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT State) as states_count,
                    COUNT(DISTINCT Crop) as crops_count,
                    ROUND(AVG(Yield_kg_ha), 1) as avg_yield_kg_ha
                FROM crop_yields
            """,
            'TOP_10_CROPS': """
                SELECT Crop, ROUND(AVG(Yield_kg_ha), 1) as avg_yield_kg_ha,
                       COUNT(*) as records
                FROM crop_yields GROUP BY Crop 
                ORDER BY avg_yield_kg_ha DESC LIMIT 10
            """,
            'TOP_10_STATES': """
                SELECT State, ROUND(AVG(Yield_kg_ha), 1) as avg_yield_kg_ha,
                       COUNT(DISTINCT Crop) as crop_types
                FROM crop_yields GROUP BY State 
                ORDER BY avg_yield_kg_ha DESC LIMIT 10
            """
        }
        
        print("\n" + "="*60)
        for title, sql in queries.items():
            print(f"\n{title}")
            print("-" * 40)
            result = pd.read_sql_query(sql, self.conn)
            print(result.to_string(index=False))
        
        # Save ML data
        ml_data = pd.read_sql_query("""
            SELECT State_Code, Crop_Code, Season_Code, Area, 
                   Annual_Rainfall, Yield_kg_ha 
            FROM crop_yields WHERE Yield_kg_ha IS NOT NULL
        """, self.conn)
        ml_data.to_csv('ml_ready_dataset.csv', index=False)
        print(f"\nML dataset saved: ml_ready_dataset.csv ({len(ml_data)} rows)")
    
    def run_pipeline(self):
        """COMPLETE PIPELINE EXECUTION"""
        try:
            logger.info("STARTING INDIA CROP ETL PIPELINE")
            df_raw = self.extract()
            df_clean = self.transform(df_raw)
            self.load(df_clean)
            self.analytics()
            
            print(f"\nSUCCESS! COMPLETE PIPELINE FINISHED")
            print(f"Database: {self.db_path.absolute()}")
            print(f"ML Data: ml_ready_dataset.csv")
            return True
            
        except Exception as e:
            logger.error(f"PIPELINE FAILED: {e}")
            return False
        finally:
            if self.conn:
                self.conn.close()

# EXECUTE
if __name__ == "__main__":
    project = IndiaCropETL()
    project.run_pipeline()

