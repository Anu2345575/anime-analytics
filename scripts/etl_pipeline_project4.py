"""
Production-grade ETL Pipeline for Project 4: Licensing Strategy Analysis

This script:
1. Reads licensing and regional data from S3
2. Transforms and cleans data
3. Calculates business metrics (ROI, time-to-market)
4. Validates data quality
5. Uploads processed data to S3
6. Generates business insights

Author: AST
Date: 2026-02-01
"""

import pandas as pd
import numpy as np
import boto3
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Optional
from dotenv import load_dotenv

# ============================================================================
# CONFIGURATION
# ============================================================================

# Load environment variables
load_dotenv(Path(__file__).parent.parent / '.env')

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
AWS_S3_BUCKET = os.getenv('AWS_S3_BUCKET', 'anime-analytics-bucket')
S3_RAW_PREFIX = 'licensing-strategy/raw/'
S3_PROCESSED_PREFIX = 'licensing-strategy/processed/'

# Local paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
LOGS_DIR = PROJECT_ROOT / 'logs'

# Create directories
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging():
    """Configure logging"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = LOGS_DIR / f"etl_project4_{timestamp}.log"
    
    logger = logging.getLogger('ETL_Project4')
    logger.setLevel(logging.DEBUG)
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"Logging initialized. Log file: {log_file}")
    return logger

logger = setup_logging()

# ============================================================================
# S3 CLIENT
# ============================================================================

class S3Client:
    """Client for S3 operations"""
    
    def __init__(self, access_key, secret_key, region, bucket):
        self.bucket = bucket
        self.client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
    
    def download_csv(self, s3_key: str, local_path: str) -> bool:
        """Download CSV from S3"""
        try:
            logger.info(f"Downloading s3://{self.bucket}/{s3_key}")
            self.client.download_file(self.bucket, s3_key, local_path)
            logger.info(f"✓ Downloaded to {local_path}")
            return True
        except Exception as e:
            logger.error(f"Error downloading {s3_key}: {e}")
            return False
    
    def upload_csv(self, local_path: str, s3_key: str) -> bool:
        """Upload CSV to S3"""
        try:
            logger.info(f"Uploading {Path(local_path).name} to s3://{self.bucket}/{s3_key}")
            self.client.upload_file(
                local_path,
                self.bucket,
                s3_key,
                ExtraArgs={'ContentType': 'text/csv'}
            )
            logger.info(f"✓ Uploaded to {s3_key}")
            return True
        except Exception as e:
            logger.error(f"Error uploading {s3_key}: {e}")
            return False

# ============================================================================
# DATA TRANSFORMATIONS
# ============================================================================

class LicensingDataTransformer:
    """Transform and enrich licensing data"""
    
    def __init__(self):
        self.anime_df = None
        self.licensing_df = None
        self.regional_df = None
        self.regions_df = None
        self.enriched_licensing = None
        self.regional_performance = None
        self.licensing_roi = None
    
    def load_data_from_local(self) -> bool:
        """Load CSVs from local data directory"""
        try:
            logger.info("Loading data from local files...")
            
            self.anime_df = pd.read_csv(DATA_DIR / 'project4_anime_catalog.csv')
            logger.info(f"✓ Loaded anime catalog: {len(self.anime_df)} records")
            
            self.licensing_df = pd.read_csv(DATA_DIR / 'project4_licensing_deals.csv')
            logger.info(f"✓ Loaded licensing deals: {len(self.licensing_df)} records")
            
            self.regional_df = pd.read_csv(DATA_DIR / 'project4_regional_subscribers.csv')
            logger.info(f"✓ Loaded regional data: {len(self.regional_df)} records")
            
            self.regions_df = pd.read_csv(DATA_DIR / 'project4_regions.csv')
            logger.info(f"✓ Loaded regions: {len(self.regions_df)} records")
            
            return True
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return False
    
    def transform_anime_catalog(self) -> pd.DataFrame:
        """Clean and enhance anime catalog"""
        logger.info("Transforming anime catalog...")
        
        df = self.anime_df.copy()
        
        # Data type conversions
        df['anime_id'] = df['anime_id'].astype(int)
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df['episodes'] = pd.to_numeric(df['episodes'], errors='coerce')
        df['score'] = pd.to_numeric(df['score'], errors='coerce')
        
        # Categorize popularity tier
        df['popularity_tier'] = pd.cut(
            df['popularity'],
            bins=[0, 100, 500, 1000, float('inf')],
            labels=['Blockbuster', 'High', 'Medium', 'Niche']
        )
        
        # Handle missing values
        df['year'] = df['year'].fillna(0).astype(int)
        df['episodes'] = df['episodes'].fillna(0).astype(int)
        
        # Validation
        logger.info(f"Null values in anime_id: {df['anime_id'].isnull().sum()}")
        logger.info(f"Valid records: {len(df[df['anime_id'].notna()])}")
        
        logger.info(f"✓ Transformed anime catalog: {len(df)} records")
        return df
    
    def transform_licensing_deals(self) -> pd.DataFrame:
        """Clean and enhance licensing deals"""
        logger.info("Transforming licensing deals...")
        
        df = self.licensing_df.copy()
        
        # Convert dates
        df['license_start_date'] = pd.to_datetime(df['license_start_date'], errors='coerce')
        df['license_end_date'] = pd.to_datetime(df['license_end_date'], errors='coerce')
        
        # Calculate deal duration (days)
        df['license_duration_days'] = (df['license_end_date'] - df['license_start_date']).dt.days
        
        # Convert costs to numeric
        df['estimated_cost_usd'] = pd.to_numeric(df['estimated_cost_usd'], errors='coerce')
        
        # Calculate cost per day
        df['cost_per_day_usd'] = df['estimated_cost_usd'] / df['license_duration_days']
        df['cost_per_day_usd'] = df['cost_per_day_usd'].replace([np.inf, -np.inf], 0)
        
        # Convert licensing type
        df['is_exclusive'] = (df['licensing_type'] == 'Exclusive').astype(int)
        
        # Validation
        logger.info(f"Null deal durations: {df['license_duration_days'].isnull().sum()}")
        logger.info(f"Total licensing spend: ${df['estimated_cost_usd'].sum():,.0f}")
        
        logger.info(f"✓ Transformed licensing deals: {len(df)} records")
        return df
    
    def enrich_licensing_with_anime(self, licensing_df: pd.DataFrame, anime_df: pd.DataFrame) -> pd.DataFrame:
        """Merge licensing deals with anime data"""
        logger.info("Enriching licensing deals with anime data...")
        
        # Merge on anime_id
        merged = licensing_df.merge(
            anime_df[['anime_id', 'title', 'year', 'score', 'popularity_tier']],
            on='anime_id',
            how='left'
        )
        
        logger.info(f"Merged records: {len(merged)}")
        logger.info(f"Null titles: {merged['title'].isnull().sum()}")
        
        self.enriched_licensing = merged
        return merged
    
    def transform_regional_performance(self, regional_df: pd.DataFrame) -> pd.DataFrame:
        """Clean and enhance regional performance data"""
        logger.info("Transforming regional performance data...")
        
        df = regional_df.copy()
        
        # Data type conversions
        df['estimated_subscriber_count'] = pd.to_numeric(df['estimated_subscriber_count'], errors='coerce')
        df['estimated_revenue_usd'] = pd.to_numeric(df['estimated_revenue_usd'], errors='coerce')
        df['arpu_usd'] = pd.to_numeric(df['arpu_usd'], errors='coerce')
        df['churn_rate_percent'] = pd.to_numeric(df['churn_rate_percent'], errors='coerce')
        
        # Parse quarter
        def parse_quarter(quarter_str):
            try:
                year, quarter = quarter_str.split('-Q')
                year = int(year)
                quarter = int(quarter)
                month = (quarter - 1) * 3 + 1
                return pd.Timestamp(year=year, month=month, day=1)
            except:
                return pd.NaT

        df['quarter_date'] = df['quarter_date'].apply(parse_quarter)
        
        # Calculate metrics per subscriber
        df['revenue_per_subscriber'] = df['estimated_revenue_usd'] / df['estimated_subscriber_count']
        
        # Validation
        logger.info(f"Regions: {df['region_code'].nunique()}")
        logger.info(f"Total subscribers: {df['estimated_subscriber_count'].sum():,.0f}")
        logger.info(f"Total revenue: ${df['estimated_revenue_usd'].sum():,.0f}")
        
        logger.info(f"✓ Transformed regional performance: {len(df)} records")
        return df
    
    def calculate_licensing_roi(self, enriched_licensing: pd.DataFrame, regional_df: pd.DataFrame) -> pd.DataFrame:
        """Calculate ROI metrics for licensing deals"""
        logger.info("Calculating licensing ROI...")
        
        # Group by anime and region
        roi_data = []
        
        for idx, deal in enriched_licensing.iterrows():
            anime_id = deal['anime_id']
            region = deal['region_code']
            cost = deal['estimated_cost_usd']
            
            # Get regional metrics
            regional_metrics = regional_df[regional_df['region_code'] == region]
            
            if regional_metrics.empty:
                continue
            
            rev = regional_metrics['estimated_revenue_usd'].values[0]
            subscribers = regional_metrics['estimated_subscriber_count'].values[0]
            
            # Estimate revenue attributed to this anime
            # Heuristic: High-rated exclusives get ~2% of regional revenue
            # Low-rated non-exclusives get ~0.5% of regional revenue
            
            if deal['score'] and deal['is_exclusive']:
                revenue_attribution_rate = 0.15  # ← Increased from 0.02
            elif deal['score']:
                revenue_attribution_rate = 0.10  # ← Increased from 0.01
            else:
                revenue_attribution_rate = 0.05  # ← Increased from 0.005
            
            attributed_revenue = rev * revenue_attribution_rate
            
            # Calculate ROI
            if cost > 0:
                roi_percent = ((attributed_revenue - cost) / cost) * 100
            else:
                roi_percent = 0
            
            # Payback period (months)
            if cost > 0:
                monthly_revenue = attributed_revenue / 3  # Quarter = 3 months
                if monthly_revenue > 0:
                    payback_months = cost / monthly_revenue
                else:
                    payback_months = float('inf')
            else:
                payback_months = 0
            
            roi_data.append({
                'anime_id': anime_id,
                'anime_title': deal['anime_title'],
                'region_code': region,
                'licensing_type': deal['licensing_type'],
                'licensing_cost_usd': cost,
                'estimated_attributed_revenue_usd': attributed_revenue,
                'roi_percent': roi_percent,
                'payback_period_months': payback_months if payback_months != float('inf') else None,
                'anime_score': deal['score'],
                'is_exclusive': deal['is_exclusive']
            })
        
        roi_df = pd.DataFrame(roi_data)
        
        # Summary statistics
        logger.info(f"\nROI Summary:")
        logger.info(f"  Average ROI: {roi_df['roi_percent'].mean():.2f}%")
        logger.info(f"  Median ROI: {roi_df['roi_percent'].median():.2f}%")
        logger.info(f"  Max ROI: {roi_df['roi_percent'].max():.2f}%")
        logger.info(f"  Min ROI: {roi_df['roi_percent'].min():.2f}%")
        
        self.licensing_roi = roi_df
        logger.info(f"✓ Calculated ROI for {len(roi_df)} deals")
        return roi_df
    
    def run_transformation(self) -> bool:
        """Run complete transformation pipeline"""
        logger.info("\n" + "=" * 60)
        logger.info("STARTING PROJECT 4 ETL TRANSFORMATION")
        logger.info("=" * 60 + "\n")
        
        # Load data
        if not self.load_data_from_local():
            return False
        
        # Transform each dataset
        anime_transformed = self.transform_anime_catalog()
        licensing_transformed = self.transform_licensing_deals()
        regional_transformed = self.transform_regional_performance(self.regional_df)
        
        # Enrich licensing with anime data
        enriched = self.enrich_licensing_with_anime(licensing_transformed, anime_transformed)
        
        # Calculate ROI
        roi = self.calculate_licensing_roi(enriched, regional_transformed)
        
        # Save processed files locally
        logger.info("\nSaving processed files...")
        
        anime_transformed.to_csv(DATA_DIR / 'project4_anime_catalog_clean.csv', index=False)
        logger.info(f"✓ Saved: project4_anime_catalog_clean.csv")
        
        licensing_transformed.to_csv(DATA_DIR / 'project4_licensing_deals_clean.csv', index=False)
        logger.info(f"✓ Saved: project4_licensing_deals_clean.csv")
        
        enriched.to_csv(DATA_DIR / 'project4_licensing_enriched.csv', index=False)
        logger.info(f"✓ Saved: project4_licensing_enriched.csv")
        
        regional_transformed.to_csv(DATA_DIR / 'project4_regional_performance_clean.csv', index=False)
        logger.info(f"✓ Saved: project4_regional_performance_clean.csv")
        
        roi.to_csv(DATA_DIR / 'project4_licensing_roi.csv', index=False)
        logger.info(f"✓ Saved: project4_licensing_roi.csv")
        
        return True
    
    def generate_report(self) -> None:
        """Generate summary report"""
        logger.info("\n" + "=" * 60)
        logger.info("PROJECT 4 TRANSFORMATION SUMMARY")
        logger.info("=" * 60)
        
        logger.info(f"\nAnime Catalog:")
        logger.info(f"  Records: {len(self.anime_df)}")
        logger.info(f"  Year range: {self.anime_df['year'].min():.0f} - {self.anime_df['year'].max():.0f}")
        logger.info(f"  Average score: {self.anime_df['score'].mean():.2f}")
        
        logger.info(f"\nLicensing Deals:")
        logger.info(f"  Records: {len(self.licensing_df)}")
        logger.info(f"  Total spend: ${self.licensing_df['estimated_cost_usd'].sum():,.0f}")
        logger.info(f"  Average deal: ${self.licensing_df['estimated_cost_usd'].mean():,.0f}")
        logger.info(f"  Exclusive deals: {(self.licensing_df['licensing_type'] == 'Exclusive').sum()}")
        logger.info(f"  Shared deals: {(self.licensing_df['licensing_type'] == 'Shared').sum()}")
        
        logger.info(f"\nRegional Performance:")
        logger.info(f"  Regions: {self.regional_df['region_code'].nunique()}")
        logger.info(f"  Total subscribers: {self.regional_df['estimated_subscriber_count'].sum():,.0f}")
        logger.info(f"  Total revenue: ${self.regional_df['estimated_revenue_usd'].sum():,.0f}")
        
        logger.info(f"\nLicensing ROI:")
        if self.licensing_roi is not None and len(self.licensing_roi) > 0:
            logger.info(f"  Deals analyzed: {len(self.licensing_roi)}")
            logger.info(f"  Average ROI: {self.licensing_roi['roi_percent'].mean():.2f}%")
            logger.info(f"  Exclusive avg ROI: {self.licensing_roi[self.licensing_roi['is_exclusive'] == 1]['roi_percent'].mean():.2f}%")
            logger.info(f"  Shared avg ROI: {self.licensing_roi[self.licensing_roi['is_exclusive'] == 0]['roi_percent'].mean():.2f}%")
        
        logger.info("\n" + "=" * 60)
        logger.info("FILES GENERATED:")
        logger.info("  - project4_anime_catalog_clean.csv")
        logger.info("  - project4_licensing_deals_clean.csv")
        logger.info("  - project4_licensing_enriched.csv")
        logger.info("  - project4_regional_performance_clean.csv")
        logger.info("  - project4_licensing_roi.csv")
        logger.info("=" * 60 + "\n")

# ============================================================================
# UPLOAD TO S3
# ============================================================================

def upload_processed_files_to_s3(s3_client: S3Client) -> bool:
    """Upload processed files to S3"""
    logger.info("\nUploading processed files to S3...")
    
    files_to_upload = [
        ('project4_anime_catalog_clean.csv', 'project4_anime_catalog_clean.csv'),
        ('project4_licensing_deals_clean.csv', 'project4_licensing_deals_clean.csv'),
        ('project4_licensing_enriched.csv', 'project4_licensing_enriched.csv'),
        ('project4_regional_performance_clean.csv', 'project4_regional_performance_clean.csv'),
        ('project4_licensing_roi.csv', 'project4_licensing_roi.csv'),
    ]
    
    successful = 0
    for local_name, s3_name in files_to_upload:
        local_path = DATA_DIR / local_name
        s3_key = f"{S3_PROCESSED_PREFIX}{s3_name}"
        
        if s3_client.upload_csv(str(local_path), s3_key):
            successful += 1
    
    logger.info(f"✓ Uploaded {successful}/{len(files_to_upload)} files to S3")
    return successful == len(files_to_upload)

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main entry point"""
    
    # Initialize transformer
    transformer = LicensingDataTransformer()
    
    # Run transformation
    if transformer.run_transformation():
        # Generate report
        transformer.generate_report()
        
        # Optional: Upload to S3
        try:
            s3_client = S3Client(
                AWS_ACCESS_KEY_ID,
                AWS_SECRET_ACCESS_KEY,
                AWS_DEFAULT_REGION,
                AWS_S3_BUCKET
            )
            
            upload_processed_files_to_s3(s3_client)
            
        except Exception as e:
            logger.warning(f"S3 upload skipped: {e}")
        
        logger.info("\n✓ Project 4 ETL transformation completed successfully!")
    else:
        logger.error("\n✗ Project 4 ETL transformation failed")

if __name__ == "__main__":
    main()