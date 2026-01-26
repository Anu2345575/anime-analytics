"""
Production-grade ETL Pipeline for Project 1: Anime Viewership Analysis

This script:
1. Fetches anime data from Jikan API
2. Uploads raw JSON to S3
3. Transforms to clean CSV
4. Validates data quality
5. Logs all operations

Author: AST
Date: 2026-01-26
"""

import requests
import json
import time
import pandas as pd
import boto3
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
from dotenv import load_dotenv
import os
from functools import wraps

# ============================================================================
# CONFIGURATION
# ============================================================================

# Load environment variables
load_dotenv(Path(__file__).parent.parent / '.env')

JIKAN_API_BASE = "https://api.jikan.moe/v4"
RATE_LIMIT_DELAY = 0.55  # 0.55 seconds between requests (safe for 2 req/sec limit)
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
AWS_S3_BUCKET = os.getenv('AWS_S3_BUCKET', 'anime-analytics-bucket')
S3_RAW_PREFIX = 'anime-impact/raw/'
S3_PROCESSED_PREFIX = 'anime-impact/processed/'

# Local paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
LOGS_DIR = PROJECT_ROOT / 'logs'
CONFIG_DIR = PROJECT_ROOT / 'config'
ANIME_IDS_FILE = CONFIG_DIR / 'anime_ids.txt'

# Create directories
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging():
    """Configure logging to file and console"""
    
    # Create timestamp for log file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = LOGS_DIR / f"etl_pipeline_{timestamp}.log"
    
    # Create logger
    logger = logging.getLogger('ETL_Pipeline')
    logger.setLevel(logging.DEBUG)
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Formatter
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
# UTILITY FUNCTIONS
# ============================================================================

def retry_on_failure(max_retries=MAX_RETRIES, delay=RETRY_DELAY):
    """Decorator for retrying failed API calls"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                        time.sleep(delay)
                    else:
                        logger.error(f"All {max_retries} attempts failed: {e}")
                        return None
        return wrapper
    return decorator

# ============================================================================
# JIKAN API OPERATIONS
# ============================================================================

class JikanAPIClient:
    """Client for Jikan API operations"""
    
    def __init__(self, base_url=JIKAN_API_BASE, rate_limit_delay=RATE_LIMIT_DELAY):
        self.base_url = base_url
        self.rate_limit_delay = rate_limit_delay
        self.requests_made = 0
        self.last_request_time = None
    
    @retry_on_failure()
    def fetch_anime(self, anime_id: int) -> Optional[Dict]:
        """
        Fetch full anime data from Jikan API
        
        Args:
            anime_id: MyAnimeList anime ID
        
        Returns:
            JSON response or None if failed
        """
        try:
            # Respect rate limit
            if self.last_request_time:
                elapsed = time.time() - self.last_request_time
                if elapsed < self.rate_limit_delay:
                    time.sleep(self.rate_limit_delay - elapsed)
            
            url = f"{self.base_url}/anime/{anime_id}/full"
            
            logger.debug(f"Fetching: {url}")
            
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            self.requests_made += 1
            self.last_request_time = time.time()
            
            return response.json()
        
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                logger.warning(f"Anime {anime_id} not found (404)")
            else:
                logger.error(f"HTTP error for anime {anime_id}: {e}")
            return None
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for anime {anime_id}: {e}")
            return None

# ============================================================================
# S3 OPERATIONS
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
    
    def upload_json(self, data: Dict, s3_key: str) -> bool:
        """Upload JSON data to S3"""
        try:
            body = json.dumps(data, indent=2, default=str)
            
            self.client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=body,
                ContentType='application/json',
                Metadata={'source': 'jikan_api', 'uploaded_at': datetime.now().isoformat()}
            )
            
            logger.debug(f"Uploaded to s3://{self.bucket}/{s3_key}")
            return True
        
        except Exception as e:
            logger.error(f"Error uploading {s3_key}: {e}")
            return False
    
    def upload_csv(self, csv_path: str, s3_key: str) -> bool:
        """Upload CSV file to S3"""
        try:
            self.client.upload_file(
                csv_path,
                self.bucket,
                s3_key,
                ExtraArgs={'ContentType': 'text/csv'}
            )
            
            logger.info(f"Uploaded CSV to s3://{self.bucket}/{s3_key}")
            return True
        
        except Exception as e:
            logger.error(f"Error uploading CSV {s3_key}: {e}")
            return False
    
    def list_objects(self, prefix: str) -> List[str]:
        """List objects in S3 with prefix"""
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix,
                MaxKeys=1000
            )
            
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            return []
        
        except Exception as e:
            logger.error(f"Error listing objects: {e}")
            return []

# ============================================================================
# DATA TRANSFORMATION
# ============================================================================

class DataTransformer:
    """Transform raw anime JSON to clean structured data"""
    
    @staticmethod
    def parse_anime_json(anime_data: Dict) -> Optional[Dict]:
        """
        Parse anime JSON and extract key fields
        
        Args:
            anime_data: Raw JSON response from Jikan API
        
        Returns:
            Dictionary with extracted fields or None if invalid
        """
        try:
            if not anime_data or 'data' not in anime_data:
                return None
            
            anime = anime_data['data']
            
            # Validate required fields
            if 'mal_id' not in anime or 'title' not in anime:
                logger.warning(f"Missing required fields in anime data")
                return None
            
            # Extract and transform data
            parsed = {
                'anime_id': anime.get('mal_id'),
                'title': anime.get('title'),
                'title_english': anime.get('title_english'),
                'title_japanese': anime.get('title_japanese'),
                'anime_type': anime.get('type'),
                'episodes': anime.get('episodes'),
                'status': anime.get('status'),
                'aired_from': anime.get('aired', {}).get('from'),
                'aired_to': anime.get('aired', {}).get('to'),
                'score': anime.get('score'),
                'scored_by': anime.get('scored_by'),
                'rank': anime.get('rank'),
                'popularity_rank': anime.get('popularity'),
                'members': anime.get('members'),
                'favorites': anime.get('favorites'),
                'source': anime.get('source'),
                'rating': anime.get('rating'),
                'season': anime.get('season'),
                'season_year': anime.get('year'),
                'duration': anime.get('duration'),
                'studios': '|'.join([s['name'] for s in anime.get('studios', [])]),
                'producers': '|'.join([p['name'] for p in anime.get('producers', [])]),
                'genres': '|'.join([g['name'] for g in anime.get('genres', [])]),
                'extracted_at': datetime.now().isoformat()
            }
            
            return parsed
        
        except Exception as e:
            logger.error(f"Error parsing anime data: {e}")
            return None

# ============================================================================
# DATA VALIDATION
# ============================================================================

class DataValidator:
    """Validate data quality"""
    
    @staticmethod
    def validate_anime_record(record: Dict) -> bool:
        """Validate a single anime record"""
        
        # Check required fields
        if not record.get('anime_id') or not record.get('title'):
            return False
        
        # Check data types
        if not isinstance(record.get('anime_id'), int):
            return False
        
        # Check value ranges
        if record.get('score') is not None:
            if not (0 <= record['score'] <= 10):
                return False
        
        if record.get('episodes') is not None:
            if record['episodes'] < 0:
                return False
        
        return True
    
    @staticmethod
    def validate_dataframe(df: pd.DataFrame) -> Dict:
        """Validate entire dataframe and return report"""
        
        validation_report = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'null_counts': df.isnull().sum().to_dict(),
            'duplicate_anime_ids': df['anime_id'].duplicated().sum(),
            'rows_with_valid_scores': (df['score'] > 0).sum(),
            'avg_score': df['score'].mean(),
            'avg_episodes': df['episodes'].mean(),
        }
        
        return validation_report

# ============================================================================
# MAIN ETL PIPELINE
# ============================================================================

class ETLPipeline:
    """Main ETL pipeline orchestrator"""
    
    def __init__(self):
        self.jikan_client = JikanAPIClient()
        self.s3_client = S3Client(
            AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY,
            AWS_DEFAULT_REGION,
            AWS_S3_BUCKET
        )
        self.transformer = DataTransformer()
        self.validator = DataValidator()
        
        self.total_anime = 0
        self.successful_fetches = 0
        self.failed_fetches = 0
        self.skipped_anime = 0
        self.records = []
    
    def load_anime_ids(self) -> List[int]:
        """Load anime IDs from config file"""
        try:
            with open(ANIME_IDS_FILE, 'r') as f:
                anime_ids = [int(line.strip()) for line in f if line.strip()]
            
            logger.info(f"Loaded {len(anime_ids)} anime IDs from config")
            return anime_ids
        
        except Exception as e:
            logger.error(f"Error loading anime IDs: {e}")
            return []
    
    def fetch_and_transform(self, anime_ids: List[int]) -> None:
        """Fetch anime data and transform to CSV"""
        
        self.total_anime = len(anime_ids)
        logger.info(f"Starting ETL pipeline for {self.total_anime} anime")
        
        for idx, anime_id in enumerate(anime_ids, 1):
            logger.info(f"Processing anime {idx}/{self.total_anime} (ID: {anime_id})")
            
            # Fetch from API
            raw_data = self.jikan_client.fetch_anime(anime_id)
            
            if not raw_data:
                self.failed_fetches += 1
                logger.warning(f"Skipped anime {anime_id} - fetch failed")
                continue
            
            # Upload raw JSON to S3
            s3_key = f"{S3_RAW_PREFIX}{anime_id}/full.json"
            if not self.s3_client.upload_json(raw_data, s3_key):
                logger.warning(f"Failed to upload raw data for anime {anime_id}")
            
            # Transform
            parsed = self.transformer.parse_anime_json(raw_data)
            
            if not parsed:
                self.skipped_anime += 1
                logger.warning(f"Skipped anime {anime_id} - parsing failed")
                continue
            
            # Validate
            if not self.validator.validate_anime_record(parsed):
                self.skipped_anime += 1
                logger.warning(f"Skipped anime {anime_id} - validation failed")
                continue
            
            # Add to records
            self.records.append(parsed)
            self.successful_fetches += 1
            
            # Log progress every 10 anime
            if idx % 10 == 0:
                logger.info(f"Progress: {idx}/{self.total_anime} completed")
        
        logger.info(f"Fetch and transform complete: {self.successful_fetches} successful, {self.failed_fetches} failed, {self.skipped_anime} skipped")
    
    def create_csv(self) -> Optional[Path]:
        """Create CSV from records"""
        
        if not self.records:
            logger.error("No records to create CSV")
            return None
        
        try:
            df = pd.DataFrame(self.records)
            
            # Validate dataframe
            validation_report = self.validator.validate_dataframe(df)
            logger.info(f"Validation report: {validation_report}")
            
            # Save locally
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_filename = f"anime_data_{timestamp}.csv"
            csv_path = DATA_DIR / csv_filename
            
            df.to_csv(csv_path, index=False)
            logger.info(f"✓ Created CSV: {csv_path}")
            
            return csv_path
        
        except Exception as e:
            logger.error(f"Error creating CSV: {e}")
            return None
    
    def upload_to_s3(self, csv_path: Path) -> bool:
        """Upload CSV to S3"""
        
        if not csv_path or not csv_path.exists():
            logger.error("CSV file does not exist")
            return False
        
        try:
            s3_key = f"{S3_PROCESSED_PREFIX}{csv_path.name}"
            
            if self.s3_client.upload_csv(str(csv_path), s3_key):
                logger.info(f"✓ CSV uploaded to S3: s3://{AWS_S3_BUCKET}/{s3_key}")
                return True
            
            return False
        
        except Exception as e:
            logger.error(f"Error uploading CSV: {e}")
            return False
    
    def generate_report(self) -> None:
        """Generate and log final report"""
        
        logger.info("\n" + "=" * 60)
        logger.info("ETL PIPELINE COMPLETION REPORT")
        logger.info("=" * 60)
        logger.info(f"Total anime requested: {self.total_anime}")
        logger.info(f"Successfully fetched: {self.successful_fetches}")
        logger.info(f"Failed fetches: {self.failed_fetches}")
        logger.info(f"Skipped (validation): {self.skipped_anime}")
        logger.info(f"Success rate: {(self.successful_fetches / self.total_anime * 100):.1f}%")
        logger.info(f"Total records in CSV: {len(self.records)}")
        logger.info("=" * 60 + "\n")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main entry point"""
    
    logger.info("=" * 60)
    logger.info("STARTING FULL ETL PIPELINE")
    logger.info("=" * 60)
    logger.info(f"Project root: {PROJECT_ROOT}")
    logger.info(f"S3 bucket: {AWS_S3_BUCKET}")
    logger.info(f"Raw prefix: {S3_RAW_PREFIX}")
    logger.info(f"Processed prefix: {S3_PROCESSED_PREFIX}")
    logger.info("=" * 60 + "\n")
    
    # Initialize pipeline
    pipeline = ETLPipeline()
    
    # Load anime IDs
    anime_ids = pipeline.load_anime_ids()
    
    if not anime_ids:
        logger.error("No anime IDs loaded. Exiting.")
        return
    
    # Run ETL
    start_time = time.time()
    
    pipeline.fetch_and_transform(anime_ids)
    
    csv_path = pipeline.create_csv()
    
    if csv_path:
        pipeline.upload_to_s3(csv_path)
    
    pipeline.generate_report()
    
    elapsed_time = time.time() - start_time
    logger.info(f"Total execution time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")

if __name__ == "__main__":
    main()
