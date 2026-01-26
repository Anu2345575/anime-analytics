"""
Helper functions for S3 operations - Will be used later in ETL
"""
import boto3
import os
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load credentials
load_dotenv()

def get_s3_client():
    """Create and return an S3 client"""
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    )

def upload_file(file_path, s3_key, bucket_name=None):
    """
    Upload a file to S3
    
    Args:
        file_path: Local file path (e.g., 'data/anime.json')
        s3_key: S3 object key (e.g., 'anime-impact/raw/1/full.json')
        bucket_name: S3 bucket name (uses env variable if not provided)
    
    Returns:
        True if successful, False otherwise
    """
    if bucket_name is None:
        bucket_name = os.getenv('AWS_S3_BUCKET')
    
    try:
        s3 = get_s3_client()
        s3.upload_file(file_path, bucket_name, s3_key)
        logger.info(f"✓ Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
        return True
    except Exception as e:
        logger.error(f"✗ Error uploading {file_path}: {e}")
        return False

def download_file(s3_key, file_path, bucket_name=None):
    """
    Download a file from S3
    
    Args:
        s3_key: S3 object key (e.g., 'anime-impact/raw/1/full.json')
        file_path: Local file path to save to
        bucket_name: S3 bucket name (uses env variable if not provided)
    
    Returns:
        True if successful, False otherwise
    """
    if bucket_name is None:
        bucket_name = os.getenv('AWS_S3_BUCKET')
    
    try:
        s3 = get_s3_client()
        s3.download_file(bucket_name, s3_key, file_path)
        logger.info(f"✓ Downloaded s3://{bucket_name}/{s3_key} to {file_path}")
        return True
    except Exception as e:
        logger.error(f"✗ Error downloading {s3_key}: {e}")
        return False

def list_objects(prefix='', bucket_name=None):
    """
    List objects in S3 bucket with optional prefix
    
    Args:
        prefix: S3 prefix filter (e.g., 'anime-impact/raw/')
        bucket_name: S3 bucket name (uses env variable if not provided)
    
    Returns:
        List of object keys
    """
    if bucket_name is None:
        bucket_name = os.getenv('AWS_S3_BUCKET')
    
    try:
        s3 = get_s3_client()
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' in response:
            objects = [obj['Key'] for obj in response['Contents']]
            logger.info(f"✓ Found {len(objects)} objects with prefix '{prefix}'")
            return objects
        else:
            logger.info(f"No objects found with prefix '{prefix}'")
            return []
    except Exception as e:
        logger.error(f"✗ Error listing objects: {e}")
        return []

def delete_object(s3_key, bucket_name=None):
    """
    Delete an object from S3
    
    Args:
        s3_key: S3 object key to delete
        bucket_name: S3 bucket name (uses env variable if not provided)
    
    Returns:
        True if successful, False otherwise
    """
    if bucket_name is None:
        bucket_name = os.getenv('AWS_S3_BUCKET')
    
    try:
        s3 = get_s3_client()
        s3.delete_object(Bucket=bucket_name, Key=s3_key)
        logger.info(f"✓ Deleted s3://{bucket_name}/{s3_key}")
        return True
    except Exception as e:
        logger.error(f"✗ Error deleting {s3_key}: {e}")
        return False

# Example usage (if running this file directly)
if __name__ == "__main__":
    print("S3 Helper module loaded. Use import s3_helper to use these functions.")
    
    # Test list_objects
    objects = list_objects('anime-impact/')
    print(f"Objects: {objects}")
