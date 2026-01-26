"""
Download sample data from S3 to verify upload worked
"""
import os
from pathlib import Path
from dotenv import load_dotenv
import boto3
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load credentials
load_dotenv(Path(__file__).parent.parent / '.env')

def get_s3_client():
    """Create S3 client"""
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    )

def download_from_s3(s3_key, local_file, bucket_name):
    """
    Download file from S3
    
    Args:
        s3_key: S3 object key
        local_file: Local file path to save to
        bucket_name: S3 bucket name
    
    Returns:
        True if successful, False otherwise
    """
    try:
        s3 = get_s3_client()
        
        logger.info(f"Downloading s3://{bucket_name}/{s3_key} to {local_file}")
        
        # Create parent directory if it doesn't exist
        Path(local_file).parent.mkdir(parents=True, exist_ok=True)
        
        s3.download_file(bucket_name, s3_key, local_file)
        
        logger.info(f"✓ Successfully downloaded to {local_file}")
        return True
        
    except Exception as e:
        logger.error(f"✗ Error downloading {s3_key}: {e}")
        return False

def main():
    """Main download function"""
    
    bucket_name = os.getenv('AWS_S3_BUCKET', 'anime-analytics-bucket')
    s3_prefix = 'anime-impact/raw/'
    
    print("=" * 60)
    print("DOWNLOADING SAMPLE DATA FROM S3")
    print("=" * 60)
    print(f"Bucket: {bucket_name}")
    print(f"Prefix: {s3_prefix}\n")
    
    # List objects in S3
    try:
        s3 = get_s3_client()
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=s3_prefix,
            MaxKeys=50
        )
        
        if 'Contents' not in response:
            logger.warning("No objects found in S3 to download")
            return
        
        objects = response['Contents']
        print(f"Found {len(objects)} objects to download\n")
        
        # Create download directory
        download_dir = Path(__file__).parent.parent / 'data' / 'downloaded'
        
        successful = 0
        failed = 0
        
        for obj in objects:
            s3_key = obj['Key']
            # Skip if it's just a folder (ends with /)
            if s3_key.endswith('/'):
                continue
            
            # Create local path
            local_file = download_dir / s3_key.replace(s3_prefix, '')
            
            if download_from_s3(s3_key, str(local_file), bucket_name):
                successful += 1
            else:
                failed += 1
        
        print(f"\n" + "=" * 60)
        print(f"RESULTS: {successful} successful, {failed} failed")
        print(f"Files saved to: {download_dir}/")
        print("=" * 60)
        
        # Verify downloaded files
        if successful > 0:
            print(f"\nVerifying downloaded files:")
            downloaded_files = list(download_dir.glob('**/*.json'))
            for f in sorted(downloaded_files)[:5]:
                size_kb = f.stat().st_size / 1024
                print(f"  - {f.name} ({size_kb:.1f} KB)")
            if len(downloaded_files) > 5:
                print(f"  ... and {len(downloaded_files) - 5} more files")
        
    except Exception as e:
        logger.error(f"Error listing S3 objects: {e}")

if __name__ == "__main__":
    main()
