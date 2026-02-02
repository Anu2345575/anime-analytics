"""
Upload Project 4 data to S3
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

def upload_file_to_s3(local_file, s3_key, bucket_name):
    """Upload file to S3"""
    try:
        s3 = get_s3_client()
        
        logger.info(f"Uploading {Path(local_file).name} to s3://{bucket_name}/{s3_key}")
        
        s3.upload_file(
            local_file,
            bucket_name,
            s3_key,
            ExtraArgs={'ContentType': 'text/csv'}
        )
        
        logger.info(f"✓ Successfully uploaded")
        return True
        
    except Exception as e:
        logger.error(f"✗ Error: {e}")
        return False

def main():
    """Main upload function"""
    
    bucket_name = os.getenv('AWS_S3_BUCKET', 'anime-analytics-bucket')
    s3_prefix = 'licensing-strategy/raw/'
    
    print("=" * 60)
    print("UPLOADING PROJECT 4 DATA TO S3")
    print("=" * 60)
    print(f"Bucket: {bucket_name}")
    print(f"Prefix: {s3_prefix}\n")
    
    # Find all CSV files for Project 4
    data_dir = Path(__file__).parent.parent / 'data'
    csv_files = list(data_dir.glob('project4_*.csv'))
    
    print(f"Found {len(csv_files)} CSV files to upload\n")
    
    successful = 0
    failed = 0
    
    for csv_file in sorted(csv_files):
        s3_key = f"{s3_prefix}{csv_file.name}"
        
        if upload_file_to_s3(str(csv_file), s3_key, bucket_name):
            successful += 1
        else:
            failed += 1
    
    print(f"\n" + "=" * 60)
    print(f"RESULTS: {successful} successful, {failed} failed")
    print("=" * 60)
    
    # Verify upload
    try:
        s3 = get_s3_client()
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=s3_prefix
        )
        
        if 'Contents' in response:
            print(f"\nFiles in S3:")
            for obj in response['Contents']:
                size_kb = obj['Size'] / 1024
                print(f"  - {obj['Key']} ({size_kb:.1f} KB)")
    except Exception as e:
        logger.error(f"Error listing S3: {e}")

if __name__ == "__main__":
    main()