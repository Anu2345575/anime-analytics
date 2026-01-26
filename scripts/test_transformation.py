"""
Test data transformation - parse JSON and create CSV
"""
import json
import pandas as pd
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_anime_json(json_file):
    """
    Parse anime JSON file and extract key fields
    
    Args:
        json_file: Path to JSON file
    
    Returns:
        Dictionary with extracted data
    """
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        anime = data.get('data', {})
        
        # Extract key fields
        parsed = {
            'anime_id': anime.get('mal_id'),
            'title': anime.get('title'),
            'title_english': anime.get('title_english'),
            'type': anime.get('type'),
            'episodes': anime.get('episodes'),
            'status': anime.get('status'),
            'aired_from': anime.get('aired', {}).get('from'),
            'aired_to': anime.get('aired', {}).get('to'),
            'score': anime.get('score'),
            'rank': anime.get('rank'),
            'popularity': anime.get('popularity'),
            'members': anime.get('members'),
            'source': anime.get('source'),
            'rating': anime.get('rating'),
            'season': anime.get('season'),
            'year': anime.get('year'),
            'genres': '|'.join([g['name'] for g in anime.get('genres', [])]),
            'studios': '|'.join([s['name'] for s in anime.get('studios', [])]),
        }
        
        return parsed
    
    except Exception as e:
        logger.error(f"Error parsing {json_file}: {e}")
        return None

def main():
    """Main transformation function"""
    
    print("=" * 60)
    print("DATA TRANSFORMATION TEST")
    print("=" * 60)
    
    # Find all JSON files
    data_dir = Path(__file__).parent.parent / 'data'
    json_files = list(data_dir.glob('anime_*.json'))
    
    print(f"\nFound {len(json_files)} JSON files\n")
    
    if not json_files:
        logger.warning("No JSON files found")
        return
    
    # Parse all files
    records = []
    for json_file in sorted(json_files):
        logger.info(f"Parsing {json_file.name}")
        parsed = parse_anime_json(json_file)
        if parsed:
            records.append(parsed)
    
    print(f"\nSuccessfully parsed {len(records)} files\n")
    
    # Create DataFrame
    df = pd.DataFrame(records)
    
    # Display summary
    print("Data Summary:")
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    print(f"\nColumn names:")
    for col in df.columns:
        print(f"  - {col}")
    
    # Display first few rows
    print(f"\nFirst 3 rows:")
    print(df[['anime_id', 'title', 'type', 'episodes', 'score', 'year']].head(3).to_string())
    
    # Save to CSV
    output_file = data_dir / 'anime_sample_clean.csv'
    df.to_csv(output_file, index=False)
    logger.info(f"✓ Saved cleaned data to {output_file}")
    
    # Data quality checks
    print(f"\n--- Data Quality Checks ---")
    print(f"Null values per column:")
    null_counts = df.isnull().sum()
    for col, count in null_counts[null_counts > 0].items():
        print(f"  - {col}: {count}")
    
    print(f"\nBasic statistics:")
    print(f"  - Average score: {df['score'].mean():.2f}")
    print(f"  - Average episodes: {df['episodes'].mean():.2f}")
    print(f"  - Year range: {df['year'].min()} - {df['year'].max()}")
    
    print(f"\n✓ Transformation complete!")

if __name__ == "__main__":
    main()
