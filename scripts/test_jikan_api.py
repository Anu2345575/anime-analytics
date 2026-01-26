"""
Test script to fetch anime data from Jikan API
"""
import requests
import json
import time
from pathlib import Path

# Jikan API base URL
JIKAN_API_BASE = "https://api.jikan.moe/v4"

# Rate limit: 0.5 seconds between requests (2 req/sec)
RATE_LIMIT_DELAY = 0.5

def fetch_anime(anime_id):
    """
    Fetch anime data from Jikan API
    
    Args:
        anime_id: MyAnimeList anime ID
    
    Returns:
        Dictionary with anime data or None if failed
    """
    try:
        url = f"{JIKAN_API_BASE}/anime/{anime_id}/full"
        print(f"Fetching: {url}")
        
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        time.sleep(RATE_LIMIT_DELAY)  # Respect rate limit
        
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching anime {anime_id}: {e}")
        return None

def save_to_json(data, filename):
    """Save data to JSON file"""
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        print(f"✓ Saved to {filename}")
        return True
    except Exception as e:
        print(f"✗ Error saving file: {e}")
        return False

def main():
    """Main test function"""
    
    print("=" * 60)
    print("JIKAN API TEST")
    print("=" * 60)
    
    # Popular anime IDs to test with
    # These are some of the most popular anime
    test_anime_ids = [
        1,      # Cowboy Bebop
        5,      # Fullmetal Alchemist
        269,    # Fullmetal Alchemist: Brotherhood
        9253,   # Demon Slayer
        28977,  # Jujutsu Kaisen
        37991,  # Jujutsu Kaisen Season 2
    ]
    
    print(f"\nFetching {len(test_anime_ids)} anime from Jikan API...\n")
    
    # Create data directory if it doesn't exist
    data_dir = Path(__file__).parent.parent / 'data'
    data_dir.mkdir(exist_ok=True)
    
    successful = 0
    failed = 0
    
    for anime_id in test_anime_ids:
        print(f"\n--- Anime ID: {anime_id} ---")
        
        data = fetch_anime(anime_id)
        
        if data:
            # Extract key information
            anime_info = data.get('data', {})
            title = anime_info.get('title', 'Unknown')
            score = anime_info.get('score', 'N/A')
            episodes = anime_info.get('episodes', 'N/A')
            
            print(f"Title: {title}")
            print(f"Score: {score}")
            print(f"Episodes: {episodes}")
            
            # Save to file
            filename = data_dir / f"anime_{anime_id}.json"
            if save_to_json(data, filename):
                successful += 1
            else:
                failed += 1
        else:
            failed += 1
    
    print(f"\n" + "=" * 60)
    print(f"RESULTS: {successful} successful, {failed} failed")
    print(f"Files saved in: {data_dir}/")
    print("=" * 60)
    
    # Show sample of first file
    first_file = data_dir / "anime_1.json"
    if first_file.exists():
        print(f"\nSample JSON structure (from {first_file.name}):")
        with open(first_file, 'r') as f:
            sample_data = json.load(f)
            # Show key fields
            if 'data' in sample_data:
                data = sample_data['data']
                print(f"  - mal_id: {data.get('mal_id')}")
                print(f"  - title: {data.get('title')}")
                print(f"  - type: {data.get('type')}")
                print(f"  - episodes: {data.get('episodes')}")
                print(f"  - score: {data.get('score')}")
                print(f"  - rank: {data.get('rank')}")
                print(f"  - genres: {[g['name'] for g in data.get('genres', [])]}")
                print(f"  - studios: {[s['name'] for s in data.get('studios', [])]}")

if __name__ == "__main__":
    main()