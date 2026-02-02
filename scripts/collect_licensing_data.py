"""
Collect sample licensing and regional data for Project 4
In a real project, this data would come from:
- MyAnimeList APIs
- Company reports
- Industry databases
- Manual research
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import requests

# Directories
DATA_DIR = Path(__file__).parent.parent / 'data'
DATA_DIR.mkdir(exist_ok=True)

class LicensingDataCollector:
    """Collect licensing and regional data"""
    
    def __init__(self):
        self.anime_data = []
        self.licensing_deals = []
        self.regional_subscribers = []
    
    def fetch_anime_for_project4(self, anime_ids: list) -> pd.DataFrame:
        """
        Fetch anime data from Jikan for licensing analysis
        Uses same Jikan API as Project 1
        """
        print("Fetching anime data from Jikan API...")
        
        records = []
        
        for anime_id in anime_ids[:20]:  # Start with 20 for testing
            try:
                url = f"https://api.jikan.moe/v4/anime/{anime_id}/full"
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                
                data = response.json()['data']
                
                record = {
                    'anime_id': data.get('mal_id'),
                    'title': data.get('title'),
                    'year': data.get('year'),
                    'episodes': data.get('episodes'),
                    'score': data.get('score'),
                    'popularity': data.get('popularity'),
                    'genres': '|'.join([g['name'] for g in data.get('genres', [])]),
                    'source': data.get('source'),
                }
                
                records.append(record)
                
                # Respect rate limit
                import time
                time.sleep(0.55)
                
                print(f"✓ Fetched: {data.get('title')}")
                
            except Exception as e:
                print(f"✗ Error fetching anime {anime_id}: {e}")
        
        return pd.DataFrame(records)
    
    def create_licensing_deals_sample(self) -> pd.DataFrame:
        """
        Create sample licensing deals data
        In reality, this would come from manual research or data partners
        """
        print("\nCreating sample licensing deals...")
        
        data = {
            'anime_id': [1, 1, 1, 5, 5, 5, 269, 269, 269, 9253, 9253],
            'anime_title': [
                'Cowboy Bebop', 'Cowboy Bebop', 'Cowboy Bebop',
                'Fullmetal Alchemist', 'Fullmetal Alchemist', 'Fullmetal Alchemist',
                'Fullmetal Alchemist: Brotherhood', 'Fullmetal Alchemist: Brotherhood', 'Fullmetal Alchemist: Brotherhood',
                'Demon Slayer', 'Demon Slayer'
            ],
            'region_code': ['US', 'UK', 'CA', 'US', 'UK', 'JP', 'US', 'UK', 'CA', 'US', 'JP'],
            'license_start_date': [
                '2022-10-01', '2022-10-15', '2022-11-01',
                '2021-01-01', '2021-01-15', '1998-10-05',
                '2012-04-05', '2012-04-05', '2012-06-01',
                '2021-10-10', '2021-10-10'
            ],
            'license_end_date': [
                '2027-10-01', '2027-10-15', '2027-11-01',
                '2026-12-31', '2026-12-31', '2005-03-31',
                '2027-12-31', '2027-12-31', '2027-12-31',
                '2026-10-10', '2026-10-10'
            ],
            'licensing_type': [
                'Exclusive', 'Shared', 'Exclusive',
                'Exclusive', 'Shared', 'Broadcast',
                'Exclusive', 'Shared', 'Exclusive',
                'Exclusive', 'Exclusive'
            ],
            'estimated_cost_usd': [
                750000, 200000, 150000,
                500000, 150000, 0,
                600000, 250000, 200000,
                1000000, 500000
            ]
        }
        
        print(f"✓ Created {len(data['anime_id'])} licensing deals")
        return pd.DataFrame(data)
    
    def create_regional_subscribers_sample(self) -> pd.DataFrame:
        """
        Create sample regional subscriber data
        In reality, from company earnings calls and analyst reports
        """
        print("\nCreating sample regional subscriber data...")
        
        data = {
            'region_code': ['US', 'CA', 'UK', 'JP', 'BR', 'MX', 'AU', 'FR'],
            'quarter_date': ['2024-Q1'] * 8,
            'estimated_subscriber_count': [
                1200000, 250000, 180000, 600000,
                150000, 120000, 100000, 90000
            ],
            'estimated_revenue_usd': [
                15600000, 2750000, 1620000, 4200000,
                600000, 360000, 400000, 360000
            ],
            'arpu_usd': [13.0, 11.0, 9.0, 7.0, 4.0, 3.0, 4.0, 4.0],
            'churn_rate_percent': [2.5, 2.8, 3.1, 2.0, 4.5, 5.0, 3.5, 3.2]
        }
        
        print(f"✓ Created {len(data['region_code'])} regions")
        return pd.DataFrame(data)
    
    def create_region_metadata(self) -> pd.DataFrame:
        """
        Create region information for dimensional table
        """
        print("\nCreating region metadata...")
        
        data = {
            'region_code': ['US', 'CA', 'UK', 'JP', 'BR', 'MX', 'AU', 'FR'],
            'region_name': [
                'United States', 'Canada', 'United Kingdom', 'Japan',
                'Brazil', 'Mexico', 'Australia', 'France'
            ],
            'market_size': [
                'Tier1', 'Tier2', 'Tier1', 'Tier1',
                'Tier2', 'Tier2', 'Tier2', 'Tier1'
            ],
            'anime_interest_index': [8.5, 7.8, 7.2, 9.8, 7.5, 7.0, 6.8, 6.5],
        }
        
        print(f"✓ Created {len(data['region_code'])} regions")
        return pd.DataFrame(data)
    
    def save_to_csv(self, df: pd.DataFrame, filename: str) -> str:
        """Save dataframe to CSV"""
        filepath = DATA_DIR / filename
        df.to_csv(filepath, index=False)
        print(f"✓ Saved: {filepath}")
        return str(filepath)
    
    def run(self):
        """Run the collector"""
        
        print("=" * 60)
        print("COLLECTING LICENSING DATA")
        print("=" * 60)
        
        # List of popular anime to analyze
        anime_ids = [1, 5, 6, 269, 9253, 28977, 37991, 40748, 50265, 48582]
        
        # Fetch anime data
        anime_df = self.fetch_anime_for_project4(anime_ids)
        self.save_to_csv(anime_df, 'project4_anime_catalog.csv')
        
        # Create licensing deals
        licensing_df = self.create_licensing_deals_sample()
        self.save_to_csv(licensing_df, 'project4_licensing_deals.csv')
        
        # Create subscriber data
        subscribers_df = self.create_regional_subscribers_sample()
        self.save_to_csv(subscribers_df, 'project4_regional_subscribers.csv')
        
        # Create region metadata
        regions_df = self.create_region_metadata()
        self.save_to_csv(regions_df, 'project4_regions.csv')
        
        print("\n" + "=" * 60)
        print("DATA COLLECTION COMPLETE")
        print("=" * 60)
        
        # Show summary
        print(f"\nAnime: {len(anime_df)} records")
        print(f"Licensing deals: {len(licensing_df)} records")
        print(f"Regional data: {len(subscribers_df)} records")
        print(f"Regions: {len(regions_df)} records")

if __name__ == "__main__":
    collector = LicensingDataCollector()
    collector.run()