#!/usr/bin/env python3
"""
FFC ADP Data Collection Script
Collects Average Draft Position data from Fantasy Football Calculator API
"""

import json
import requests
import pandas as pd
from datetime import datetime
import os
import sys
import time

class ADPDataCollector:
    def __init__(self):
        self.data_dir = "data"
        self.base_url = "https://fantasyfootballcalculator.com/api/v1/adp"
        self.current_season = 2025
        self.ensure_directories()
        
    def ensure_directories(self):
        """Create necessary directories"""
        os.makedirs(self.data_dir, exist_ok=True)
        
    def collect_adp_data(self, scoring='ppr', teams=12, position='all'):
        """Collect ADP data from FFC API"""
        try:
            url = f"{self.base_url}/{scoring}"
            params = {
                'teams': teams,
                'year': self.current_season
            }
            
            # Add position filter if specified
            if position != 'all':
                params['pos'] = position
                
            print(f"Fetching {scoring.upper()} ADP data for {teams}-team leagues, position: {position}")
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') != 'Success':
                print(f"API returned error status: {data}")
                return None
                
            players = data.get('players', [])
            meta = data.get('meta', {})
            
            print(f"Retrieved {len(players)} players from {meta.get('total_drafts', 'unknown')} drafts")
            
            return {
                'meta': meta,
                'players': players,
                'collected_at': datetime.now().isoformat(),
                'scoring': scoring,
                'teams': teams,
                'position': position
            }
            
        except requests.exceptions.RequestException as e:
            print(f"Network error collecting ADP data: {e}")
            return None
        except Exception as e:
            print(f"Error collecting ADP data: {e}")
            return None
            
    def collect_all_scoring_formats(self):
        """Collect ADP data for all scoring formats"""
        scoring_formats = ['standard', 'ppr', 'half-ppr']
        all_data = {}
        
        for scoring in scoring_formats:
            print(f"\nCollecting {scoring.upper()} ADP data...")
            
            # Collect data for all positions
            adp_data = self.collect_adp_data(scoring=scoring, teams=12, position='all')
            
            if adp_data:
                all_data[scoring] = adp_data
                
                # Save individual scoring format file
                filename = f"{self.data_dir}/adp_{scoring}_{self.current_season}.json"
                with open(filename, 'w') as f:
                    json.dump(adp_data, f, indent=2)
                    
                print(f"Saved {scoring} ADP data to {filename}")
            else:
                print(f"Failed to collect {scoring} ADP data")
                
            # Rate limiting - don't call API too frequently
            time.sleep(2)
            
        return all_data
        
    def collect_position_specific_data(self):
        """Collect position-specific ADP data for detailed analysis"""
        positions = ['qb', 'rb', 'wr', 'te', 'k', 'def']
        position_data = {}
        
        for position in positions:
            print(f"\nCollecting {position.upper()} specific ADP data...")
            
            # Get PPR data for each position (most common format)
            adp_data = self.collect_adp_data(scoring='ppr', teams=12, position=position)
            
            if adp_data:
                position_data[position] = adp_data
                
                # Save position-specific file
                filename = f"{self.data_dir}/adp_ppr_{position}_{self.current_season}.json"
                with open(filename, 'w') as f:
                    json.dump(adp_data, f, indent=2)
                    
                print(f"Saved {position.upper()} ADP data to {filename}")
            else:
                print(f"Failed to collect {position.upper()} ADP data")
                
            # Rate limiting
            time.sleep(2)
            
        return position_data
        
    def create_consolidated_adp_database(self):
        """Create unified ADP database for easy analysis"""
        try:
            # Load PPR data as primary source
            ppr_file = f"{self.data_dir}/adp_ppr_{self.current_season}.json"
            
            if not os.path.exists(ppr_file):
                print("PPR ADP data not found - run collection first")
                return False
                
            with open(ppr_file, 'r') as f:
                ppr_data = json.load(f)
                
            # Create consolidated database
            consolidated_db = {
                'meta': {
                    'created_at': datetime.now().isoformat(),
                    'season': self.current_season,
                    'primary_scoring': 'ppr',
                    'total_players': len(ppr_data.get('players', [])),
                    'source_meta': ppr_data.get('meta', {})
                },
                'players': {}
            }
            
            # Process each player
            for player in ppr_data.get('players', []):
                player_id = player.get('player_id')
                if not player_id:
                    continue
                    
                # Create comprehensive player record
                consolidated_db['players'][str(player_id)] = {
                    'name': player.get('name', ''),
                    'position': player.get('position', ''),
                    'team': player.get('team', ''),
                    'adp': {
                        'ppr': {
                            'adp': player.get('adp', 0),
                            'adp_formatted': player.get('adp_formatted', ''),
                            'times_drafted': player.get('times_drafted', 0),
                            'high': player.get('high', 0),
                            'low': player.get('low', 0),
                            'stdev': player.get('stdev', 0)
                        }
                    },
                    'bye_week': player.get('bye', 0),
                    'last_updated': datetime.now().isoformat()
                }
                
            # Add standard and half-PPR data if available
            for scoring in ['standard', 'half-ppr']:
                scoring_file = f"{self.data_dir}/adp_{scoring}_{self.current_season}.json"
                
                if os.path.exists(scoring_file):
                    with open(scoring_file, 'r') as f:
                        scoring_data = json.load(f)
                        
                    for player in scoring_data.get('players', []):
                        player_id = str(player.get('player_id', ''))
                        
                        if player_id in consolidated_db['players']:
                            # Add scoring format data
                            scoring_key = scoring.replace('-', '_')  # half-ppr -> half_ppr
                            consolidated_db['players'][player_id]['adp'][scoring_key] = {
                                'adp': player.get('adp', 0),
                                'adp_formatted': player.get('adp_formatted', ''),
                                'times_drafted': player.get('times_drafted', 0),
                                'high': player.get('high', 0),
                                'low': player.get('low', 0),
                                'stdev': player.get('stdev', 0)
                            }
                            
            # Save consolidated database
            consolidated_file = f"{self.data_dir}/adp_consolidated_{self.current_season}.json"
            with open(consolidated_file, 'w') as f:
                json.dump(consolidated_db, f, indent=2)
                
            print(f"Created consolidated ADP database with {len(consolidated_db['players'])} players")
            return True
            
        except Exception as e:
            print(f"Error creating consolidated ADP database: {e}")
            return False
            
    def update_historical_tracking(self):
        """Update historical ADP tracking for trend analysis"""
        try:
            # Load current ADP data
            consolidated_file = f"{self.data_dir}/adp_consolidated_{self.current_season}.json"
            
            if not os.path.exists(consolidated_file):
                print("No consolidated ADP data found")
                return False
                
            with open(consolidated_file, 'r') as f:
                current_data = json.load(f)
                
            # Load or create historical tracking
            historical_file = f"{self.data_dir}/adp_historical_tracking_{self.current_season}.json"
            
            if os.path.exists(historical_file):
                with open(historical_file, 'r') as f:
                    historical_data = json.load(f)
            else:
                historical_data = {
                    'meta': {
                        'season': self.current_season,
                        'tracking_started': datetime.now().isoformat()
                    },
                    'snapshots': [],
                    'players': {}
                }
                
            # Create snapshot of current data
            snapshot = {
                'date': datetime.now().isoformat(),
                'total_players': len(current_data.get('players', {})),
                'source_meta': current_data.get('meta', {})
            }
            
            historical_data['snapshots'].append(snapshot)
            
            # Update player tracking
            for player_id, player_data in current_data.get('players', {}).items():
                if player_id not in historical_data['players']:
                    historical_data['players'][player_id] = {
                        'name': player_data.get('name', ''),
                        'position': player_data.get('position', ''),
                        'team': player_data.get('team', ''),
                        'adp_history': []
                    }
                    
                # Add current ADP to history
                adp_entry = {
                    'date': datetime.now().isoformat(),
                    'ppr_adp': player_data.get('adp', {}).get('ppr', {}).get('adp', 0),
                    'times_drafted': player_data.get('adp', {}).get('ppr', {}).get('times_drafted', 0)
                }
                
                historical_data['players'][player_id]['adp_history'].append(adp_entry)
                
            # Save updated historical data
            with open(historical_file, 'w') as f:
                json.dump(historical_data, f, indent=2)
                
            print(f"Updated historical ADP tracking for {len(historical_data['players'])} players")
            return True
            
        except Exception as e:
            print(f"Error updating historical tracking: {e}")
            return False

def main():
    """Main execution function"""
    collector = ADPDataCollector()
    
    print("Starting ADP data collection...")
    
    # Collect all scoring formats
    all_data = collector.collect_all_scoring_formats()
    
    if not all_data:
        print("Failed to collect any ADP data")
        return False
        
    # Collect position-specific data for detailed analysis
    position_data = collector.collect_position_specific_data()
    
    # Create consolidated database
    consolidated = collector.create_consolidated_adp_database()
    
    # Update historical tracking
    historical = collector.update_historical_tracking()
    
    print("\nADP data collection completed!")
    print(f"Collected data for {len(all_data)} scoring formats")
    print(f"Position-specific data: {len(position_data)} positions")
    print(f"Consolidated database: {'✓' if consolidated else '✗'}")
    print(f"Historical tracking: {'✓' if historical else '✗'}")
    
    return consolidated and historical

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
