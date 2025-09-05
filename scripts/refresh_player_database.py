#!/usr/bin/env python3
"""
Enhanced Player Database Refresh Script
Updates Sleeper player database and integrates with ADP data collection
"""

import json
import requests
import pandas as pd
from datetime import datetime
import os
import sys

class PlayerDatabaseRefresher:
    def __init__(self):
        self.data_dir = "data"
        self.sleeper_api_base = "https://api.sleeper.app/v1"
        self.players_file = f"{self.data_dir}/players.json"
        self.ensure_directories()
        
    def ensure_directories(self):
        """Create necessary directories"""
        os.makedirs(self.data_dir, exist_ok=True)
        
    def fetch_sleeper_players(self):
        """Fetch complete player database from Sleeper API"""
        try:
            print("Fetching player database from Sleeper API...")
            
            url = f"{self.sleeper_api_base}/players/nfl"
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            
            players_data = response.json()
            
            if not isinstance(players_data, dict):
                print("Invalid response format from Sleeper API")
                return None
                
            print(f"Retrieved {len(players_data)} players from Sleeper")
            return players_data
            
        except requests.exceptions.RequestException as e:
            print(f"Network error fetching Sleeper players: {e}")
            return None
        except Exception as e:
            print(f"Error fetching Sleeper players: {e}")
            return None
            
    def clean_player_data(self, raw_players):
        """Clean and validate player data"""
        try:
            print("Cleaning and validating player data...")
            
            cleaned_players = {}
            fantasy_positions = ['QB', 'RB', 'WR', 'TE', 'K', 'DEF']
            
            for player_id, player_data in raw_players.items():
                if not isinstance(player_data, dict):
                    continue
                    
                # Extract relevant fields
                position = player_data.get('position', '')
                fantasy_pos_list = player_data.get('fantasy_positions', [])
                
                # Skip non-fantasy positions
                if position not in fantasy_positions and not any(pos in fantasy_positions for pos in fantasy_pos_list):
                    continue
                    
                # Clean player record
                cleaned_player = {
                    'player_id': player_id,
                    'first_name': player_data.get('first_name', ''),
                    'last_name': player_data.get('last_name', ''),
                    'full_name': player_data.get('full_name', ''),
                    'position': position,
                    'team': player_data.get('team', ''),
                    'number': player_data.get('number'),
                    'age': player_data.get('age'),
                    'height': player_data.get('height', ''),
                    'weight': player_data.get('weight', ''),
                    'college': player_data.get('college', ''),
                    'years_exp': player_data.get('years_exp'),
                    'status': player_data.get('status', 'Active'),
                    'injury_status': player_data.get('injury_status'),
                    'fantasy_positions': fantasy_pos_list,
                    'search_full_name': player_data.get('search_full_name', ''),
                    'search_first_name': player_data.get('search_first_name', ''),
                    'search_last_name': player_data.get('search_last_name', ''),
                    'espn_id': player_data.get('espn_id'),
                    'yahoo_id': player_data.get('yahoo_id'),
                    'rotowire_id': player_data.get('rotowire_id'),
                    'rotoworld_id': player_data.get('rotoworld_id'),
                    'fantasy_data_id': player_data.get('fantasy_data_id'),
                    'last_updated': datetime.now().isoformat()
                }
                
                # Ensure full_name is populated
                if not cleaned_player['full_name'] and cleaned_player['first_name'] and cleaned_player['last_name']:
                    cleaned_player['full_name'] = f"{cleaned_player['first_name']} {cleaned_player['last_name']}"
                    
                cleaned_players[player_id] = cleaned_player
                
            print(f"Cleaned data for {len(cleaned_players)} fantasy-relevant players")
            return cleaned_players
            
        except Exception as e:
            print(f"Error cleaning player data: {e}")
            return {}
            
    def validate_data_quality(self, players_data):
        """Validate data quality and completeness"""
        try:
            print("Validating data quality...")
            
            validation_results = {
                'total_players': len(players_data),
                'position_counts': {},
                'team_counts': {},
                'missing_data': {
                    'no_name': 0,
                    'no_position': 0,
                    'no_team': 0
                },
                'data_quality_score': 0
            }
            
            # Analyze data completeness
            for player_id, player_data in players_data.items():
                position = player_data.get('position', '')
                team = player_data.get('team', '')
                full_name = player_data.get('full_name', '')
                
                # Count positions
                if position:
                    validation_results['position_counts'][position] = validation_results['position_counts'].get(position, 0) + 1
                else:
                    validation_results['missing_data']['no_position'] += 1
                    
                # Count teams
                if team:
                    validation_results['team_counts'][team] = validation_results['team_counts'].get(team, 0) + 1
                else:
                    validation_results['missing_data']['no_team'] += 1
                    
                # Check name completeness
                if not full_name:
                    validation_results['missing_data']['no_name'] += 1
                    
            # Calculate quality score
            total_players = validation_results['total_players']
            if total_players > 0:
                missing_total = sum(validation_results['missing_data'].values())
                validation_results['data_quality_score'] = round((total_players - missing_total) / total_players * 100, 2)
                
            print(f"Data quality validation completed:")
            print(f"  Total players: {total_players}")
            print(f"  Positions: {len(validation_results['position_counts'])}")
            print(f"  Teams: {len(validation_results['team_counts'])}")
            print(f"  Quality score: {validation_results['data_quality_score']}%")
            
            return validation_results
            
        except Exception as e:
            print(f"Error validating data quality: {e}")
            return {}
            
    def save_player_database(self, players_data, validation_results):
        """Save player database with metadata"""
        try:
            print("Saving player database...")
            
            # Create database structure
            database = {
                'meta': {
                    'last_updated': datetime.now().isoformat(),
                    'total_players': len(players_data),
                    'data_source': 'Sleeper API',
                    'validation_results': validation_results
                },
                'players': players_data
            }
            
            # Save main players file
            with open(self.players_file, 'w') as f:
                json.dump(players_data, f, indent=2)
                
            # Save detailed database file
            detailed_file = f"{self.data_dir}/player_database_detailed.json"
            with open(detailed_file, 'w') as f:
                json.dump(database, f, indent=2)
                
            print(f"Saved player database: {len(players_data)} players")
            print(f"Main file: {self.players_file}")
            print(f"Detailed file: {detailed_file}")
            
            return True
            
        except Exception as e:
            print(f"Error saving player database: {e}")
            return False
            
    def create_filtered_databases(self, players_data):
        """Create filtered databases for specific use cases"""
        try:
            print("Creating filtered player databases...")
            
            # Fantasy-relevant players only (300 most relevant)
            fantasy_relevant = {}
            position_limits = {
                'QB': 40,
                'RB': 80,
                'WR': 100,
                'TE': 30,
                'K': 20,
                'DEF': 32
            }
            
            # Group players by position
            players_by_position = {}
            for player_id, player_data in players_data.items():
                position = player_data.get('position', '')
                if position in position_limits:
                    if position not in players_by_position:
                        players_by_position[position] = []
                    players_by_position[position].append((player_id, player_data))
                    
            # Select top players per position based on status and experience
            for position, players_list in players_by_position.items():
                # Sort by status (Active first) and years of experience
                sorted_players = sorted(players_list, key=lambda x: (
                    x[1].get('status', '') != 'Active',
                    -(x[1].get('years_exp', 0) or 0)
                ))
                
                # Take top players for this position
                limit = position_limits.get(position, 50)
                for player_id, player_data in sorted_players[:limit]:
                    fantasy_relevant[player_id] = player_data
                    
            # Save fantasy-relevant database
            fantasy_file = f"{self.data_dir}/players_fantasy_relevant.json"
            with open(fantasy_file, 'w') as f:
                json.dump(fantasy_relevant, f, indent=2)
                
            print(f"Created fantasy-relevant database: {len(fantasy_relevant)} players")
            
            # Create position-specific files
            for position in position_limits.keys():
                position_players = {
                    pid: pdata for pid, pdata in fantasy_relevant.items() 
                    if pdata.get('position') == position
                }
                
                position_file = f"{self.data_dir}/players_{position.lower()}.json"
                with open(position_file, 'w') as f:
                    json.dump(position_players, f, indent=2)
                    
                print(f"Created {position} database: {len(position_players)} players")
                
            return True
            
        except Exception as e:
            print(f"Error creating filtered databases: {e}")
            return False
            
    def update_player_mapping(self, players_data):
        """Create player ID mapping file for cross-platform integration"""
        try:
            print("Creating player ID mapping...")
            
            mapping = {
                'meta': {
                    'created_at': datetime.now().isoformat(),
                    'total_players': len(players_data),
                    'mapping_purpose': 'Cross-platform player ID integration'
                },
                'mappings': {}
            }
            
            for player_id, player_data in players_data.items():
                # Extract all available IDs
                player_mapping = {
                    'sleeper_id': player_id,
                    'name': player_data.get('full_name', ''),
                    'position': player_data.get('position', ''),
                    'team': player_data.get('team', ''),
                    'external_ids': {}
                }
                
                # Add external IDs if available
                for id_field in ['espn_id', 'yahoo_id', 'rotowire_id', 'rotoworld_id', 'fantasy_data_id']:
                    external_id = player_data.get(id_field)
                    if external_id:
                        player_mapping['external_ids'][id_field] = external_id
                        
                mapping['mappings'][player_id] = player_mapping
                
            # Save mapping file
            mapping_file = f"{self.data_dir}/player_id_mapping.json"
            with open(mapping_file, 'w') as f:
                json.dump(mapping, f, indent=2)
                
            print(f"Created player ID mapping for {len(mapping['mappings'])} players")
            return True
            
        except Exception as e:
            print(f"Error creating player mapping: {e}")
            return False

def main():
    """Main execution function"""
    refresher = PlayerDatabaseRefresher()
    
    print("Starting player database refresh...")
    
    # Fetch latest player data from Sleeper
    raw_players = refresher.fetch_sleeper_players()
    
    if not raw_players:
        print("Failed to fetch player data")
        return False
        
    # Clean and validate data
    cleaned_players = refresher.clean_player_data(raw_players)
    
    if not cleaned_players:
        print("Failed to clean player data")
        return False
        
    # Validate data quality
    validation_results = refresher.validate_data_quality(cleaned_players)
    
    # Save main database
    save_success = refresher.save_player_database(cleaned_players, validation_results)
    
    if not save_success:
        print("Failed to save player database")
        return False
        
    # Create filtered databases
    filtered_success = refresher.create_filtered_databases(cleaned_players)
    
    # Create player mapping
    mapping_success = refresher.update_player_mapping(cleaned_players)
    
    print("\nPlayer database refresh completed!")
    print(f"Main database: {'✓' if save_success else '✗'}")
    print(f"Filtered databases: {'✓' if filtered_success else '✗'}")
    print(f"Player mapping: {'✓' if mapping_success else '✗'}")
    print(f"Data quality score: {validation_results.get('data_quality_score', 0)}%")
    
    return save_success and filtered_success and mapping_success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
