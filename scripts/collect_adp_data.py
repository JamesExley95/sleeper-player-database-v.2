#!/usr/bin/env python3
"""
Fixed NFL Data Collection Script
Handles nfl_data_py API changes and missing 2025 data gracefully
"""

import json
import pandas as pd
import os
import sys
from datetime import datetime, timedelta

# Handle nfl_data_py import with error handling
try:
    import nfl_data_py as nfl
    NFL_DATA_AVAILABLE = True
except ImportError:
    print("Warning: nfl_data_py not available")
    NFL_DATA_AVAILABLE = False

class NFLDataCollector:
    def __init__(self):
        self.data_dir = "data"
        self.current_season = 2025
        self.ensure_directories()
        
    def ensure_directories(self):
        """Create necessary directories"""
        os.makedirs(self.data_dir, exist_ok=True)
        
    def get_current_week(self):
        """Determine current NFL week based on date"""
        # NFL season typically starts first Thursday after Labor Day
        season_start = datetime(2025, 9, 5)  # Adjust for actual 2025 season start
        current_date = datetime.now()
        
        if current_date < season_start:
            return 0  # Preseason
        
        days_since_start = (current_date - season_start).days
        week = min((days_since_start // 7) + 1, 18)
        return week
        
    def collect_weekly_stats(self, week=None):
        """Collect weekly player statistics with error handling"""
        if not NFL_DATA_AVAILABLE:
            print("NFL data library not available - skipping weekly stats")
            return None
            
        if week is None:
            week = self.get_current_week()
            
        if week == 0:
            print("Preseason - no weekly stats to collect")
            return None
            
        try:
            print(f"Collecting Week {week} statistics...")
            
            # Try current season first, fall back to previous season for testing
            try:
                weekly_stats = nfl.import_weekly_data([self.current_season])
                print(f"Successfully loaded {self.current_season} weekly data")
            except Exception as e:
                print(f"Error loading {self.current_season} data: {e}")
                print("Falling back to 2024 data for testing...")
                weekly_stats = nfl.import_weekly_data([2024])
                
            if weekly_stats.empty:
                print(f"No weekly data available yet")
                return None
                
            # Filter for current week if available
            if 'week' in weekly_stats.columns:
                current_week_stats = weekly_stats[weekly_stats['week'] == week]
                if current_week_stats.empty:
                    print(f"No data available for Week {week} yet")
                    return None
            else:
                current_week_stats = weekly_stats
                
            # Focus on fantasy-relevant stats
            fantasy_columns = [
                'player_id', 'player_name', 'player_display_name', 'position', 
                'team', 'week', 'passing_yards', 'passing_tds', 'interceptions',
                'rushing_yards', 'rushing_tds', 'receiving_yards', 'receiving_tds',
                'receptions', 'targets', 'fantasy_points', 'fantasy_points_ppr'
            ]
            
            # Keep only available columns
            available_columns = [col for col in fantasy_columns if col in current_week_stats.columns]
            if available_columns:
                filtered_stats = current_week_stats[available_columns]
            else:
                # Fallback with basic columns
                basic_columns = ['player_name', 'position', 'team']
                available_basic = [col for col in basic_columns if col in current_week_stats.columns]
                filtered_stats = current_week_stats[available_basic] if available_basic else current_week_stats
            
            # Save weekly data
            week_file = f"{self.data_dir}/week_{week}_stats_{self.current_season}.json"
            filtered_stats.to_json(week_file, orient='records', indent=2)
            
            print(f"Saved {len(filtered_stats)} player records for Week {week}")
            return filtered_stats
            
        except Exception as e:
            print(f"Error collecting weekly stats: {e}")
            return None
            
    def collect_season_stats(self):
        """Collect season-to-date statistics with error handling"""
        if not NFL_DATA_AVAILABLE:
            print("NFL data library not available - skipping season stats")
            return None
            
        try:
            print("Collecting season-to-date statistics...")
            
            # Try current season first, fall back to previous season
            try:
                season_stats = nfl.import_seasonal_data([self.current_season])
                print(f"Successfully loaded {self.current_season} season data")
            except Exception as e:
                print(f"Error loading {self.current_season} season data: {e}")
                print("Falling back to 2024 season data...")
                season_stats = nfl.import_seasonal_data([2024])
                
            if season_stats.empty:
                print("No season data available yet")
                return None
            
            # Focus on fantasy-relevant stats
            fantasy_columns = [
                'player_id', 'player_name', 'player_display_name', 'position',
                'team', 'games', 'passing_yards', 'passing_tds', 'interceptions',
                'rushing_yards', 'rushing_tds', 'receiving_yards', 'receiving_tds',
                'receptions', 'targets', 'fantasy_points', 'fantasy_points_ppr'
            ]
            
            # Keep only available columns
            available_columns = [col for col in fantasy_columns if col in season_stats.columns]
            if available_columns:
                filtered_stats = season_stats[available_columns]
            else:
                # Fallback with basic columns
                basic_columns = ['player_name', 'position', 'team']
                available_basic = [col for col in basic_columns if col in season_stats.columns]
                filtered_stats = season_stats[available_basic] if available_basic else season_stats
            
            # Save season data
            season_file = f"{self.data_dir}/season_{self.current_season}_stats.json"
            filtered_stats.to_json(season_file, orient='records', indent=2)
            
            print(f"Saved {len(filtered_stats)} player season records")
            return filtered_stats
            
        except Exception as e:
            print(f"Error collecting season stats: {e}")
            return None
            
    def collect_team_data(self):
        """Collect team information with error handling"""
        if not NFL_DATA_AVAILABLE:
            print("NFL data library not available - creating basic team data")
            # Create basic team data as fallback
            basic_teams = [
                {"team_abbr": "ARI", "team_name": "Arizona Cardinals"},
                {"team_abbr": "ATL", "team_name": "Atlanta Falcons"},
                {"team_abbr": "BAL", "team_name": "Baltimore Ravens"},
                {"team_abbr": "BUF", "team_name": "Buffalo Bills"},
                {"team_abbr": "CAR", "team_name": "Carolina Panthers"},
                {"team_abbr": "CHI", "team_name": "Chicago Bears"},
                {"team_abbr": "CIN", "team_name": "Cincinnati Bengals"},
                {"team_abbr": "CLE", "team_name": "Cleveland Browns"},
                {"team_abbr": "DAL", "team_name": "Dallas Cowboys"},
                {"team_abbr": "DEN", "team_name": "Denver Broncos"},
                {"team_abbr": "DET", "team_name": "Detroit Lions"},
                {"team_abbr": "GB", "team_name": "Green Bay Packers"},
                {"team_abbr": "HOU", "team_name": "Houston Texans"},
                {"team_abbr": "IND", "team_name": "Indianapolis Colts"},
                {"team_abbr": "JAX", "team_name": "Jacksonville Jaguars"},
                {"team_abbr": "KC", "team_name": "Kansas City Chiefs"},
                {"team_abbr": "LV", "team_name": "Las Vegas Raiders"},
                {"team_abbr": "LAC", "team_name": "Los Angeles Chargers"},
                {"team_abbr": "LAR", "team_name": "Los Angeles Rams"},
                {"team_abbr": "MIA", "team_name": "Miami Dolphins"},
                {"team_abbr": "MIN", "team_name": "Minnesota Vikings"},
                {"team_abbr": "NE", "team_name": "New England Patriots"},
                {"team_abbr": "NO", "team_name": "New Orleans Saints"},
                {"team_abbr": "NYG", "team_name": "New York Giants"},
                {"team_abbr": "NYJ", "team_name": "New York Jets"},
                {"team_abbr": "PHI", "team_name": "Philadelphia Eagles"},
                {"team_abbr": "PIT", "team_name": "Pittsburgh Steelers"},
                {"team_abbr": "SEA", "team_name": "Seattle Seahawks"},
                {"team_abbr": "SF", "team_name": "San Francisco 49ers"},
                {"team_abbr": "TB", "team_name": "Tampa Bay Buccaneers"},
                {"team_abbr": "TEN", "team_name": "Tennessee Titans"},
                {"team_abbr": "WAS", "team_name": "Washington Commanders"}
            ]
            
            teams_df = pd.DataFrame(basic_teams)
            team_file = f"{self.data_dir}/teams_{self.current_season}.json"
            teams_df.to_json(team_file, orient='records', indent=2)
            print(f"Saved {len(basic_teams)} team records (basic data)")
            return teams_df
            
        try:
            print("Collecting team data...")
            
            teams = nfl.import_team_desc()
            
            # Save team data
            team_file = f"{self.data_dir}/teams_{self.current_season}.json"
            teams.to_json(team_file, orient='records', indent=2)
            
            print(f"Saved {len(teams)} team records")
            return teams
            
        except Exception as e:
            print(f"Error collecting team data: {e}")
            # Fall back to basic team data
            return self.collect_team_data()
            
    def update_consolidated_data(self):
        """Update consolidated performance database with error handling"""
        try:
            # Load existing consolidated data
            consolidated_file = f"{self.data_dir}/season_{self.current_season}_performances.json"
            
            if os.path.exists(consolidated_file):
                try:
                    with open(consolidated_file, 'r') as f:
                        content = f.read().strip()
                        if content:
                            consolidated_data = json.loads(content)
                        else:
                            consolidated_data = {}
                except (json.JSONDecodeError, ValueError):
                    print("Invalid JSON in consolidated file, starting fresh")
                    consolidated_data = {}
            else:
                consolidated_data = {}
                
            # Get current week
            current_week = self.get_current_week()
            
            if current_week == 0:
                print("Preseason - no performance data to consolidate")
                # Ensure file exists with empty structure
                with open(consolidated_file, 'w') as f:
                    json.dump({}, f, indent=2)
                return
                
            # Add weekly data if available
            week_file = f"{self.data_dir}/week_{current_week}_stats_{self.current_season}.json"
            if os.path.exists(week_file):
                try:
                    with open(week_file, 'r') as f:
                        week_data = json.load(f)
                        
                    # Update consolidated data with weekly performance
                    for player in week_data:
                        player_id = player.get('player_id') or player.get('player_name', '').replace(' ', '_').lower()
                        
                        if player_id:
                            if player_id not in consolidated_data:
                                consolidated_data[player_id] = {
                                    'player_name': player.get('player_name', ''),
                                    'position': player.get('position', ''),
                                    'team': player.get('team', ''),
                                    'weekly_performances': {}
                                }
                            
                            # Add this week's performance
                            consolidated_data[player_id]['weekly_performances'][str(current_week)] = {
                                'fantasy_points': player.get('fantasy_points', 0),
                                'fantasy_points_ppr': player.get('fantasy_points_ppr', 0),
                                'passing_yards': player.get('passing_yards', 0),
                                'rushing_yards': player.get('rushing_yards', 0),
                                'receiving_yards': player.get('receiving_yards', 0),
                                'total_tds': (player.get('passing_tds', 0) + 
                                            player.get('rushing_tds', 0) + 
                                            player.get('receiving_tds', 0))
                            }
                except json.JSONDecodeError:
                    print(f"Error reading week {current_week} data file")
                        
            # Save updated consolidated data
            with open(consolidated_file, 'w') as f:
                json.dump(consolidated_data, f, indent=2)
                
            print(f"Updated consolidated data with Week {current_week} performances")
            
        except Exception as e:
            print(f"Error updating consolidated data: {e}")
            # Ensure file exists with basic structure
            consolidated_file = f"{self.data_dir}/season_{self.current_season}_performances.json"
            with open(consolidated_file, 'w') as f:
                json.dump({}, f, indent=2)

def main():
    """Main execution function"""
    collector = NFLDataCollector()
    
    print("Starting NFL data collection...")
    
    # Collect all data types
    weekly_stats = collector.collect_weekly_stats()
    season_stats = collector.collect_season_stats()
    team_data = collector.collect_team_data()
    
    # Update consolidated database
    collector.update_consolidated_data()
    
    print("NFL data collection completed!")
    
    # Return success if at least team data worked
    return team_data is not None

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
