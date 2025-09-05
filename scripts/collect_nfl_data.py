#!/usr/bin/env python3
"""
Enhanced NFL Data Collection Script
Collects weekly performance data and integrates with ADP analysis
"""

import json
import pandas as pd
import nfl_data_py as nfl
from datetime import datetime, timedelta
import os
import sys

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
        # This is a simplified calculation - adjust based on actual season
        season_start = datetime(2025, 9, 4)  # Adjust for actual 2025 season start
        current_date = datetime.now()
        
        if current_date < season_start:
            return 0  # Preseason
        
        days_since_start = (current_date - season_start).days
        week = min((days_since_start // 7) + 1, 18)
        return week
        
    def collect_weekly_stats(self, week=None):
        """Collect weekly player statistics"""
        if week is None:
            week = self.get_current_week()
            
        if week == 0:
            print("Preseason - no weekly stats to collect")
            return None
            
        try:
            print(f"Collecting Week {week} statistics...")
            
            # Get weekly stats for current season
            weekly_stats = nfl.import_weekly_data([self.current_season], include_postseason=False)
            
            # Filter for current week
            current_week_stats = weekly_stats[weekly_stats['week'] == week]
            
            if current_week_stats.empty:
                print(f"No data available for Week {week} yet")
                return None
                
            # Focus on fantasy-relevant stats
            fantasy_columns = [
                'player_id', 'player_name', 'player_display_name', 'position', 
                'team', 'week', 'passing_yards', 'passing_tds', 'interceptions',
                'rushing_yards', 'rushing_tds', 'receiving_yards', 'receiving_tds',
                'receptions', 'targets', 'fantasy_points', 'fantasy_points_ppr'
            ]
            
            # Keep only available columns
            available_columns = [col for col in fantasy_columns if col in current_week_stats.columns]
            filtered_stats = current_week_stats[available_columns]
            
            # Save weekly data
            week_file = f"{self.data_dir}/week_{week}_stats_{self.current_season}.json"
            filtered_stats.to_json(week_file, orient='records', indent=2)
            
            print(f"Saved {len(filtered_stats)} player records for Week {week}")
            return filtered_stats
            
        except Exception as e:
            print(f"Error collecting weekly stats: {e}")
            return None
            
    def collect_season_stats(self):
        """Collect season-to-date statistics"""
        try:
            print("Collecting season-to-date statistics...")
            
            # Get season stats
            season_stats = nfl.import_seasonal_data([self.current_season])
            
            # Focus on fantasy-relevant stats
            fantasy_columns = [
                'player_id', 'player_name', 'player_display_name', 'position',
                'team', 'games', 'passing_yards', 'passing_tds', 'interceptions',
                'rushing_yards', 'rushing_tds', 'receiving_yards', 'receiving_tds',
                'receptions', 'targets', 'fantasy_points', 'fantasy_points_ppr'
            ]
            
            # Keep only available columns
            available_columns = [col for col in fantasy_columns if col in season_stats.columns]
            filtered_stats = season_stats[available_columns]
            
            # Save season data
            season_file = f"{self.data_dir}/season_{self.current_season}_stats.json"
            filtered_stats.to_json(season_file, orient='records', indent=2)
            
            print(f"Saved {len(filtered_stats)} player season records")
            return filtered_stats
            
        except Exception as e:
            print(f"Error collecting season stats: {e}")
            return None
            
    def collect_team_data(self):
        """Collect team information"""
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
            return None
            
    def update_consolidated_data(self):
        """Update consolidated performance database"""
        try:
            # Load existing consolidated data
            consolidated_file = f"{self.data_dir}/season_2025_performances.json"
            
            if os.path.exists(consolidated_file):
                with open(consolidated_file, 'r') as f:
                    consolidated_data = json.load(f)
            else:
                consolidated_data = {}
                
            # Get current week
            current_week = self.get_current_week()
            
            # Add weekly data if available
            week_file = f"{self.data_dir}/week_{current_week}_stats_{self.current_season}.json"
            if os.path.exists(week_file):
                with open(week_file, 'r') as f:
                    week_data = json.load(f)
                    
                # Update consolidated data with weekly performance
                for player in week_data:
                    player_id = player.get('player_id')
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
                        
            # Save updated consolidated data
            with open(consolidated_file, 'w') as f:
                json.dump(consolidated_data, f, indent=2)
                
            print(f"Updated consolidated data with Week {current_week} performances")
            
        except Exception as e:
            print(f"Error updating consolidated data: {e}")

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
    
    # Return success status
    return all([
        weekly_stats is not None or collector.get_current_week() == 0,
        season_stats is not None,
        team_data is not None
    ])

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
