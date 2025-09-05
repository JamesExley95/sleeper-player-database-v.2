#!/usr/bin/env python3
"""
Fixed Weekly Performance Tracker
Made resilient to missing dependencies and data structure issues
"""

import json
import pandas as pd
from datetime import datetime, timedelta
import os
import sys
import statistics

class WeeklyPerformanceTracker:
    def __init__(self):
        self.data_dir = "data"
        self.current_season = 2025
        self.performance_file = f"{self.data_dir}/season_{self.current_season}_performances.json"
        self.weekly_snapshots_file = f"{self.data_dir}/weekly_snapshots.json"
        
    def get_current_week(self):
        """Determine current NFL week"""
        season_start = datetime(2025, 9, 5)
        current_date = datetime.now()
        
        if current_date < season_start:
            return 0
            
        days_since_start = (current_date - season_start).days
        week = min((days_since_start // 7) + 1, 18)
        return week
        
    def load_weekly_stats(self, week):
        """Load weekly NFL statistics"""
        try:
            week_file = f"{self.data_dir}/week_{week}_stats_{self.current_season}.json"
            
            if not os.path.exists(week_file):
                print(f"Week {week} stats file not found")
                return []
                
            with open(week_file, 'r') as f:
                content = f.read().strip()
                if not content:
                    print(f"Week {week} stats file is empty")
                    return []
                    
                week_stats = json.loads(content)
                
            print(f"Loaded {len(week_stats)} player performances for Week {week}")
            return week_stats
            
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error parsing Week {week} stats: {e}")
            return []
        except Exception as e:
            print(f"Error loading Week {week} stats: {e}")
            return []
            
    def load_existing_performance_data(self):
        """Load existing performance tracking data"""
        try:
            if os.path.exists(self.performance_file):
                with open(self.performance_file, 'r') as f:
                    content = f.read().strip()
                    if content:
                        performance_data = json.loads(content)
                    else:
                        performance_data = {}
                        
                print(f"Loaded existing performance data for {len(performance_data)} players")
                return performance_data
            else:
                print("No existing performance data found - starting fresh")
                return {}
                
        except (json.JSONDecodeError, ValueError):
            print("Corrupted performance data file - starting fresh")
            return {}
        except Exception as e:
            print(f"Error loading existing performance data: {e}")
            return {}
            
    def load_draft_database(self):
        """Load draft database for ADP integration with error handling"""
        try:
            draft_file = f"{self.data_dir}/draft_database_{self.current_season}.json"
            
            if not os.path.exists(draft_file):
                print("Draft database not found - will proceed without ADP integration")
                return {}
                
            with open(draft_file, 'r') as f:
                draft_data = json.load(f)
                
            players = draft_data.get('players', {})
            print(f"Loaded draft database with {len(players)} players")
            return players
            
        except Exception as e:
            print(f"Error loading draft database: {e}")
            return {}
            
    def safe_get_nested(self, data, keys, default=None):
        """Safely get nested dictionary values"""
        try:
            for key in keys:
                data = data.get(key, {})
            return data if data != {} else default
        except (AttributeError, TypeError):
            return default
            
    def initialize_player_data(self, player_id, player_name, position, team):
        """Initialize player data structure"""
        return {
            'player_name': player_name,
            'position': position or 'Unknown',
            'team': team or 'Unknown',
            'weekly_performances': {},
            'season_totals': {
                'games_played': 0,
                'total_fantasy_points': 0,
                'total_fantasy_points_ppr': 0,
                'total_passing_yards': 0,
                'total_rushing_yards': 0,
                'total_receiving_yards': 0,
                'total_tds': 0,
                'best_week': 0,
                'worst_week': 999,
                'consistency_score': 0
            },
            'adp_integration': {},
            'last_updated': datetime.now().isoformat()
        }
        
    def update_performance_tracking(self, week=None):
        """Update performance tracking with latest weekly data"""
        try:
            if week is None:
                week = self.get_current_week()
                
            if week == 0:
                print("No performance data to track in preseason")
                # Ensure performance file exists
                with open(self.performance_file, 'w') as f:
                    json.dump({}, f, indent=2)
                return True
                
            print(f"Updating performance tracking for Week {week}...")
            
            # Load data sources
            week_stats = self.load_weekly_stats(week)
            existing_data = self.load_existing_performance_data()
            draft_db = self.load_draft_database()
            
            if not week_stats:
                print(f"No Week {week} stats available")
                # Still ensure file exists
                with open(self.performance_file, 'w') as f:
                    json.dump(existing_data, f, indent=2)
                return True
                
            # Process weekly performances
            for player_stat in week_stats:
                player_id = player_stat.get('player_id') or str(hash(player_stat.get('player_name', '')))
                player_name = player_stat.get('player_name', player_stat.get('player_display_name', ''))
                position = player_stat.get('position', '')
                team = player_stat.get('team', '')
                
                if not player_name:
                    continue
                    
                # Initialize player tracking if new
                if player_id not in existing_data:
                    existing_data[player_id] = self.initialize_player_data(player_id, player_name, position, team)
                    
                # Ensure season_totals exists (fix for the error)
                if 'season_totals' not in existing_data[player_id]:
                    existing_data[player_id]['season_totals'] = {
                        'games_played': 0,
                        'total_fantasy_points': 0,
                        'total_fantasy_points_ppr': 0,
                        'total_passing_yards': 0,
                        'total_rushing_yards': 0,
                        'total_receiving_yards': 0,
                        'total_tds': 0,
                        'best_week': 0,
                        'worst_week': 999,
                        'consistency_score': 0
                    }
                    
                # Add ADP data if available
                if draft_db:
                    draft_player = None
                    for did, ddata in draft_db.items():
                        if (ddata.get('sleeper_id') == player_id or 
                            (ddata.get('name', '').lower() == player_name.lower() and 
                             ddata.get('team', '').upper() == team.upper())):
                            draft_player = ddata
                            break
                            
                    if draft_player:
                        existing_data[player_id]['adp_integration'] = {
                            'adp': draft_player.get('adp', 999),
                            'adp_stdev': draft_player.get('adp_stdev', 0),
                            'times_drafted': draft_player.get('times_drafted', 0),
                            'expectations': self.calculate_expectations(draft_player.get('adp', 999))
                        }
                        
                # Add weekly performance
                week_performance = {
                    'week': week,
                    'fantasy_points': player_stat.get('fantasy_points', 0),
                    'fantasy_points_ppr': player_stat.get('fantasy_points_ppr', 0),
                    'passing_yards': player_stat.get('passing_yards', 0),
                    'passing_tds': player_stat.get('passing_tds', 0),
                    'interceptions': player_stat.get('interceptions', 0),
                    'rushing_yards': player_stat.get('rushing_yards', 0),
                    'rushing_tds': player_stat.get('rushing_tds', 0),
                    'receiving_yards': player_stat.get('receiving_yards', 0),
                    'receiving_tds': player_stat.get('receiving_tds', 0),
                    'receptions': player_stat.get('receptions', 0),
                    'targets': player_stat.get('targets', 0),
                    'total_tds': (player_stat.get('passing_tds', 0) + 
                                player_stat.get('rushing_tds', 0) + 
                                player_stat.get('receiving_tds', 0))
                }
                
                existing_data[player_id]['weekly_performances'][str(week)] = week_performance
                
                # Update season totals
                self.update_season_totals(existing_data[player_id], week_performance)
                
                # Update last modified
                existing_data[player_id]['last_updated'] = datetime.now().isoformat()
                
            # Calculate advanced metrics
            self.calculate_advanced_metrics(existing_data, week)
            
            # Save updated performance data
            with open(self.performance_file, 'w') as f:
                json.dump(existing_data, f, indent=2)
                
            print(f"Updated performance tracking for {len(week_stats)} players")
            return True
            
        except Exception as e:
            print(f"Error updating performance tracking: {e}")
            # Ensure file exists even on error
            try:
                with open(self.performance_file, 'w') as f:
                    json.dump({}, f, indent=2)
            except:
                pass
            return False
            
    def calculate_expectations(self, adp):
        """Calculate performance expectations based on ADP"""
        if adp <= 12:
            return "Elite weekly performance (15+ points)"
        elif adp <= 24:
            return "High weekly performance (12+ points)"
        elif adp <= 60:
            return "Solid weekly performance (8+ points)"
        elif adp <= 120:
            return "Flex-worthy performance (5+ points)"
        else:
            return "Waiver wire/bench production"
            
    def update_season_totals(self, player_data, week_performance):
        """Update season totals with new weekly performance"""
        season_totals = player_data.get('season_totals', {})
        
        # Increment games played
        season_totals['games_played'] = season_totals.get('games_played', 0) + 1
        
        # Add to totals
        fantasy_points = week_performance.get('fantasy_points_ppr', 0)
        season_totals['total_fantasy_points'] = season_totals.get('total_fantasy_points', 0) + week_performance.get('fantasy_points', 0)
        season_totals['total_fantasy_points_ppr'] = season_totals.get('total_fantasy_points_ppr', 0) + fantasy_points
        season_totals['total_passing_yards'] = season_totals.get('total_passing_yards', 0) + week_performance.get('passing_yards', 0)
        season_totals['total_rushing_yards'] = season_totals.get('total_rushing_yards', 0) + week_performance.get('rushing_yards', 0)
        season_totals['total_receiving_yards'] = season_totals.get('total_receiving_yards', 0) + week_performance.get('receiving_yards', 0)
        season_totals['total_tds'] = season_totals.get('total_tds', 0) + week_performance.get('total_tds', 0)
        
        # Update best/worst weeks
        season_totals['best_week'] = max(season_totals.get('best_week', 0), fantasy_points)
        if fantasy_points > 0:
            season_totals['worst_week'] = min(season_totals.get('worst_week', 999), fantasy_points)
            
    def calculate_advanced_metrics(self, performance_data, current_week):
        """Calculate advanced performance metrics"""
        try:
            print("Calculating advanced performance metrics...")
            
            for player_id, player_data in performance_data.items():
                weekly_perfs = player_data.get('weekly_performances', {})
                
                if not weekly_perfs:
                    continue
                    
                # Get PPR points for all weeks
                ppr_points = []
                for week_str, week_data in weekly_perfs.items():
                    points = week_data.get('fantasy_points_ppr', 0)
                    if points > 0:
                        ppr_points.append(points)
                        
                if len(ppr_points) < 1:
                    continue
                    
                # Calculate consistency metrics
                avg_points = statistics.mean(ppr_points)
                std_dev = statistics.stdev(ppr_points) if len(ppr_points) > 1 else 0
                consistency_score = max(0, 100 - (std_dev / avg_points * 100)) if avg_points > 0 else 0
                
                # Update advanced metrics
                player_data['advanced_metrics'] = {
                    'avg_ppr_points': round(avg_points, 2),
                    'consistency_score': round(consistency_score, 1),
                    'standard_deviation': round(std_dev, 2),
                    'weeks_played': len(ppr_points),
                    'ceiling': max(ppr_points) if ppr_points else 0,
                    'floor': min(ppr_points) if ppr_points else 0
                }
                
        except Exception as e:
            print(f"Error calculating advanced metrics: {e}")
            
    def create_weekly_snapshot(self, week=None):
        """Create snapshot of weekly performance for historical tracking"""
        try:
            if week is None:
                week = self.get_current_week()
                
            if week == 0:
                print("Preseason - no snapshot to create")
                return True
                
            print(f"Creating Week {week} performance snapshot...")
            
            # Load current performance data
            performance_data = self.load_existing_performance_data()
            
            if not performance_data:
                print("No performance data to snapshot")
                return True
                
            # Load existing snapshots
            if os.path.exists(self.weekly_snapshots_file):
                try:
                    with open(self.weekly_snapshots_file, 'r') as f:
                        content = f.read().strip()
                        if content:
                            snapshots_data = json.loads(content)
                        else:
                            snapshots_data = {'meta': {'season': self.current_season}, 'weekly_snapshots': {}}
                except:
                    snapshots_data = {'meta': {'season': self.current_season}, 'weekly_snapshots': {}}
            else:
                snapshots_data = {'meta': {'season': self.current_season}, 'weekly_snapshots': {}}
                
            # Create snapshot for this week
            week_snapshot = {
                'week': week,
                'snapshot_date': datetime.now().isoformat(),
                'players': {}
            }
            
            # Process each player's performance
            for player_id, player_data in performance_data.items():
                weekly_perfs = player_data.get('weekly_performances', {})
                
                if str(week) in weekly_perfs:
                    week_performance = weekly_perfs[str(week)]
                    
                    week_snapshot['players'][player_id] = {
                        'name': player_data.get('player_name', ''),
                        'position': player_data.get('position', ''),
                        'team': player_data.get('team', ''),
                        'week_points': week_performance.get('fantasy_points_ppr', 0)
                    }
                    
            # Save snapshot
            snapshots_data['weekly_snapshots'][str(week)] = week_snapshot
            
            with open(self.weekly_snapshots_file, 'w') as f:
                json.dump(snapshots_data, f, indent=2)
                
            print(f"Created Week {week} snapshot with {len(week_snapshot['players'])} players")
            return True
            
        except Exception as e:
            print(f"Error creating weekly snapshot: {e}")
            return False

def main():
    """Main execution function"""
    tracker = WeeklyPerformanceTracker()
    
    print("Starting weekly performance tracking...")
    
    current_week = tracker.get_current_week()
    
    # Update performance tracking
    tracking_success = tracker.update_performance_tracking(current_week)
    
    # Create weekly snapshot
    snapshot_success = tracker.create_weekly_snapshot(current_week)
    
    print(f"\nWeekly performance tracking completed!")
    print(f"Performance tracking: {'✓' if tracking_success else '✗'}")
    print(f"Weekly snapshot: {'✓' if snapshot_success else '✗'}")
    
    return tracking_success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
