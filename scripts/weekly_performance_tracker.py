#!/usr/bin/env python3
"""
Enhanced Weekly Performance Tracker
Tracks player performance and integrates with ADP analysis for storytelling
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
        # Simplified week calculation - adjust based on actual season
        season_start = datetime(2025, 9, 4)
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
                week_stats = json.load(f)
                
            print(f"Loaded {len(week_stats)} player performances for Week {week}")
            return week_stats
            
        except Exception as e:
            print(f"Error loading Week {week} stats: {e}")
            return []
            
    def load_existing_performance_data(self):
        """Load existing performance tracking data"""
        try:
            if os.path.exists(self.performance_file):
                with open(self.performance_file, 'r') as f:
                    performance_data = json.load(f)
                    
                print(f"Loaded existing performance data for {len(performance_data)} players")
                return performance_data
            else:
                print("No existing performance data found - starting fresh")
                return {}
                
        except Exception as e:
            print(f"Error loading existing performance data: {e}")
            return {}
            
    def load_draft_database(self):
        """Load draft database for ADP integration"""
        try:
            draft_file = f"{self.data_dir}/draft_database_{self.current_season}.json"
            
            if not os.path.exists(draft_file):
                print("Draft database not found")
                return {}
                
            with open(draft_file, 'r') as f:
                draft_data = json.load(f)
                
            return draft_data.get('players', {})
            
        except Exception as e:
            print(f"Error loading draft database: {e}")
            return {}
            
    def update_performance_tracking(self, week=None):
        """Update performance tracking with latest weekly data"""
        try:
            if week is None:
                week = self.get_current_week()
                
            if week == 0:
                print("No performance data to track in preseason")
                return True
                
            print(f"Updating performance tracking for Week {week}...")
            
            # Load data sources
            week_stats = self.load_weekly_stats(week)
            existing_data = self.load_existing_performance_data()
            draft_db = self.load_draft_database()
            
            if not week_stats:
                print(f"No Week {week} stats available")
                return False
                
            # Process weekly performances
            for player_stat in week_stats:
                player_id = player_stat.get('player_id')
                player_name = player_stat.get('player_name', player_stat.get('player_display_name', ''))
                
                if not player_id or not player_name:
                    continue
                    
                # Initialize player tracking if new
                if player_id not in existing_data:
                    existing_data[player_id] = {
                        'player_name': player_name,
                        'position': player_stat.get('position', ''),
                        'team': player_stat.get('team', ''),
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
                    
                # Add ADP data if available
                draft_player = None
                for did, ddata in draft_db.items():
                    if (ddata.get('sleeper_id') == player_id or 
                        ddata.get('name', '').lower() == player_name.lower()):
                        draft_player = ddata
                        break
                        
                if draft_player:
                    adp_data = draft_player.get('adp_data', {}).get('ppr', {})
                    existing_data[player_id]['adp_integration'] = {
                        'adp': adp_data.get('adp', 999),
                        'draft_round': draft_player.get('draft_analysis', {}).get('draft_round', 0),
                        'volatility_tier': draft_player.get('draft_analysis', {}).get('volatility_tier', ''),
                        'expectations': self.calculate_expectations(adp_data.get('adp', 999))
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
            return False
            
    def calculate_expectations(self, adp):
        """Calculate performance expectations based on ADP"""
        if adp <= 12:  # Round 1
            return "Elite weekly performance (15+ points)"
        elif adp <= 24:  # Round 2
            return "High weekly performance (12+ points)"
        elif adp <= 60:  # Rounds 3-5
            return "Solid weekly performance (8+ points)"
        elif adp <= 120:  # Rounds 6-10
            return "Flex-worthy performance (5+ points)"
        else:
            return "Waiver wire/bench production"
            
    def update_season_totals(self, player_data, week_performance):
        """Update season totals with new weekly performance"""
        season_totals = player_data['season_totals']
        
        # Increment games played
        season_totals['games_played'] += 1
        
        # Add to totals
        fantasy_points = week_performance.get('fantasy_points_ppr', 0)
        season_totals['total_fantasy_points'] += week_performance.get('fantasy_points', 0)
        season_totals['total_fantasy_points_ppr'] += fantasy_points
        season_totals['total_passing_yards'] += week_performance.get('passing_yards', 0)
        season_totals['total_rushing_yards'] += week_performance.get('rushing_yards', 0)
        season_totals['total_receiving_yards'] += week_performance.get('receiving_yards', 0)
        season_totals['total_tds'] += week_performance.get('total_tds', 0)
        
        # Update best/worst weeks
        season_totals['best_week'] = max(season_totals['best_week'], fantasy_points)
        if fantasy_points > 0:  # Only count weeks where player actually played
            season_totals['worst_week'] = min(season_totals['worst_week'], fantasy_points)
            
    def calculate_advanced_metrics(self, performance_data, current_week):
        """Calculate advanced performance metrics"""
        try:
            print("Calculating advanced performance metrics...")
            
            for player_id, player_data in performance_data.items():
                weekly_perfs = player_data.get('weekly_performances', {})
                season_totals = player_data.get('season_totals', {})
                adp_integration = player_data.get('adp_integration', {})
                
                if not weekly_perfs:
                    continue
                    
                # Get PPR points for all weeks
                ppr_points = []
                for week_str, week_data in weekly_perfs.items():
                    points = week_data.get('fantasy_points_ppr', 0)
                    if points > 0:  # Only count weeks where player played
                        ppr_points.append(points)
                        
                if len(ppr_points) < 2:
                    continue
                    
                # Calculate consistency metrics
                avg_points = statistics.mean(ppr_points)
                std_dev = statistics.stdev(ppr_points) if len(ppr_points) > 1 else 0
                consistency_score = max(0, 100 - (std_dev / avg_points * 100)) if avg_points > 0 else 0
                
                # Calculate trend (last 3 weeks vs first 3 weeks)
                recent_weeks = ppr_points[-3:] if len(ppr_points) >= 3 else ppr_points
                early_weeks = ppr_points[:3] if len(ppr_points) >= 3 else ppr_points
                
                trend_direction = "stable"
                if len(recent_weeks) >= 2 and len(early_weeks) >= 2:
                    recent_avg = statistics.mean(recent_weeks)
                    early_avg = statistics.mean(early_weeks)
                    
                    if recent_avg > early_avg * 1.2:
                        trend_direction = "trending_up"
                    elif recent_avg < early_avg * 0.8:
                        trend_direction = "trending_down"
                        
                # Calculate ADP vs performance analysis
                adp_performance_grade = self.calculate_adp_performance_grade(
                    adp_integration.get('adp', 999),
                    avg_points,
                    current_week
                )
                
                # Update advanced metrics
                player_data['advanced_metrics'] = {
                    'avg_ppr_points': round(avg_points, 2),
                    'consistency_score': round(consistency_score, 1),
                    'standard_deviation': round(std_dev, 2),
                    'boom_weeks': len([p for p in ppr_points if p >= 20]),
                    'bust_weeks': len([p for p in ppr_points if p < 5]),
                    'trend_direction': trend_direction,
                    'adp_performance_grade': adp_performance_grade,
                    'weeks_played': len(ppr_points),
                    'ceiling': max(ppr_points) if ppr_points else 0,
                    'floor': min(ppr_points) if ppr_points else 0
                }
                
        except Exception as e:
            print(f"Error calculating advanced metrics: {e}")
            
    def calculate_adp_performance_grade(self, adp, avg_points, weeks_played):
        """Calculate how player is performing vs ADP expectations"""
        if adp > 200 or weeks_played < 2:
            return "N/A"
            
        # Expected points per game based on ADP
        if adp <= 12:  # Round 1
            expected_ppg = 15
        elif adp <= 24:  # Round 2
            expected_ppg = 12
        elif adp <= 60:  # Rounds 3-5
            expected_ppg = 8
        elif adp <= 120:  # Rounds 6-10
            expected_ppg = 5
        else:
            expected_ppg = 3
            
        # Calculate performance ratio
        performance_ratio = avg_points / expected_ppg if expected_ppg > 0 else 0
        
        # Assign grade
        if performance_ratio >= 1.3:
            return "A+ (Exceeding expectations)"
        elif performance_ratio >= 1.1:
            return "A (Meeting/exceeding expectations)"
        elif performance_ratio >= 0.9:
            return "B (Solid value)"
        elif performance_ratio >= 0.7:
            return "C (Below expectations)"
        elif performance_ratio >= 0.5:
            return "D (Disappointing)"
        else:
            return "F (Major bust)"
            
    def create_weekly_snapshot(self, week=None):
        """Create snapshot of weekly performance for historical tracking"""
        try:
            if week is None:
                week = self.get_current_week()
                
            print(f"Creating Week {week} performance snapshot...")
            
            # Load current performance data
            performance_data = self.load_existing_performance_data()
            
            if not performance_data:
                return False
                
            # Load existing snapshots
            if os.path.exists(self.weekly_snapshots_file):
                with open(self.weekly_snapshots_file, 'r') as f:
                    snapshots_data = json.load(f)
            else:
                snapshots_data = {
                    'meta': {
                        'season': self.current_season,
                        'created_at': datetime.now().isoformat()
                    },
                    'weekly_snapshots': {}
                }
                
            # Create snapshot for this week
            week_snapshot = {
                'week': week,
                'snapshot_date': datetime.now().isoformat(),
                'players': {}
            }
            
            # Process each player's performance
            for player_id, player_data in performance_data.items():
                weekly_perfs = player_data.get('weekly_performances', {})
                advanced_metrics = player_data.get('advanced_metrics', {})
                
                if str(week) in weekly_perfs:
                    week_performance = weekly_perfs[str(week)]
                    
                    week_snapshot['players'][player_id] = {
                        'name': player_data.get('player_name', ''),
                        'position': player_data.get('position', ''),
                        'team': player_data.get('team', ''),
                        'week_points': week_performance.get('fantasy_points_ppr', 0),
                        'season_avg': advanced_metrics.get('avg_ppr_points', 0),
                        'consistency_score': advanced_metrics.get('consistency_score', 0),
                        'adp_grade': advanced_metrics.get('adp_performance_grade', 'N/A'),
                        'trend': advanced_metrics.get('trend_direction', 'stable')
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
            
    def generate_performance_insights(self):
        """Generate performance insights for content creation"""
        try:
            print("Generating performance insights...")
            
            performance_data = self.load_existing_performance_data()
            
            if not performance_data:
                return None
                
            current_week = self.get_current_week()
            
            insights = {
                'generated_at': datetime.now().isoformat(),
                'current_week': current_week,
                'top_performers': [],
                'biggest_disappointments': [],
                'most_consistent': [],
                'trending_up': [],
                'trending_down': [],
                'adp_steals': [],
                'adp_busts': []
            }
            
            # Analyze all players
            for player_id, player_data in performance_data.items():
                advanced_metrics = player_data.get('advanced_metrics', {})
                adp_integration = player_data.get('adp_integration', {})
                
                if not advanced_metrics or advanced_metrics.get('weeks_played', 0) < 2:
                    continue
                    
                player_summary = {
                    'player_id': player_id,
                    'name': player_data.get('player_name', ''),
                    'position': player_data.get('position', ''),
                    'team': player_data.get('team', ''),
                    'avg_points': advanced_metrics.get('avg_ppr_points', 0),
                    'consistency': advanced_metrics.get('consistency_score', 0),
                    'adp': adp_integration.get('adp', 999),
                    'adp_grade': advanced_metrics.get('adp_performance_grade', 'N/A'),
                    'trend': advanced_metrics.get('trend_direction', 'stable'),
                    'weeks_played': advanced_metrics.get('weeks_played', 0)
                }
                
                # Categorize players
                avg_points = advanced_metrics.get('avg_ppr_points', 0)
                adp = adp_integration.get('adp', 999)
                
                # Top performers (high scoring)
                if avg_points >= 15:
                    insights['top_performers'].append(player_summary)
                    
                # Most consistent
                if advanced_metrics.get('consistency_score', 0) >= 80:
                    insights['most_consistent'].append(player_summary)
                    
                # Trending players
                trend = advanced_metrics.get('trend_direction', 'stable')
                if trend == 'trending_up':
                    insights['trending_up'].append(player_summary)
                elif trend == 'trending_down':
                    insights['trending_down'].append(player_summary)
                    
                # ADP analysis
                if adp <= 100:  # Only analyze drafted players
                    grade = advanced_metrics.get('adp_performance_grade', '')
                    
                    if 'A+' in grade or 'A' in grade:
                        insights['adp_steals'].append(player_summary)
                    elif 'D' in grade or 'F' in grade:
                        insights['adp_busts'].append(player_summary)
                        
                    # Disappointments (early picks underperforming)
                    if adp <= 50 and avg_points < 8:
                        insights['biggest_disappointments'].append(player_summary)
                        
            # Sort insights
            insights['top_performers'].sort(key=lambda x: x['avg_points'], reverse=True)
            insights['most_consistent'].sort(key=lambda x: x['consistency'], reverse=True)
            insights['adp_steals'].sort(key=lambda x: x['avg_points'], reverse=True)
            insights['adp_busts'].sort(key=lambda x: x['adp'])
            insights['biggest_disappointments'].sort(key=lambda x: x['adp'])
            
            # Limit results
            for key in insights:
                if isinstance(insights[key], list):
                    insights[key] = insights[key][:20]
                    
            # Save insights
            insights_file = f"{self.data_dir}/performance_insights.json"
            with open(insights_file, 'w') as f:
                json.dump(insights, f, indent=2)
                
            print("Generated performance insights")
            return insights
            
        except Exception as e:
            print(f"Error generating performance insights: {e}")
            return None

def main():
    """Main execution function"""
    tracker = WeeklyPerformanceTracker()
    
    print("Starting weekly performance tracking...")
    
    current_week = tracker.get_current_week()
    
    if current_week == 0:
        print("Preseason - no performance tracking needed")
        return True
        
    # Update performance tracking
    tracking_success = tracker.update_performance_tracking(current_week)
    
    if not tracking_success:
        print("Failed to update performance tracking")
        return False
        
    # Create weekly snapshot
    snapshot_success = tracker.create_weekly_snapshot(current_week)
    
    # Generate performance insights
    insights = tracker.generate_performance_insights()
    
    print(f"\nWeekly performance tracking completed!")
    print(f"Performance tracking: {'✓' if tracking_success else '✗'}")
    print(f"Weekly snapshot: {'✓' if snapshot_success else '✗'}")
    print(f"Performance insights: {'✓' if insights else '✗'}")
    
    return tracking_success and snapshot_success and (insights is not None)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
