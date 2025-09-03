#!/usr/bin/env python3
"""
Byline Content MVP Data Collector
Focused on weekly fantasy football recap generation
Simplified for 3-week MVP timeline
"""

import json
import os
import requests
from datetime import datetime, timezone
from typing import Dict, List, Any
import pandas as pd

SCRIPT_VERSION = "MVP_1.0"
CURRENT_YEAR = 2025
SLEEPER_URL = "https://raw.githubusercontent.com/JamesExley95/sleeper-player-database/refs/heads/main/players_detailed.json"

def setup_directories():
    """Create necessary directories"""
    directories = ['data', 'data/weekly_snapshots', 'data/player_histories']
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
    print("✅ Directory structure created")

def load_sleeper_database() -> Dict:
    """Load the comprehensive Sleeper player database"""
    print("=== Loading Sleeper Player Database ===")
    
    try:
        response = requests.get(SLEEPER_URL, timeout=30)
        response.raise_for_status()
        
        sleeper_data = response.json()
        players = sleeper_data.get('players', [])
        
        print(f"✅ Sleeper database loaded: {len(players)} total players")
        return sleeper_data
        
    except Exception as e:
        print(f"❌ Failed to load Sleeper database: {e}")
        raise

def filter_active_players(all_players: List[Dict]) -> List[Dict]:
    """Filter to ~300 fantasy-relevant active players"""
    print("=== Filtering to Active Fantasy Players ===")
    
    # Fantasy positions only
    fantasy_positions = ['QB', 'RB', 'WR', 'TE', 'K', 'DEF']
    
    # Active statuses
    active_statuses = ['Active', 'Injured Reserve']
    
    active_players = []
    
    for player in all_players:
        # Must have fantasy position
        if player.get('position') not in fantasy_positions:
            continue
            
        # Must be active or IR
        if player.get('status') not in active_statuses:
            continue
            
        # Must have a team or be recently relevant
        if not player.get('team') and player.get('status') != 'Active':
            continue
            
        # Skip invalid entries
        if 'Duplicate Player' in player.get('name', ''):
            continue
            
        active_players.append({
            'player_id': player.get('player_id'),
            'name': player.get('name'),
            'position': player.get('position'),
            'team': player.get('team'),
            'status': player.get('status'),
            'years_exp': player.get('years_exp', 0)
        })
    
    # Sort by position then name
    active_players.sort(key=lambda x: (x['position'], x['name']))
    
    print(f"✅ Filtered to {len(active_players)} active fantasy players")
    
    # Position breakdown
    positions = {}
    for p in active_players:
        pos = p['position']
        positions[pos] = positions.get(pos, 0) + 1
    
    print(f"📊 Position breakdown: {positions}")
    return active_players

def load_weekly_performance_data(week: int = 1) -> Dict[str, Any]:
    """Load weekly performance data from nfl_data_py"""
    print(f"=== Loading Week {week} Performance Data ===")
    
    try:
        import nfl_data_py as nfl
        
        # Get weekly data for current year
        weekly_data = nfl.import_weekly_data([CURRENT_YEAR])
        
        if weekly_data is None or weekly_data.empty:
            print("⚠️  No weekly data available yet for 2025")
            return {}
        
        # Filter to specific week if data exists
        if 'week' in weekly_data.columns:
            week_data = weekly_data[weekly_data['week'] == week]
            print(f"✅ Week {week} data loaded: {len(week_data)} player performances")
        else:
            week_data = weekly_data
            print(f"✅ Season data loaded: {len(week_data)} total performances")
        
        return {
            'week': week,
            'data': week_data,
            'total_records': len(week_data)
        }
        
    except ImportError:
        print("⚠️  nfl_data_py not available - using basic structure")
        return {}
    except Exception as e:
        print(f"⚠️  Performance data load failed: {e}")
        return {}

def create_player_performance_summary(active_players: List[Dict], performance_data: Dict) -> List[Dict]:
    """Create performance summary for active players"""
    print("=== Creating Player Performance Summary ===")
    
    # Create performance lookup if we have data
    performance_lookup = {}
    if performance_data and 'data' in performance_data:
        for _, row in performance_data['data'].iterrows():
            player_id = row.get('player_id')
            if player_id:
                performance_lookup[player_id] = {
                    'fantasy_points': row.get('fantasy_points', 0) or 0,
                    'fantasy_points_ppr': row.get('fantasy_points_ppr', 0) or 0,
                    'passing_yards': row.get('passing_yards', 0) or 0,
                    'rushing_yards': row.get('rushing_yards', 0) or 0,
                    'receiving_yards': row.get('receiving_yards', 0) or 0,
                    'passing_tds': row.get('passing_tds', 0) or 0,
                    'rushing_tds': row.get('rushing_tds', 0) or 0,
                    'receiving_tds': row.get('receiving_tds', 0) or 0,
                    'targets': row.get('targets', 0) or 0,
                    'receptions': row.get('receptions', 0) or 0
                }
    
    # Enhance player data with performance
    enhanced_players = []
    
    for player in active_players:
        player_id = player['player_id']
        performance = performance_lookup.get(player_id, {})
        
        enhanced_player = {
            **player,
            'current_week_performance': performance,
            'last_updated': datetime.now(timezone.utc).isoformat()
        }
        
        enhanced_players.append(enhanced_player)
    
    print(f"✅ Enhanced {len(enhanced_players)} players with performance data")
    
    # Count players with actual performance data
    with_stats = len([p for p in enhanced_players if p['current_week_performance'].get('fantasy_points', 0) > 0])
    print(f"📊 Players with performance data: {with_stats}/{len(enhanced_players)}")
    
    return enhanced_players

def generate_basic_insights(players: List[Dict]) -> Dict:
    """Generate basic insights for story generation"""
    print("=== Generating Basic Insights ===")
    
    # Get players with performance data
    active_performers = [
        p for p in players 
        if p['current_week_performance'].get('fantasy_points', 0) > 0
    ]
    
    if not active_performers:
        print("⚠️  No performance data available for insights")
        return {
            'top_performers': [],
            'position_leaders': {},
            'notable_performances': [],
            'summary_stats': {
                'total_players_analyzed': len(players),
                'players_with_data': 0
            }
        }
    
    # Sort by fantasy points
    by_points = sorted(active_performers, 
                      key=lambda x: x['current_week_performance'].get('fantasy_points_ppr', 0), 
                      reverse=True)
    
    # Top performers overall
    top_performers = by_points[:10]
    
    # Position leaders
    position_leaders = {}
    for position in ['QB', 'RB', 'WR', 'TE', 'K']:
        pos_players = [p for p in by_points if p['position'] == position]
        if pos_players:
            position_leaders[position] = pos_players[0]
    
    # Notable performances (>= 20 points)
    notable = [p for p in by_points if p['current_week_performance'].get('fantasy_points_ppr', 0) >= 20]
    
    insights = {
        'top_performers': top_performers[:10],
        'position_leaders': position_leaders,
        'notable_performances': notable[:15],
        'summary_stats': {
            'total_players_analyzed': len(players),
            'players_with_data': len(active_performers),
            'average_points': round(sum(p['current_week_performance']['fantasy_points_ppr'] for p in active_performers) / len(active_performers), 2) if active_performers else 0,
            'highest_score': by_points[0]['current_week_performance']['fantasy_points_ppr'] if by_points else 0
        }
    }
    
    print(f"✅ Generated insights from {len(active_performers)} performances")
    return insights

def save_data(players: List[Dict], insights: Dict, week: int = 1):
    """Save all generated data"""
    print("=== Saving Data Files ===")
    
    # Main player database
    main_database = {
        'metadata': {
            'version': SCRIPT_VERSION,
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'week': week,
            'season': CURRENT_YEAR,
            'total_players': len(players),
            'data_sources': ['sleeper_database', 'nfl_data_py']
        },
        'players': players
    }
    
    with open('data/players.json', 'w') as f:
        json.dump(main_database, f, indent=2)
    
    # Weekly insights
    weekly_insights = {
        'metadata': {
            'version': SCRIPT_VERSION,
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'week': week,
            'season': CURRENT_YEAR
        },
        'insights': insights
    }
    
    with open('data/weekly_insights.json', 'w') as f:
        json.dump(weekly_insights, f, indent=2)
    
    # Weekly snapshot for historical tracking
    snapshot_file = f'data/weekly_snapshots/week_{week:02d}_snapshot.json'
    with open(snapshot_file, 'w') as f:
        json.dump({
            'week': week,
            'season': CURRENT_YEAR,
            'snapshot_date': datetime.now(timezone.utc).isoformat(),
            'players': players,
            'insights': insights
        }, f, indent=2)
    
    # Calculate file sizes
    main_size = os.path.getsize('data/players.json') / 1024
    insights_size = os.path.getsize('data/weekly_insights.json') / 1024
    
    print(f"✅ Main database saved: {len(players)} players ({main_size:.1f} KB)")
    print(f"✅ Weekly insights saved: ({insights_size:.1f} KB)")
    print(f"✅ Weekly snapshot saved: {snapshot_file}")

def main():
    """Main execution function"""
    print(f"=== Byline Content MVP Data Collector v{SCRIPT_VERSION} ===")
    print(f"Target: Automated fantasy football recap generation")
    print(f"Execution time: {datetime.now(timezone.utc).isoformat()}")
    
    try:
        # Setup
        setup_directories()
        
        # Load comprehensive player database
        sleeper_data = load_sleeper_database()
        all_players = sleeper_data.get('players', [])
        
        # Filter to active fantasy players (~300)
        active_players = filter_active_players(all_players)
        
        if len(active_players) < 200:
            print("⚠️  Warning: Less than 200 active players found")
        
        # Load weekly performance data
        week = 1  # Default to week 1, could be parameter
        performance_data = load_weekly_performance_data(week)
        
        # Create enhanced player summaries
        enhanced_players = create_player_performance_summary(active_players, performance_data)
        
        # Generate insights for story creation
        insights = generate_basic_insights(enhanced_players)
        
        # Save all data
        save_data(enhanced_players, insights, week)
        
        print(f"\n=== SUCCESS ===")
        print(f"MVP Data Collection Complete")
        print(f"Players processed: {len(enhanced_players)}")
        print(f"Performance data available: {insights['summary_stats']['players_with_data'] > 0}")
        print(f"Ready for story generation workflows")
        
    except Exception as e:
        print(f"\n=== ERROR ===")
        print(f"Data collection failed: {str(e)}")
        
        # Create error report
        error_report = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error': str(e),
            'script_version': SCRIPT_VERSION,
            'status': 'FAILED'
        }
        
        os.makedirs('data', exist_ok=True)
        with open('data/error_report.json', 'w') as f:
            json.dump(error_report, f, indent=2)
        
        raise

if __name__ == "__main__":
    main()
