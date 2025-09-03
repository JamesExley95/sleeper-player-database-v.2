#!/usr/bin/env python3
"""
Byline Content MVP Data Collector
Focused on weekly fantasy football recap generation
Fixed to work with actual Sleeper database structure
"""

import json
import os
import requests
from datetime import datetime, timezone
from typing import Dict, List, Any
import pandas as pd

SCRIPT_VERSION = "MVP_1.1_Fixed"
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
        
        # The Sleeper database is a flat dict of player_id: player_data
        sleeper_data = response.json()
        
        print(f"✅ Sleeper database loaded: {len(sleeper_data)} total players")
        return sleeper_data
        
    except Exception as e:
        print(f"❌ Failed to load Sleeper database: {e}")
        raise

def filter_active_players(sleeper_data: Dict) -> List[Dict]:
    """Filter to ~300 fantasy-relevant active players"""
    print("=== Filtering to Active Fantasy Players ===")
    
    # Fantasy positions only
    fantasy_positions = ['QB', 'RB', 'WR', 'TE', 'K', 'DEF']
    
    active_players = []
    
    # Iterate through the flat dictionary structure
    for player_id, player_info in sleeper_data.items():
        # Skip if no player info
        if not player_info or not isinstance(player_info, dict):
            continue
            
        # Must have fantasy position
        position = player_info.get('position')
        if position not in fantasy_positions:
            continue
            
        # Skip inactive players (but keep IR for potential)
        status = player_info.get('status')
        if status in ['Inactive', 'Retired']:
            continue
            
        # Must have basic info
        full_name = player_info.get('full_name')
        if not full_name:
            first_name = player_info.get('first_name', '')
            last_name = player_info.get('last_name', '')
            full_name = f"{first_name} {last_name}".strip()
        
        if not full_name or full_name == " ":
            continue
            
        # Skip duplicates or invalid entries
        if 'Duplicate' in full_name or len(full_name) < 3:
            continue
        
        active_players.append({
            'player_id': player_id,
            'sleeper_id': player_id,
            'name': full_name,
            'position': position,
            'team': player_info.get('team'),
            'status': status or 'Active',
            'years_exp': player_info.get('years_exp', 0),
            'age': player_info.get('age'),
            'height': player_info.get('height'),
            'weight': player_info.get('weight'),
            'college': player_info.get('college'),
            'injury_status': player_info.get('injury_status'),
            'sleeper_data': player_info  # Keep full record for reference
        })
    
    # Sort by position priority then name
    position_priority = {'QB': 1, 'RB': 2, 'WR': 3, 'TE': 4, 'K': 5, 'DEF': 6}
    active_players.sort(key=lambda x: (
        position_priority.get(x['position'], 7),
        x['name']
    ))
    
    # Take top ~300 most relevant players
    active_players = active_players[:300]
    
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
        
        # Try to get 2025 data first
        try:
            weekly_data = nfl.import_weekly_data([CURRENT_YEAR])
            print(f"✅ {CURRENT_YEAR} weekly data loaded: {len(weekly_data)} records")
        except:
            # Fall back to 2024 data for baseline
            print(f"⚠️  No {CURRENT_YEAR} data yet, using 2024 baseline")
            weekly_data = nfl.import_weekly_data([2024])
            print(f"✅ 2024 baseline data loaded: {len(weekly_data)} records")
        
        if weekly_data is None or weekly_data.empty:
            print("⚠️  No performance data available")
            return {}
        
        # Filter to specific week if possible
        if 'week' in weekly_data.columns and week <= weekly_data['week'].max():
            week_data = weekly_data[weekly_data['week'] == week]
            print(f"✅ Week {week} filtered data: {len(week_data)} performances")
        else:
            # Use most recent week or season data
            if 'week' in weekly_data.columns:
                latest_week = weekly_data['week'].max()
                week_data = weekly_data[weekly_data['week'] == latest_week]
                print(f"✅ Latest week {latest_week} data: {len(week_data)} performances")
            else:
                week_data = weekly_data
                print(f"✅ Season aggregate data: {len(week_data)} performances")
        
        return {
            'week': week,
            'data': week_data,
            'total_records': len(week_data),
            'season': CURRENT_YEAR if CURRENT_YEAR in str(weekly_data.get('season', []).iloc[0] if not weekly_data.empty else 2024) else 2024
        }
        
    except ImportError:
        print("⚠️  nfl_data_py not available - install with: pip install nfl_data_py")
        return {}
    except Exception as e:
        print(f"⚠️  Performance data error: {e}")
        return {}

def create_player_performance_summary(active_players: List[Dict], performance_data: Dict) -> List[Dict]:
    """Create performance summary for active players"""
    print("=== Creating Player Performance Summary ===")
    
    # Create performance lookup by player name matching
    performance_lookup = {}
    if performance_data and 'data' in performance_data:
        pdf = performance_data['data']
        
        for _, row in pdf.iterrows():
            player_name = row.get('player_name', '').strip()
            if player_name:
                # Use player_name as key since Sleeper IDs might not match NFL data IDs
                performance_lookup[player_name] = {
                    'fantasy_points': float(row.get('fantasy_points', 0) or 0),
                    'fantasy_points_ppr': float(row.get('fantasy_points_ppr', 0) or 0),
                    'passing_yards': float(row.get('passing_yards', 0) or 0),
                    'rushing_yards': float(row.get('rushing_yards', 0) or 0),
                    'receiving_yards': float(row.get('receiving_yards', 0) or 0),
                    'passing_tds': int(row.get('passing_tds', 0) or 0),
                    'rushing_tds': int(row.get('rushing_tds', 0) or 0),
                    'receiving_tds': int(row.get('receiving_tds', 0) or 0),
                    'targets': int(row.get('targets', 0) or 0),
                    'receptions': int(row.get('receptions', 0) or 0),
                    'interceptions': int(row.get('interceptions', 0) or 0),
                    'carries': int(row.get('carries', 0) or 0)
                }
    
    # Enhance player data with performance
    enhanced_players = []
    performance_matches = 0
    
    for player in active_players:
        player_name = player['name']
        
        # Try exact match first
        performance = performance_lookup.get(player_name, {})
        
        # If no exact match, try partial matching for common name variations
        if not performance:
            for perf_name in performance_lookup.keys():
                if perf_name in player_name or player_name in perf_name:
                    # Simple similarity check
                    if len(set(player_name.split()) & set(perf_name.split())) >= 1:
                        performance = performance_lookup[perf_name]
                        break
        
        if performance:
            performance_matches += 1
        
        enhanced_player = {
            **player,
            'current_week_performance': performance,
            'has_performance_data': bool(performance),
            'last_updated': datetime.now(timezone.utc).isoformat()
        }
        
        enhanced_players.append(enhanced_player)
    
    print(f"✅ Enhanced {len(enhanced_players)} players with performance data")
    print(f"📊 Performance matches found: {performance_matches}/{len(enhanced_players)}")
    
    return enhanced_players

def generate_basic_insights(players: List[Dict]) -> Dict:
    """Generate basic insights for story generation"""
    print("=== Generating Basic Insights ===")
    
    # Get players with performance data
    active_performers = [
        p for p in players 
        if p['current_week_performance'].get('fantasy_points_ppr', 0) > 0
    ]
    
    if not active_performers:
        print("⚠️  No performance data available for insights yet")
        return {
            'top_performers': [],
            'position_leaders': {},
            'notable_performances': [],
            'summary_stats': {
                'total_players_analyzed': len(players),
                'players_with_data': 0,
                'average_points': 0,
                'highest_score': 0
            }
        }
    
    # Sort by fantasy points (PPR)
    by_points = sorted(active_performers, 
                      key=lambda x: x['current_week_performance'].get('fantasy_points_ppr', 0), 
                      reverse=True)
    
    # Top performers overall
    top_performers = by_points[:10]
    
    # Position leaders
    position_leaders = {}
    for position in ['QB', 'RB', 'WR', 'TE', 'K', 'DEF']:
        pos_players = [p for p in by_points if p['position'] == position]
        if pos_players:
            position_leaders[position] = pos_players[0]
    
    # Notable performances (>= 20 points PPR)
    notable = [p for p in by_points if p['current_week_performance'].get('fantasy_points_ppr', 0) >= 20]
    
    # Calculate summary statistics
    total_points = sum(p['current_week_performance']['fantasy_points_ppr'] for p in active_performers)
    avg_points = total_points / len(active_performers) if active_performers else 0
    highest_score = by_points[0]['current_week_performance']['fantasy_points_ppr'] if by_points else 0
    
    insights = {
        'top_performers': top_performers,
        'position_leaders': position_leaders,
        'notable_performances': notable,
        'summary_stats': {
            'total_players_analyzed': len(players),
            'players_with_data': len(active_performers),
            'average_points': round(avg_points, 2),
            'highest_score': round(highest_score, 2),
            'notable_count': len(notable)
        }
    }
    
    print(f"✅ Generated insights from {len(active_performers)} performances")
    print(f"📊 Top score: {highest_score:.1f}, Average: {avg_points:.1f}, Notable (20+): {len(notable)}")
    
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
            'players_with_performance': len([p for p in players if p.get('has_performance_data')]),
            'data_sources': ['sleeper_database', 'nfl_data_py'],
            'data_quality_score': calculate_data_quality_score(players)
        },
        'players': players
    }
    
    with open('data/players.json', 'w') as f:
        json.dump(main_database, f, indent=2)
    
    # Weekly insights for story generation
    weekly_insights = {
        'metadata': {
            'version': SCRIPT_VERSION,
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'week': week,
            'season': CURRENT_YEAR,
            'ready_for_stories': insights['summary_stats']['players_with_data'] > 0
        },
        'insights': insights
    }
    
    with open('data/weekly_insights.json', 'w') as f:
        json.dump(weekly_insights, f, indent=2)
    
    # Database metadata for system monitoring
    metadata = {
        'last_updated': datetime.now(timezone.utc).isoformat(),
        'version': SCRIPT_VERSION,
        'status': 'SUCCESS',
        'data_health': {
            'total_players': len(players),
            'performance_coverage': f"{len([p for p in players if p.get('has_performance_data')])}/{len(players)}",
            'quality_score': calculate_data_quality_score(players),
            'insights_available': insights['summary_stats']['players_with_data'] > 0
        },
        'files_generated': [
            'data/players.json',
            'data/weekly_insights.json',
            'data/metadata.json'
        ]
    }
    
    with open('data/metadata.json', 'w') as f:
        json.dump(metadata, f, indent=2)
    
    # Create weekly snapshot for historical tracking
    snapshot_file = f'data/weekly_snapshots/week_{week:02d}_snapshot.json'
    with open(snapshot_file, 'w') as f:
        json.dump({
            'week': week,
            'season': CURRENT_YEAR,
            'snapshot_date': datetime.now(timezone.utc).isoformat(),
            'player_count': len(players),
            'performance_data_available': insights['summary_stats']['players_with_data'] > 0,
            'top_performer': insights['top_performers'][0] if insights['top_performers'] else None,
            'summary_stats': insights['summary_stats']
        }, f, indent=2)
    
    # Calculate and display file sizes
    main_size = os.path.getsize('data/players.json') / 1024
    insights_size = os.path.getsize('data/weekly_insights.json') / 1024
    
    print(f"✅ Players database: {len(players)} players ({main_size:.1f} KB)")
    print(f"✅ Weekly insights: {insights['summary_stats']['players_with_data']} with data ({insights_size:.1f} KB)")
    print(f"✅ Metadata: Quality score {calculate_data_quality_score(players)}/100")
    print(f"✅ Weekly snapshot: {snapshot_file}")

def calculate_data_quality_score(players: List[Dict]) -> int:
    """Calculate data quality score (0-100)"""
    if not players:
        return 0
    
    # Count completeness levels
    complete_basic_info = sum(1 for p in players if p.get('name') and p.get('position') and p.get('team'))
    has_performance = sum(1 for p in players if p.get('has_performance_data'))
    
    # Quality scoring
    basic_score = (complete_basic_info / len(players)) * 60  # Basic info worth 60%
    performance_score = (has_performance / len(players)) * 40  # Performance worth 40%
    
    return int(basic_score + performance_score)

def main():
    """Main execution function"""
    print(f"=== Byline Content MVP Data Collector v{SCRIPT_VERSION} ===")
    print(f"Purpose: Create comprehensive player database for fantasy recap generation")
    print(f"Target: ~300 active players with performance tracking")
    print(f"Execution: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print()
    
    try:
        # Setup
        setup_directories()
        
        # Load comprehensive Sleeper player database (11,400+ players)
        sleeper_data = load_sleeper_database()
        
        if not sleeper_data:
            raise Exception("No Sleeper data loaded")
        
        # Filter to ~300 active fantasy-relevant players
        active_players = filter_active_players(sleeper_data)
        
        if len(active_players) < 250:
            print(f"⚠️  Warning: Only {len(active_players)} active players - may impact coverage")
        elif len(active_players) >= 300:
            print(f"✅ Target achieved: {len(active_players)} active players")
        
        # Load current week performance data
        week = 1  # Could be parameterized later
        performance_data = load_weekly_performance_data(week)
        
        # Create enhanced player database with performance
        enhanced_players = create_player_performance_summary(active_players, performance_data)
        
        # Generate insights for Pipedream story generation
        insights = generate_basic_insights(enhanced_players)
        
        # Save all data files
        save_data(enhanced_players, insights, week)
        
        # Success summary
        quality_score = calculate_data_quality_score(enhanced_players)
        performance_ready = insights['summary_stats']['players_with_data'] > 0
        
        print(f"\n🎯 === DATA COLLECTION COMPLETE ===")
        print(f"✅ Database Status: SUCCESS")
        print(f"✅ Players Processed: {len(enhanced_players)}")
        print(f"✅ Data Quality Score: {quality_score}/100")
        print(f"✅ Performance Data: {'Available' if performance_ready else 'Pending (games not played)'}")
        print(f"✅ Ready for Pipedream: {'YES' if quality_score >= 70 else 'NEEDS REVIEW'}")
        
        if not performance_ready:
            print(f"\n📅 Note: Performance data will populate after Week {week} games are played")
            print(f"🔄 Re-run this script after games to get live stats for story generation")
        
        return True
        
    except Exception as e:
        print(f"\n❌ === DATA COLLECTION FAILED ===")
        print(f"Error: {str(e)}")
        
        # Create error report for debugging
        error_report = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error': str(e),
            'script_version': SCRIPT_VERSION,
            'status': 'FAILED'
        }
        
        os.makedirs('data', exist_ok=True)
        with open('data/error_report.json', 'w') as f:
            json.dump(error_report, f, indent=2)
        
        print(f"💾 Error details saved to: data/error_report.json")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
