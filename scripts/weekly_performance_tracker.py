#!/usr/bin/env python3
“””
Weekly Performance Tracker - Standalone Script
Updates consolidated performance files with multi-format scoring
For manual execution, testing, and data corrections
“””

import json
import os
import sys
import argparse
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import pandas as pd
import time

# Import nfl_data_py at module level for better error handling

try:
import nfl_data_py as nfl
NFL_DATA_AVAILABLE = True
except ImportError:
NFL_DATA_AVAILABLE = False
nfl = None

SCRIPT_VERSION = “Standalone_v1.1_Fixed”
CURRENT_SEASON = 2025

def safe_float(value, default=0.0):
“”“Safely convert value to float with fallback”””
try:
if pd.isna(value) or value is None:
return default
return float(value)
except (ValueError, TypeError):
return default

def safe_int(value, default=0):
“”“Safely convert value to int with fallback”””
try:
if pd.isna(value) or value is None:
return default
return int(float(value))  # Convert via float to handle decimal strings
except (ValueError, TypeError):
return default

def calculate_fantasy_points(stats: Dict) -> Dict:
“”“Calculate fantasy points for all scoring formats”””
# Standard scoring multipliers
scoring = {
‘passing_yards’: 0.04,      # 1 pt per 25 yards (25 yards = 1 point)
‘passing_tds’: 4,           # 4 pts per TD
‘interceptions’: -2,        # -2 pts per INT
‘rushing_yards’: 0.1,       # 1 pt per 10 yards
‘rushing_tds’: 6,           # 6 pts per TD
‘receiving_yards’: 0.1,     # 1 pt per 10 yards
‘receiving_tds’: 6,         # 6 pts per TD
‘receptions_ppr’: 1,        # PPR: 1 pt per reception
‘receptions_half’: 0.5,     # Half PPR: 0.5 pt per reception
‘fumbles_lost’: -2,         # -2 pts per fumble lost
‘two_pt_conversions’: 2     # 2 pts per 2PT conversion
}

```
# Calculate base points (standard scoring)
standard_points = (
    stats.get('passing_yards', 0) * scoring['passing_yards'] +
    stats.get('passing_tds', 0) * scoring['passing_tds'] +
    stats.get('interceptions', 0) * scoring['interceptions'] +
    stats.get('rushing_yards', 0) * scoring['rushing_yards'] +
    stats.get('rushing_tds', 0) * scoring['rushing_tds'] +
    stats.get('receiving_yards', 0) * scoring['receiving_yards'] +
    stats.get('receiving_tds', 0) * scoring['receiving_tds'] +
    stats.get('fumbles_lost', 0) * scoring['fumbles_lost'] +
    stats.get('two_pt_conversions', 0) * scoring['two_pt_conversions']
)

# Add reception bonuses for PPR formats
receptions = stats.get('receptions', 0)
half_ppr_points = standard_points + (receptions * scoring['receptions_half'])
ppr_points = standard_points + (receptions * scoring['receptions_ppr'])

return {
    'standard': round(standard_points, 2),
    'half_ppr': round(half_ppr_points, 2),
    'ppr': round(ppr_points, 2)
}
```

def load_existing_players() -> Optional[Dict]:
“”“Load the main player database”””
try:
with open(‘data/players.json’, ‘r’) as f:
data = json.load(f)
if ‘players’ in data:
return data
else:
print(“Error: Invalid players.json structure - missing ‘players’ key”)
return None
except FileNotFoundError:
print(“Error: data/players.json not found - run collect_nfl_data.py first”)
return None
except json.JSONDecodeError as e:
print(f”Error: Invalid JSON in players.json - {e}”)
return None

def get_week_performance_data(week: int) -> Optional[pd.DataFrame]:
“”“Get NFL performance data for specific week”””
print(f”Loading Week {week} NFL performance data…”)

```
try:
    import nfl_data_py as nfl
    
    # Try current season first
    try:
        weekly_data = nfl.import_weekly_data([CURRENT_SEASON])
        if weekly_data is not None and not weekly_data.empty:
            print(f"Loaded {CURRENT_SEASON} data: {len(weekly_data)} total records")
        else:
            raise Exception(f"No {CURRENT_SEASON} data available")
    except:
        print(f"No {CURRENT_SEASON} data - falling back to 2024")
        weekly_data = nfl.import_weekly_data([2024])
        if weekly_data is None or weekly_data.empty:
            print("No performance data available from any season")
            return None
    
    # Filter for specific week
    if 'week' in weekly_data.columns:
        week_data = weekly_data[weekly_data['week'] == week]
        if week_data.empty:
            # Use latest available week if requested week doesn't exist
            latest_week = weekly_data['week'].max()
            week_data = weekly_data[weekly_data['week'] == latest_week]
            print(f"Week {week} not found - using latest available week {latest_week}")
    else:
        print("Warning: No 'week' column found - using all available data")
        week_data = weekly_data
    
    print(f"Filtered to {len(week_data)} player performances for analysis")
    return week_data
    
except ImportError:
    print("Error: nfl_data_py not installed - run: pip install nfl_data_py")
    return None
except Exception as e:
    print(f"Error loading performance data: {e}")
    return None
```

def load_season_performances() -> Dict:
“”“Load or create season performances file”””
filepath = ‘data/season_2025_performances.json’

```
if os.path.exists(filepath):
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            # Validate structure
            if 'metadata' not in data or 'players' not in data:
                print("Warning: Invalid season_2025_performances.json structure - creating new")
                raise ValueError("Invalid structure")
            return data
    except (json.JSONDecodeError, ValueError):
        print("Error reading existing season file - creating new structure")

# Create new structure
return {
    'metadata': {
        'season': CURRENT_SEASON,
        'created_at': datetime.now(timezone.utc).isoformat(),
        'last_updated': None,
        'weeks_processed': [],
        'total_players': 0,
        'scoring_formats': ['standard', 'half_ppr', 'ppr']
    },
    'players': {}
}
```

def load_weekly_snapshots() -> Dict:
“”“Load or create weekly snapshots file”””
filepath = ‘data/weekly_snapshots.json’

```
if os.path.exists(filepath):
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            # Validate structure
            if 'metadata' not in data or 'snapshots' not in data:
                print("Warning: Invalid weekly_snapshots.json structure - creating new")
                raise ValueError("Invalid structure")
            return data
    except (json.JSONDecodeError, ValueError):
        print("Error reading existing snapshots file - creating new structure")

# Create new structure
return {
    'metadata': {
        'season': CURRENT_SEASON,
        'created_at': datetime.now(timezone.utc).isoformat(),
        'scoring_formats': ['standard', 'half_ppr', 'ppr']
    },
    'snapshots': []
}
```

def update_season_performances(season_data: Dict, players_db: Dict, week_data: pd.DataFrame, week: int, force_update: bool = False) -> int:
“”“Update season performance data with new week”””
updated_count = 0

```
# Create performance lookup from NFL data
performance_lookup = {}
for _, row in week_data.iterrows():
    player_name = str(row.get('player_name', '')).strip()
    if not player_name or player_name.lower() == 'nan':
        continue
    
    # Extract all relevant stats with safe conversion
    stats = {
        'passing_yards': safe_float(row.get('passing_yards')),
        'passing_tds': safe_int(row.get('passing_tds')),
        'interceptions': safe_int(row.get('interceptions')),
        'rushing_yards': safe_float(row.get('rushing_yards')),
        'rushing_tds': safe_int(row.get('rushing_tds')),
        'receiving_yards': safe_float(row.get('receiving_yards')),
        'receiving_tds': safe_int(row.get('receiving_tds')),
        'receptions': safe_int(row.get('receptions')),
        'targets': safe_int(row.get('targets')),
        'carries': safe_int(row.get('carries')),
        'fumbles_lost': safe_int(row.get('fumbles_lost')),
        'two_pt_conversions': safe_int(row.get('two_point_conversions', 0))  # Fixed column name
    }
    
    # Calculate fantasy points for all formats
    fantasy_points = calculate_fantasy_points(stats)
    
    performance_lookup[player_name] = {
        'week': week,
        'opponent': str(row.get('opponent_team', '')),
        'home_away': 'Home' if str(row.get('home', '')).lower() in ['true', '1', 'home'] else 'Away',
        'stats': stats,
        'fantasy_points': fantasy_points
    }

print(f"Created performance lookup for {len(performance_lookup)} players")

# Update player records
players_list = players_db.get('players', [])
if not players_list:
    print("Warning: No players found in players database")
    return 0

for player in players_list:
    player_id = player.get('sleeper_id') or player.get('player_id')
    player_name = player.get('name', '').strip()
    
    if not player_id or not player_name:
        continue
    
    # Find performance data - try exact match first
    performance = performance_lookup.get(player_name)
    
    # If no exact match, try improved fuzzy matching with stricter criteria
    if not performance:
        player_name_lower = player_name.lower().replace('.', '').replace("'", "")
        player_name_words = set(player_name_lower.split())
        best_match = None
        best_score = 0
        
        for perf_name, perf_data in performance_lookup.items():
            perf_name_lower = perf_name.lower().replace('.', '').replace("'", "")
            perf_name_words = set(perf_name_lower.split())
            
            # Calculate multiple matching criteria
            word_overlap = len(player_name_words & perf_name_words)
            total_unique_words = len(player_name_words | perf_name_words)
            
            # Jaccard similarity with minimum overlap requirement
            if word_overlap >= 1 and total_unique_words > 0:
                jaccard_score = word_overlap / total_unique_words
                
                # Bonus for exact substring matches (handles Jr., III, etc.)
                substring_bonus = 0
                if any(word in perf_name_lower for word in player_name_words if len(word) > 2):
                    substring_bonus = 0.1
                
                final_score = jaccard_score + substring_bonus
                
                # Require higher threshold for better accuracy
                if final_score > best_score and final_score >= 0.6:  # Increased from 0.5
                    best_match = perf_data
                    best_score = final_score
        
        performance = best_match
    
    if not performance:
        continue
    
    # Initialize or update player record
    if player_id not in season_data['players']:
        season_data['players'][player_id] = {
            'info': {
                'name': player_name,
                'position': player.get('position'),
                'team': player.get('team'),
                'sleeper_id': player_id
            },
            'weekly_performances': [],
            'season_totals': {
                'games_played': 0,
                'fantasy_points': {'standard': 0, 'half_ppr': 0, 'ppr': 0}
            }
        }
    
    player_record = season_data['players'][player_id]
    
    # Remove existing week data if force update or if week already exists
    existing_weeks = {p.get('week') for p in player_record['weekly_performances']}
    if week in existing_weeks:
        if force_update:
            player_record['weekly_performances'] = [
                p for p in player_record['weekly_performances'] 
                if p.get('week') != week
            ]
            print(f"Force updating Week {week} data for {player_name}")
        else:
            print(f"Skipping {player_name} - Week {week} already exists (use --force to override)")
            continue
    
    # Add new performance
    player_record['weekly_performances'].append(performance)
    player_record['weekly_performances'].sort(key=lambda x: x.get('week', 0))
    
    # Recalculate season totals
    games_played = len(player_record['weekly_performances'])
    totals = {'standard': 0, 'half_ppr': 0, 'ppr': 0}
    
    for perf in player_record['weekly_performances']:
        for format_name in totals.keys():
            totals[format_name] += perf['fantasy_points'].get(format_name, 0)
    
    player_record['season_totals'] = {
        'games_played': games_played,
        'fantasy_points': {
            format_name: round(total, 2) 
            for format_name, total in totals.items()
        },
        'averages': {
            format_name: round(total / games_played if games_played > 0 else 0, 2)
            for format_name, total in totals.items()
        }
    }
    
    updated_count += 1

# Update metadata
weeks_processed = season_data['metadata'].get('weeks_processed', [])
if week not in weeks_processed:
    weeks_processed.append(week)
    weeks_processed.sort()

season_data['metadata'].update({
    'last_updated': datetime.now(timezone.utc).isoformat(),
    'weeks_processed': weeks_processed,
    'total_players': len(season_data['players']),
    'script_version': SCRIPT_VERSION
})

return updated_count
```

def create_week_snapshot(week_data: Optional[pd.DataFrame], week: int) -> Dict:
“”“Create comprehensive weekly snapshot with multi-format data”””
if week_data is None or week_data.empty:
return {
‘week’: week,
‘date’: datetime.now(timezone.utc).date().isoformat(),
‘data_available’: False,
‘message’: f’No performance data available for Week {week}’,
‘total_performances’: 0
}

```
# Calculate multi-format performance for all players
player_performances = []
for _, row in week_data.iterrows():
    player_name = str(row.get('player_name', '')).strip()
    if not player_name or player_name.lower() == 'nan':
        continue
        
    stats = {
        'passing_yards': float(row.get('passing_yards', 0) or 0),
        'passing_tds': int(row.get('passing_tds', 0) or 0),
        'interceptions': int(row.get('interceptions', 0) or 0),
        'rushing_yards': float(row.get('rushing_yards', 0) or 0),
        'rushing_tds': int(row.get('rushing_tds', 0) or 0),
        'receiving_yards': float(row.get('receiving_yards', 0) or 0),
        'receiving_tds': int(row.get('receiving_tds', 0) or 0),
        'receptions': int(row.get('receptions', 0) or 0)
    }
    
    fantasy_points = calculate_fantasy_points(stats)
    
    player_performances.append({
        'name': player_name,
        'position': str(row.get('position', '')),
        'team': str(row.get('team', '')),
        'fantasy_points': fantasy_points
    })

# Create comprehensive snapshot
snapshot = {
    'week': week,
    'date': datetime.now(timezone.utc).date().isoformat(),
    'data_available': True,
    'total_performances': len(player_performances),
    'scoring_leaders': {}
}

# Calculate leaders and stats for each scoring format
for format_name in ['standard', 'half_ppr', 'ppr']:
    # Sort players by this format's points
    sorted_players = sorted(
        player_performances, 
        key=lambda x: x['fantasy_points'][format_name], 
        reverse=True
    )
    
    # Calculate position leaders
    position_leaders = {}
    for position in ['QB', 'RB', 'WR', 'TE']:
        pos_players = [p for p in sorted_players if p['position'] == position]
        if pos_players:
            leader = pos_players[0]
            position_leaders[position] = {
                'name': leader['name'],
                'team': leader['team'],
                'points': leader['fantasy_points'][format_name]
            }
    
    # Calculate overall statistics for this format
    format_points = [p['fantasy_points'][format_name] for p in player_performances]
    
    if format_points:
        snapshot['scoring_leaders'][format_name] = {
            'position_leaders': position_leaders,
            'overall_leader': {
                'name': sorted_players[0]['name'],
                'position': sorted_players[0]['position'],
                'team': sorted_players[0]['team'],
                'points': sorted_players[0]['fantasy_points'][format_name]
            } if sorted_players else None,
            'statistics': {
                'average_points': round(sum(format_points) / len(format_points), 2),
                'median_points': round(sorted(format_points)[len(format_points)//2], 2),
                'boom_games_20_plus': len([p for p in format_points if p >= 20]),
                'bust_games_under_5': len([p for p in format_points if p < 5]),
                'highest_score': max(format_points),
                'players_scored': len([p for p in format_points if p > 0])
            }
        }

return snapshot
```

def save_data_files(season_data: Dict, snapshots_data: Dict) -> bool:
“”“Save updated data files with validation and file locking protection”””
try:
# Ensure data directory exists
os.makedirs(‘data’, exist_ok=True)

```
    # Create temporary files first to avoid corruption during writes
    season_file = 'data/season_2025_performances.json'
    snapshots_file = 'data/weekly_snapshots.json'
    
    temp_season_file = season_file + '.tmp'
    temp_snapshots_file = snapshots_file + '.tmp'
    
    # Write to temporary files first
    with open(temp_season_file, 'w') as f:
        json.dump(season_data, f, indent=2, ensure_ascii=False)
    
    with open(temp_snapshots_file, 'w') as f:
        json.dump(snapshots_data, f, indent=2, ensure_ascii=False)
    
    # Validate temporary files before replacing originals
    with open(temp_season_file, 'r') as f:
        test_load = json.load(f)
        assert 'metadata' in test_load and 'players' in test_load
        assert isinstance(test_load['players'], dict)
    
    with open(temp_snapshots_file, 'r') as f:
        test_load = json.load(f)
        assert 'metadata' in test_load and 'snapshots' in test_load
        assert isinstance(test_load['snapshots'], list)
    
    # Atomic replacement - rename temp files to actual files
    if os.path.exists(season_file):
        os.replace(temp_season_file, season_file)
    else:
        os.rename(temp_season_file, season_file)
        
    if os.path.exists(snapshots_file):
        os.replace(temp_snapshots_file, snapshots_file)
    else:
        os.rename(temp_snapshots_file, snapshots_file)
    
    # Calculate file sizes for reporting
    season_size_kb = os.path.getsize(season_file) / 1024
    snapshots_size_kb = os.path.getsize(snapshots_file) / 1024
    
    print(f"Successfully saved:")
    print(f"  - {season_file} ({len(season_data['players'])} players, {season_size_kb:.1f} KB)")
    print(f"  - {snapshots_file} ({len(snapshots_data['snapshots'])} snapshots, {snapshots_size_kb:.1f} KB)")
    
    return True
    
except Exception as e:
    # Clean up temporary files on error
    for temp_file in [temp_season_file, temp_snapshots_file]:
        if 'temp_file' in locals() and os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass
    
    print(f"Error saving data files: {e}")
    return False
```

def main():
“”“Main execution function with argument parsing”””
parser = argparse.ArgumentParser(description=‘Update weekly fantasy performance data’)
parser.add_argument(‘week’, type=int, help=‘Week number to process (1-18)’)
parser.add_argument(’–force’, action=‘store_true’, help=‘Force update existing week data’)
parser.add_argument(’–verbose’, action=‘store_true’, help=‘Enable verbose output’)

```
args = parser.parse_args()

if args.week < 1 or args.week > 18:
    print("Error: Week must be between 1 and 18")
    return False

print(f"=== Weekly Performance Tracker v{SCRIPT_VERSION} ===")
print(f"Processing Week {args.week} ({'Force update' if args.force else 'Normal update'})")
print(f"Season: {CURRENT_SEASON}")
print()

try:
    # Load existing data structures
    print("Loading existing data...")
    players_db = load_existing_players()
    if not players_db:
        return False
    
    season_data = load_season_performances()
    snapshots_data = load_weekly_snapshots()
    
    # Check if week already processed
    processed_weeks = season_data['metadata'].get('weeks_processed', [])
    if args.week in processed_weeks and not args.force:
        print(f"Week {args.week} already processed. Use --force to override.")
        return True
    
    # Get performance data
    print("Fetching NFL performance data...")
    week_data = get_week_performance_data(args.week)
    
    if week_data is not None and not week_data.empty:
        # Update season performances
        print("Updating player performance records...")
        updated_count = update_season_performances(
            season_data, players_db, week_data, args.week, args.force
        )
        print(f"Updated {updated_count} player records")
        
        # Create week snapshot
        print("Creating weekly snapshot...")
        snapshot = create_week_snapshot(week_data, args.week)
    else:
        print(f"No performance data available for Week {args.week}")
        snapshot = create_week_snapshot(None, args.week)
        updated_count = 0
    
    # Add/update snapshot
    snapshots_data['snapshots'] = [
        s for s in snapshots_data['snapshots'] 
        if s.get('week') != args.week
    ]
    snapshots_data['snapshots'].append(snapshot)
    snapshots_data['snapshots'].sort(key=lambda x: x.get('week', 0))
    
    # Save updated files
    print("Saving updated data...")
    if save_data_files(season_data, snapshots_data):
        print(f"\n✅ Week {args.week} processing completed successfully")
        if snapshot.get('data_available'):
            for format_name in ['standard', 'half_ppr', 'ppr']:
                leaders = snapshot['scoring_leaders'][format_name]
                overall_leader = leaders.get('overall_leader', {})
                if overall_leader:
                    print(f"  {format_name.upper()} leader: {overall_leader['name']} ({overall_leader['points']} pts)")
        return True
    else:
        print(f"\n❌ Failed to save data files")
        return False
        
except Exception as e:
    print(f"\n❌ Processing failed: {str(e)}")
    if args.verbose:
        import traceback
        traceback.print_exc()
    return False
```

if **name** == “**main**”:
success = main()
sys.exit(0 if success else 1)
