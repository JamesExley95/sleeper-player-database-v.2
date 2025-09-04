#!/usr/bin/env python3

import json
import pandas as pd
import nfl_data_py as nfl
from datetime import datetime
import os
import sys

def load_players():
“”“Load players from the detailed JSON file”””
try:
with open(‘players_detailed.json’, ‘r’) as f:
players_data = json.load(f)
print(f”Loaded {len(players_data)} players from players_detailed.json”)
return players_data
except FileNotFoundError:
print(“Error: players_detailed.json not found”)
sys.exit(1)
except json.JSONDecodeError as e:
print(f”Error decoding players_detailed.json: {e}”)
sys.exit(1)

def get_fantasy_relevant_players(players_data):
“”“Filter to fantasy-relevant positions and active players”””
relevant_positions = [‘QB’, ‘RB’, ‘WR’, ‘TE’, ‘K’, ‘DEF’]
fantasy_players = []

```
for player_id, player_info in players_data.items():
    # Skip if no position or not fantasy relevant
    if not player_info.get('position') or player_info['position'] not in relevant_positions:
        continue
        
    # Skip if player is inactive, injured reserve, etc.
    status = player_info.get('status', 'Active')
    if status in ['Inactive', 'Reserve/Injured', 'Reserve/PUP']:
        continue
        
    # Only include players with teams
    if not player_info.get('team'):
        continue
        
    fantasy_players.append({
        'sleeper_id': player_id,  # Keep as string to match JSON structure
        'player_name': player_info.get('player_name', ''),
        'position': player_info['position'],
        'team': player_info['team'],
        'status': status,
        'first_name': player_info.get('first_name', ''),
        'last_name': player_info.get('last_name', ''),
        'years_exp': player_info.get('years_exp', 0)
    })

print(f"Filtered to {len(fantasy_players)} fantasy-relevant players")
return fantasy_players
```

def collect_weekly_stats(year=2025, weeks=None):
“”“Collect weekly NFL statistics”””
try:
print(f”Collecting weekly stats for {year}…”)

```
    # Get current week if not specified
    if weeks is None:
        # For now, collect all available weeks
        weekly_stats = nfl.import_weekly_data([year], columns=[
            'player_id', 'player_name', 'player_display_name', 
            'position', 'position_group', 'team', 'week',
            'completions', 'attempts', 'passing_yards', 'passing_tds', 'interceptions',
            'carries', 'rushing_yards', 'rushing_tds', 
            'targets', 'receptions', 'receiving_yards', 'receiving_tds',
            'fantasy_points', 'fantasy_points_ppr'
        ])
    else:
        weekly_stats = nfl.import_weekly_data([year], columns=[
            'player_id', 'player_name', 'player_display_name', 
            'position', 'position_group', 'team', 'week',
            'completions', 'attempts', 'passing_yards', 'passing_tds', 'interceptions',
            'carries', 'rushing_yards', 'rushing_tds', 
            'targets', 'receptions', 'receiving_yards', 'receiving_tds',
            'fantasy_points', 'fantasy_points_ppr'
        ])
        # Filter to specific weeks if provided
        if isinstance(weeks, (list, tuple)):
            weekly_stats = weekly_stats[weekly_stats['week'].isin(weeks)]
        else:
            weekly_stats = weekly_stats[weekly_stats['week'] == weeks]
    
    print(f"Collected {len(weekly_stats)} stat records")
    return weekly_stats
    
except Exception as e:
    print(f"Error collecting weekly stats: {e}")
    return pd.DataFrame()
```

def match_players_to_stats(fantasy_players, weekly_stats):
“”“Match Sleeper players to NFL stats using multiple methods”””
matched_data = []
unmatched_sleeper = []
unmatched_nfl = set(weekly_stats[‘player_name’].unique())

```
# Create lookup dictionaries for better matching
sleeper_by_name = {}
sleeper_by_last_first = {}

for player in fantasy_players:
    full_name = player['player_name'].strip()
    sleeper_by_name[full_name.lower()] = player
    
    # Also try "Last, First" format
    if player.get('first_name') and player.get('last_name'):
        last_first = f"{player['last_name']}, {player['first_name']}"
        sleeper_by_last_first[last_first.lower()] = player

# Match players
for _, stat_row in weekly_stats.iterrows():
    nfl_name = stat_row['player_name']
    nfl_display_name = stat_row.get('player_display_name', nfl_name)
    
    matched_player = None
    match_method = None
    
    # Try exact name match first
    if nfl_name.lower() in sleeper_by_name:
        matched_player = sleeper_by_name[nfl_name.lower()]
        match_method = "exact_name"
    elif nfl_display_name.lower() in sleeper_by_name:
        matched_player = sleeper_by_name[nfl_display_name.lower()]
        match_method = "display_name"
    elif nfl_name.lower() in sleeper_by_last_first:
        matched_player = sleeper_by_last_first[nfl_name.lower()]
        match_method = "last_first"
    
    if matched_player:
        # Remove from unmatched NFL set
        if nfl_name in unmatched_nfl:
            unmatched_nfl.remove(nfl_name)
        
        # Create combined record
        combined_record = {
            'sleeper_id': matched_player['sleeper_id'],
            'nfl_player_id': stat_row.get('player_id', ''),
            'player_name': matched_player['player_name'],
            'nfl_name': nfl_name,
            'position': matched_player['position'],
            'team': matched_player['team'],
            'week': int(stat_row['week']),
            'season': 2025,
            'match_method': match_method,
            
            # Passing stats
            'completions': stat_row.get('completions', 0) or 0,
            'attempts': stat_row.get('attempts', 0) or 0,
            'passing_yards': stat_row.get('passing_yards', 0) or 0,
            'passing_tds': stat_row.get('passing_tds', 0) or 0,
            'interceptions': stat_row.get('interceptions', 0) or 0,
            
            # Rushing stats
            'carries': stat_row.get('carries', 0) or 0,
            'rushing_yards': stat_row.get('rushing_yards', 0) or 0,
            'rushing_tds': stat_row.get('rushing_tds', 0) or 0,
            
            # Receiving stats
            'targets': stat_row.get('targets', 0) or 0,
            'receptions': stat_row.get('receptions', 0) or 0,
            'receiving_yards': stat_row.get('receiving_yards', 0) or 0,
            'receiving_tds': stat_row.get('receiving_tds', 0) or 0,
            
            # Fantasy points
            'fantasy_points': stat_row.get('fantasy_points', 0) or 0,
            'fantasy_points_ppr': stat_row.get('fantasy_points_ppr', 0) or 0,
            
            'last_updated': datetime.now().isoformat()
        }
        
        matched_data.append(combined_record)
    else:
        # Add to unmatched if it's a fantasy-relevant position
        position = stat_row.get('position', '')
        if position in ['QB', 'RB', 'WR', 'TE', 'K']:
            unmatched_sleeper.append({
                'nfl_name': nfl_name,
                'nfl_display_name': nfl_display_name,
                'position': position,
                'team': stat_row.get('team', ''),
                'week': stat_row.get('week', 0)
            })

print(f"Successfully matched {len(matched_data)} player-week records")
print(f"Unmatched NFL players: {len(unmatched_nfl)}")
print(f"Fantasy-relevant unmatched: {len(unmatched_sleeper)}")

return matched_data, list(unmatched_nfl), unmatched_sleeper
```

def save_performance_data(matched_data):
“”“Save performance data to season_2025_performances.json”””
# Load existing data if it exists
performance_file = ‘season_2025_performances.json’
existing_data = []

```
if os.path.exists(performance_file):
    try:
        with open(performance_file, 'r') as f:
            existing_data = json.load(f)
        print(f"Loaded {len(existing_data)} existing performance records")
    except (json.JSONDecodeError, FileNotFoundError):
        print("No existing performance data found, starting fresh")
        existing_data = []

# Create a set of existing records for deduplication (sleeper_id + week)
existing_keys = set()
for record in existing_data:
    key = f"{record.get('sleeper_id', '')}_{record.get('week', 0)}"
    existing_keys.add(key)

# Add new records, avoiding duplicates
new_records = []
duplicates = 0

for record in matched_data:
    key = f"{record['sleeper_id']}_{record['week']}"
    if key not in existing_keys:
        new_records.append(record)
        existing_keys.add(key)
    else:
        duplicates += 1

# Combine and save
all_data = existing_data + new_records

with open(performance_file, 'w') as f:
    json.dump(all_data, f, indent=2, default=str)

print(f"Saved {len(all_data)} total performance records")
print(f"Added {len(new_records)} new records")
print(f"Skipped {duplicates} duplicates")

return len(new_records)
```

def create_weekly_snapshot(matched_data, week_num):
“”“Create a snapshot for a specific week”””
week_data = [record for record in matched_data if record[‘week’] == week_num]

```
if not week_data:
    print(f"No data found for week {week_num}")
    return

snapshot = {
    'week': week_num,
    'season': 2025,
    'generated_at': datetime.now().isoformat(),
    'player_count': len(week_data),
    'performances': week_data
}

# Save individual week snapshot
snapshot_file = f'weekly_snapshots/week_{week_num}_2025.json'
os.makedirs('weekly_snapshots', exist_ok=True)

with open(snapshot_file, 'w') as f:
    json.dump(snapshot, f, indent=2, default=str)

print(f"Created snapshot for week {week_num} with {len(week_data)} players")
```

def main():
“”“Main execution function”””
print(“Starting NFL data collection…”)

```
# Load Sleeper players
players_data = load_players()

# Filter to fantasy-relevant players
fantasy_players = get_fantasy_relevant_players(players_data)

# Collect NFL weekly stats
weekly_stats = collect_weekly_stats(2025)

if weekly_stats.empty:
    print("No NFL stats collected, exiting")
    sys.exit(1)

# Match players to stats
matched_data, unmatched_nfl, unmatched_sleeper = match_players_to_stats(fantasy_players, weekly_stats)

if not matched_data:
    print("No players matched, exiting")
    sys.exit(1)

# Save performance data
new_records = save_performance_data(matched_data)

# Create weekly snapshots for any weeks we have data for
weeks_available = sorted(set(record['week'] for record in matched_data))
print(f"Creating snapshots for weeks: {weeks_available}")

for week in weeks_available:
    create_weekly_snapshot(matched_data, week)

# Print summary
print("\n=== COLLECTION SUMMARY ===")
print(f"Fantasy players processed: {len(fantasy_players)}")
print(f"NFL stat records collected: {len(weekly_stats)}")
print(f"Successfully matched records: {len(matched_data)}")
print(f"New records added: {new_records}")
print(f"Weeks processed: {weeks_available}")

# Show some unmatched for debugging
if unmatched_sleeper:
    print(f"\nTop 10 unmatched NFL players:")
    for player in unmatched_sleeper[:10]:
        print(f"  {player['nfl_name']} ({player['position']}, {player['team']})")
```

if **name** == “**main**”:
main()