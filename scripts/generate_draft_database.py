#!/usr/bin/env python3
“””
Generate Draft Database - NFL Draft Analysis
Creates ADP analysis, value picks, and bust warnings for fantasy drafts
Uses historical performance and current projections
“””

import json
import os
import sys
import argparse
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
import statistics
import requests

# Import availability checks

try:
import pandas as pd
PANDAS_AVAILABLE = True
except ImportError:
PANDAS_AVAILABLE = False
pd = None

SCRIPT_VERSION = “Draft_Analysis_v1.0”
CURRENT_SEASON = 2025

# ADP data sources

ADP_SOURCES = {
‘fantasyfootballcalculator’: ‘https://fantasyfootballcalculator.com/api/v1/adp/ppr’,
‘fantasyfootballcalculator_standard’: ‘https://fantasyfootballcalculator.com/api/v1/adp/standard’,
‘fantasyfootballcalculator_half’: ‘https://fantasyfootballcalculator.com/api/v1/adp/half-ppr’
}

def safe_float(value, default=0.0):
“”“Safely convert value to float with fallback”””
try:
if pd and pd.isna(value) or value is None:
return default
return float(value)
except (ValueError, TypeError):
return default

def safe_int(value, default=0):
“”“Safely convert value to int with fallback”””
try:
if pd and pd.isna(value) or value is None:
return default
return int(float(value))
except (ValueError, TypeError):
return default

def load_existing_players() -> Optional[Dict]:
“”“Load the main player database for draft analysis”””
try:
with open(‘data/players.json’, ‘r’) as f:
data = json.load(f)
if ‘players’ not in data:
print(“Error: Invalid players.json structure - missing ‘players’ key”)
return None
return data
except FileNotFoundError:
print(“Error: data/players.json not found - run collect_nfl_data.py first”)
return None
except json.JSONDecodeError as e:
print(f”Error: Invalid JSON in players.json - {e}”)
return None

def load_season_performances() -> Optional[Dict]:
“”“Load season performance data for historical analysis”””
filepath = ‘data/season_2025_performances.json’

```
if not os.path.exists(filepath):
    print(f"Warning: {filepath} not found - using limited analysis")
    return None

try:
    with open(filepath, 'r') as f:
        data = json.load(f)
        if 'players' not in data:
            print("Error: Invalid season performances structure")
            return None
        return data
except json.JSONDecodeError as e:
    print(f"Error: Invalid JSON in season performances - {e}")
    return None
```

def simulate_adp_data(players: List[Dict]) -> Dict:
“””
Simulate ADP data since real APIs require authentication
In production, this would fetch from actual ADP sources
“””
print(“Simulating ADP data (replace with real API calls in production)”)

```
adp_data = {}

# Position-based ADP ranges
position_adp_ranges = {
    'QB': (48, 144),    # QBs typically go rounds 4-12
    'RB': (12, 84),     # RBs go early, rounds 1-7
    'WR': (6, 96),      # WRs throughout, rounds 1-8
    'TE': (36, 120),    # TEs typically rounds 3-10
    'K': (144, 180),    # Kickers very late
    'DEF': (132, 168)   # Defenses late
}

# Create simulated ADP based on position and some randomness
import random
random.seed(42)  # Consistent results for testing

for i, player in enumerate(players):
    position = player.get('position', 'WR')
    if position in position_adp_ranges:
        min_adp, max_adp = position_adp_ranges[position]
        
        # Add some variance based on player quality indicators
        base_adp = random.uniform(min_adp, max_adp)
        
        # Simulate some elite players with better ADP
        if i < 50 and position in ['RB', 'WR']:  # Top 50 skill players
            base_adp = max(6, base_adp * 0.7)  # Push toward earlier rounds
        
        adp_data[player.get('name', '')] = {
            'adp': round(base_adp, 1),
            'min_pick': max(1, round(base_adp - 12)),
            'max_pick': min(200, round(base_adp + 12)),
            'std_dev': round(random.uniform(8, 20), 1),
            'source': 'simulated'
        }

print(f"Generated simulated ADP for {len(adp_data)} players")
return adp_data
```

def fetch_real_adp_data() -> Dict:
“””
Fetch real ADP data from fantasy platforms
This is a placeholder - actual implementation would need API keys
“””
print(“Real ADP fetching not implemented - using simulated data”)
return {}

def calculate_projected_points(player: Dict, performance_data: Optional[Dict] = None) -> Dict:
“”“Calculate projected fantasy points for different scoring formats”””

```
# If we have performance data, use it for projections
if performance_data and player.get('sleeper_id') in performance_data.get('players', {}):
    player_perf = performance_data['players'][player.get('sleeper_id')]
    season_totals = player_perf.get('season_totals', {})
    games_played = season_totals.get('games_played', 1)
    
    if games_played > 0:
        # Project based on per-game averages over 17 games
        averages = season_totals.get('averages', {})
        return {
            'standard': round(averages.get('standard', 0) * 17, 1),
            'half_ppr': round(averages.get('half_ppr', 0) * 17, 1),
            'ppr': round(averages.get('ppr', 0) * 17, 1),
            'confidence': min(100, games_played * 20)  # Higher confidence with more games
        }

# Fallback to position-based projections
position = player.get('position', '')

position_projections = {
    'QB': {'standard': 280, 'half_ppr': 280, 'ppr': 280},
    'RB': {'standard': 180, 'half_ppr': 200, 'ppr': 220},
    'WR': {'standard': 160, 'half_ppr': 180, 'ppr': 200},
    'TE': {'standard': 120, 'half_ppr': 140, 'ppr': 160},
    'K': {'standard': 140, 'half_ppr': 140, 'ppr': 140},
    'DEF': {'standard': 120, 'half_ppr': 120, 'ppr': 120}
}

base_projection = position_projections.get(position, 
                                         {'standard': 100, 'half_ppr': 110, 'ppr': 120})

return {
    'standard': base_projection['standard'],
    'half_ppr': base_projection['half_ppr'],
    'ppr': base_projection['ppr'],
    'confidence': 50  # Low confidence for position-based projections
}
```

def analyze_draft_value(player: Dict, adp: float, projections: Dict) -> Dict:
“”“Analyze if player provides good draft value”””

```
position = player.get('position', '')

# Convert ADP to expected points based on draft position
# This is simplified - real analysis would use VBD (Value Based Drafting)
if adp <= 12:       # Round 1
    expected_points = 250
elif adp <= 24:     # Round 2
    expected_points = 200
elif adp <= 36:     # Round 3
    expected_points = 170
elif adp <= 48:     # Round 4
    expected_points = 150
elif adp <= 72:     # Rounds 5-6
    expected_points = 130
elif adp <= 96:     # Rounds 7-8
    expected_points = 110
elif adp <= 120:    # Rounds 9-10
    expected_points = 95
else:               # Late rounds
    expected_points = 80

# Compare projected points to expected points for draft position
projected_ppr = projections.get('ppr', 0)
value_difference = projected_ppr - expected_points
value_percentage = (value_difference / expected_points * 100) if expected_points > 0 else 0

# Determine value category
if value_percentage >= 15:
    value_tier = 'value'
    tier_description = 'Strong Value Pick'
elif value_percentage >= 8:
    value_tier = 'slight_value'
    tier_description = 'Slight Value'
elif value_percentage <= -15:
    value_tier = 'bust'
    tier_description = 'Bust Risk'
elif value_percentage <= -8:
    value_tier = 'slight_bust'
    tier_description = 'Slight Reach'
else:
    value_tier = 'fair'
    tier_description = 'Fair Value'

return {
    'value_tier': value_tier,
    'tier_description': tier_description,
    'expected_points': expected_points,
    'projected_points': projected_ppr,
    'value_difference': round(value_difference, 1),
    'value_percentage': round(value_percentage, 1),
    'draft_round': (adp - 1) // 12 + 1,
    'confidence_score': projections.get('confidence', 50)
}
```

def create_draft_insights(draft_players: List[Dict]) -> Dict:
“”“Create actionable draft insights and recommendations”””

```
# Categorize players by value tiers
value_picks = [p for p in draft_players if p['analysis']['value_tier'] == 'value']
bust_risks = [p for p in draft_players if p['analysis']['value_tier'] == 'bust']
sleepers = [p for p in draft_players 
           if p['adp']['adp'] > 100 and p['analysis']['value_tier'] in ['value', 'slight_value']]

# Sort by value for recommendations
value_picks.sort(key=lambda x: x['analysis']['value_percentage'], reverse=True)
bust_risks.sort(key=lambda x: x['analysis']['value_percentage'])
sleepers.sort(key=lambda x: x['analysis']['value_percentage'], reverse=True)

# Position scarcity analysis
position_depth = {}
for player in draft_players:
    pos = player['info']['position']
    if pos not in position_depth:
        position_depth[pos] = {'total': 0, 'high_value': 0, 'early_round': 0}
    
    position_depth[pos]['total'] += 1
    if player['analysis']['value_tier'] in ['value', 'slight_value']:
        position_depth[pos]['high_value'] += 1
    if player['adp']['adp'] <= 72:  # First 6 rounds
        position_depth[pos]['early_round'] += 1

# Create strategic insights
strategic_insights = []

# Position scarcity insights
for pos, depth in position_depth.items():
    scarcity_ratio = depth['early_round'] / max(depth['total'], 1)
    if scarcity_ratio < 0.3 and depth['total'] > 5:
        strategic_insights.append({
            'type': 'position_scarcity',
            'message': f"{pos} position is deep - wait on drafting {pos}s until later rounds",
            'position': pos,
            'early_options': depth['early_round'],
            'total_options': depth['total']
        })

return {
    'value_picks': value_picks[:10],  # Top 10 value picks
    'bust_warnings': bust_risks[:10],  # Top 10 bust risks
    'sleepers': sleepers[:15],  # Top 15 sleepers
    'position_depth': position_depth,
    'strategic_insights': strategic_insights,
    'draft_strategy_notes': [
        "Focus on value picks in rounds 3-6 where ADP inefficiencies are common",
        "Avoid bust risks unless they fall significantly below their ADP",
        "Target sleepers in rounds 10+ for upside potential"
    ]
}
```

def create_draft_database(players: List[Dict], performance_data: Optional[Dict] = None, use_real_adp: bool = False) -> Dict:
“”“Create comprehensive draft database”””
print(“Creating draft analysis database…”)

```
# Get ADP data
if use_real_adp:
    adp_data = fetch_real_adp_data()
    if not adp_data:
        print("Failed to fetch real ADP - falling back to simulated")
        adp_data = simulate_adp_data(players)
else:
    adp_data = simulate_adp_data(players)

draft_players = []

for player in players:
    player_name = player.get('name', '')
    
    # Skip players without ADP data (likely not fantasy relevant for drafts)
    if player_name not in adp_data:
        continue
    
    # Calculate projections
    projections = calculate_projected_points(player, performance_data)
    
    # Get ADP info
    adp_info = adp_data[player_name]
    
    # Analyze draft value
    analysis = analyze_draft_value(player, adp_info['adp'], projections)
    
    # Compile player draft profile
    draft_player = {
        'info': {
            'name': player_name,
            'position': player.get('position'),
            'team': player.get('team'),
            'sleeper_id': player.get('sleeper_id')
        },
        'adp': adp_info,
        'projections': projections,
        'analysis': analysis
    }
    
    draft_players.append(draft_player)

# Sort by ADP
draft_players.sort(key=lambda x: x['adp']['adp'])

# Create insights
insights = create_draft_insights(draft_players)

# Compile final database
database = {
    'metadata': {
        'version': SCRIPT_VERSION,
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'season': CURRENT_SEASON,
        'total_players': len(draft_players),
        'scoring_formats': ['standard', 'half_ppr', 'ppr'],
        'adp_source': 'simulated' if not use_real_adp else 'real_apis'
    },
    'players': draft_players,
    'insights': insights,
    'usage_notes': {
        'value_picks': 'Players projected to outperform their ADP by 15%+',
        'bust_warnings': 'Players projected to underperform their ADP by 15%+',
        'sleepers': 'Late-round picks (ADP 100+) with value potential',
        'confidence_scores': 'Based on available performance data (0-100)'
    }
}

print(f"Generated draft database with {len(draft_players)} players")
return database
```

def save_draft_database(database: Dict) -> bool:
“”“Save draft database with validation”””
try:
os.makedirs(‘data’, exist_ok=True)

```
    draft_file = 'data/NFL_draft_database.json'
    temp_file = draft_file + '.tmp'
    
    # Write to temporary file first
    with open(temp_file, 'w') as f:
        json.dump(database, f, indent=2, ensure_ascii=False)
    
    # Validate temporary file
    with open(temp_file, 'r') as f:
        test_load = json.load(f)
        assert 'metadata' in test_load
        assert 'players' in test_load
        assert 'insights' in test_load
        assert isinstance(test_load['players'], list)
    
    # Atomic replacement
    if os.path.exists(draft_file):
        os.replace(temp_file, draft_file)
    else:
        os.rename(temp_file, draft_file)
    
    file_size_kb = os.path.getsize(draft_file) / 1024
    
    print(f"Successfully saved draft database:")
    print(f"  - {draft_file} ({len(database['players'])} players, {file_size_kb:.1f} KB)")
    print(f"  - {len(database['insights']['value_picks'])} value picks identified")
    print(f"  - {len(database['insights']['bust_warnings'])} bust warnings created")
    print(f"  - {len(database['insights']['sleepers'])} sleeper candidates found")
    
    return True
    
except Exception as e:
    # Clean up temporary file
    if 'temp_file' in locals() and os.path.exists(temp_file):
        try:
            os.remove(temp_file)
        except:
            pass
    
    print(f"Error saving draft database: {e}")
    return False
```

def main():
“”“Main execution function”””
parser = argparse.ArgumentParser(description=‘Generate NFL draft analysis database’)
parser.add_argument(’–real-adp’, action=‘store_true’,
help=‘Attempt to fetch real ADP data (requires API access)’)
parser.add_argument(’–verbose’, action=‘store_true’, help=‘Enable verbose output’)

```
args = parser.parse_args()

print(f"=== Draft Database Generator v{SCRIPT_VERSION} ===")
print(f"Season: {CURRENT_SEASON}")
print(f"ADP Source: {'Real APIs' if args.real_adp else 'Simulated'}")
print()

try:
    # Load required data
    print("Loading player data...")
    players_db = load_existing_players()
    if not players_db:
        return False
    
    players_list = players_db.get('players', [])
    if not players_list:
        print("Error: No players found in database")
        return False
    
    print(f"Loaded {len(players_list)} players")
    
    # Load performance data if available
    print("Loading performance data...")
    performance_data = load_season_performances()
    
    # Generate draft database
    database = create_draft_database(players_list, performance_data, args.real_adp)
    
    # Save database
    if save_draft_database(database):
        print(f"\n✅ Draft database generation completed successfully")
        
        # Display summary statistics
        insights = database['insights']
        print(f"\n📊 Draft Analysis Summary:")
        print(f"  Value Picks: {len(insights['value_picks'])}")
        print(f"  Bust Warnings: {len(insights['bust_warnings'])}")
        print(f"  Sleeper Picks: {len(insights['sleepers'])}")
        print(f"  Strategic Insights: {len(insights['strategic_insights'])}")
        
        return True
    else:
        print(f"\n❌ Failed to save draft database")
        return False
        
except Exception as e:
    print(f"\n❌ Draft database generation failed: {str(e)}")
    if args.verbose:
        import traceback
        traceback.print_exc()
    return False
```

if **name** == “**main**”:
success = main()
sys.exit(0 if success else 1)
