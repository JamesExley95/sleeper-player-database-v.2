#!/usr/bin/env python3

import json
import os
import sys
from datetime import datetime

# Import with error handling

try:
import pandas as pd
import nfl_data_py as nfl
except ImportError as e:
print(f"Required module not available: {e}")
sys.exit(1)

def load_sleeper_players():
"""Load and process Sleeper player database"""
try:
with open("players_detailed.json", "r") as f:
raw_data = json.load(f)

'''
    if "players" not in raw_data:
        print("Error: Invalid players_detailed.json structure - missing 'players' key")
        sys.exit(1)
        
    players_array = raw_data["players"]
    print(f"Loaded {len(players_array)} total players from Sleeper database")
    return players_array
    
except FileNotFoundError:
    print("Error: players_detailed.json not found")
    sys.exit(1)
except json.JSONDecodeError as e:
    print(f"Error parsing player data: {e}")
    sys.exit(1)
'''

def load_adp_data():
"""Load ADP data created by generate_draft_database.py"""
try:
with open("adp_database.json", "r") as f:
adp_data = json.load(f)

'''
    adp_players = adp_data.get("players", {})
    print(f"Loaded ADP data for {len(adp_players)} players")
    return adp_data, adp_players
    
except FileNotFoundError:
    print("Warning: adp_database.json not found - ADP analysis will be skipped")
    return None, {}
except json.JSONDecodeError as e:
    print(f"Error parsing ADP data: {e}")
    return None, {}
'''

def filter_fantasy_players(players_array):
"""Filter to fantasy-relevant players with active status"""
fantasy_positions = ["QB", "RB", "WR", "TE", "K", "DEF"]
inactive_statuses = ["Inactive", "Reserve/Injured", "Reserve/PUP", "Suspended"]

'''
fantasy_players = []

for player in players_array:
    if not isinstance(player, dict):
        continue
        
    position = player.get("position")
    team = player.get("team")
    status = player.get("status", "Active")
    
    # Must have fantasy-relevant position
    if position not in fantasy_positions:
        continue
        
    # Must have active NFL team
    if not team:
        continue
        
    # Must not be inactive/injured
    if status in inactive_statuses:
        continue
        
    # Ensure required fields exist
    if not player.get("player_id") or not player.get("name"):
        continue
        
    # Convert to consistent format for NFL matching
    fantasy_player = {
        "sleeper_id": str(player["player_id"]),
        "player_name": str(player["name"]),
        "first_name": str(player.get("first_name", "")),
        "last_name": str(player.get("last_name", "")),
        "position": str(position),
        "team": str(team),
        "status": str(status),
        "years_exp": int(player.get("years_exp", 0)),
        "height": str(player.get("height", "")),
        "weight": str(player.get("weight", ""))
    }
    
    fantasy_players.append(fantasy_player)

print(f"Filtered to {len(fantasy_players)} fantasy-relevant players")
return fantasy_players
'''

def match_players_to_adp(fantasy_players, adp_players):
"""Match Sleeper players to ADP data"""
if not adp_players:
print("No ADP data available for matching")
return fantasy_players

'''
matched_count = 0
adp_lookup = {}

# Create lookup from ADP data (try multiple name formats)
for adp_id, adp_player in adp_players.items():
    if not isinstance(adp_player, dict):
        continue
        
    name = str(adp_player.get("name", "")).lower().strip()
    if name:
        adp_lookup[name] = adp_player
        
        # Also try "First Last" to "Last, First" conversion
        name_parts = name.split()
        if len(name_parts) >= 2:
            last_first = f"{name_parts[-1]}, {' '.join(name_parts[:-1])}"
            adp_lookup[last_first] = adp_player

# Match players
for player in fantasy_players:
    player_name = str(player["player_name"]).lower().strip()
    
    # Try exact match first
    adp_match = adp_lookup.get(player_name)
    
    # Try partial matching if no exact match
    if not adp_match:
        for adp_name, adp_player in adp_lookup.items():
            # Simple fuzzy matching - check if names have common words
            player_words = set(player_name.split())
            adp_words = set(adp_name.split())
            
            # If they share at least 2 words (first+last usually), consider it a match
            if len(player_words & adp_words) >= 2:
                adp_match = adp_player
                break
    
    # Add ADP data to player if found
    if adp_match and isinstance(adp_match, dict):
        player["adp_data"] = {
            "standard": adp_match.get("standard", {}),
            "half_ppr": adp_match.get("half_ppr", {}),
            "ppr": adp_match.get("ppr", {})
        }
        matched_count += 1
    else:
        player["adp_data"] = None

print(f"Matched {matched_count} players to ADP data")
return fantasy_players
'''

def get_adp_tier(adp_position):
"""Classify ADP into tiers"""
try:
adp_num = float(adp_position)
if adp_num <= 12:
return "Elite (Rounds 1-2)"
elif adp_num <= 36:
return "High-End (Rounds 3-6)"
elif adp_num <= 60:
return "Mid-Tier (Rounds 7-10)"
elif adp_num <= 100:
return "Late Round (Rounds 11-16)"
else:
return "Waiver Wire"
except (ValueError, TypeError):
return "Unknown"

def determine_value_status(position, avg_standard, avg_ppr, standard_adp, ppr_adp):
"""Determine if player is outperforming, meeting, or underperforming ADP"""
try:
primary_avg = float(avg_standard)
primary_adp = float(standard_adp)
except (ValueError, TypeError):
return "Unknown"

'''
# Position-based performance thresholds (points per game)
position_thresholds = {
    "QB": {"elite": 20, "good": 15, "average": 12, "poor": 8},
    "RB": {"elite": 15, "good": 12, "average": 9, "poor": 6},
    "WR": {"elite": 15, "good": 12, "average": 9, "poor": 6},
    "TE": {"elite": 12, "good": 9, "average": 7, "poor": 4},
    "K": {"elite": 10, "good": 8, "average": 6, "poor": 4}
}

thresholds = position_thresholds.get(position, position_thresholds["RB"])

# Determine expected performance based on ADP
if primary_adp <= 12:  # Rounds 1-2
    expected = thresholds["elite"]
elif primary_adp <= 36:  # Rounds 3-6
    expected = thresholds["good"]
elif primary_adp <= 60:  # Rounds 7-10
    expected = thresholds["average"]
else:  # Late rounds
    expected = thresholds["poor"]

# Compare actual vs expected (avoid division by zero)
if expected <= 0:
    return "Unknown"

performance_ratio = primary_avg / expected

if performance_ratio >= 1.2:
    return "Draft Steal"
elif performance_ratio >= 0.8:
    return "Meeting Expectations"
else:
    return "Underperforming"
'''

def calculate_draft_grade(value_status, games_played):
"""Calculate overall draft grade A-F"""
try:
games = int(games_played)
if games < 3:
return "Incomplete"
except (ValueError, TypeError):
return "Incomplete"

'''
grade_map = {
    "Draft Steal": "A",
    "Meeting Expectations": "B", 
    "Underperforming": "D"
}

return grade_map.get(value_status, "C")
'''

def calculate_adp_value_metrics(player, performance_records):
"""Calculate value metrics comparing performance to ADP expectations"""
adp_data = player.get("adp_data")
if not adp_data or not isinstance(adp_data, dict):
return {}

'''
# Get ADP positions for different formats
standard_adp = None
ppr_adp = None

try:
    standard_data = adp_data.get("standard", {})
    ppr_data = adp_data.get("ppr", {})
    
    if isinstance(standard_data, dict):
        standard_adp = standard_data.get("adp")
    if isinstance(ppr_data, dict):
        ppr_adp = ppr_data.get("adp")
        
    if standard_adp is None:
        return {}
        
    standard_adp = float(standard_adp)
    if ppr_adp is not None:
        ppr_adp = float(ppr_adp)
        
except (ValueError, TypeError):
    return {}

# Calculate season performance averages
if not performance_records:
    return {
        "adp_tier": get_adp_tier(standard_adp), 
        "value_status": "unknown"
    }

total_games = len(performance_records)
total_standard_points = 0
total_ppr_points = 0

# Safely sum performance points
for record in performance_records:
    try:
        total_standard_points += float(record.get("fantasy_points", 0))
        total_ppr_points += float(record.get("fantasy_points_ppr", 0))
    except (ValueError, TypeError):
        continue

avg_standard = total_standard_points / total_games if total_games > 0 else 0
avg_ppr = total_ppr_points / total_games if total_games > 0 else 0

# Determine value status based on performance vs ADP expectations
adp_tier = get_adp_tier(standard_adp)
value_status = determine_value_status(
    player["position"], avg_standard, avg_ppr, standard_adp, ppr_adp
)

return {
    "adp_standard": standard_adp,
    "adp_ppr": ppr_adp,
    "adp_tier": adp_tier,
    "avg_points_standard": round(avg_standard, 2),
    "avg_points_ppr": round(avg_ppr, 2),
    "games_played": total_games,
    "value_status": value_status,
    "draft_grade": calculate_draft_grade(value_status, total_games)
}
'''

def collect_nfl_stats(year=2025, weeks=None):
"""Collect NFL weekly statistics via nfl_data_py"""
try:
print(f"Attempting to collect NFL stats for {year}…")

'''
    # Define required columns
    stat_columns = [
        "player_id", "player_name", "player_display_name", 
        "position", "position_group", "team", "week", "season",
        "completions", "attempts", "passing_yards", "passing_tds", "interceptions",
        "carries", "rushing_yards", "rushing_tds", 
        "targets", "receptions", "receiving_yards", "receiving_tds",
        "fantasy_points", "fantasy_points_ppr"
    ]
    
    # Attempt data collection
    weekly_stats = nfl.import_weekly_data([year], columns=stat_columns)
    
    # Check if data was returned
    if weekly_stats is None or weekly_stats.empty:
        print(f"No data returned for {year}")
        return pd.DataFrame()
    
    # Filter to specific weeks if requested
    if weeks is not None:
        if isinstance(weeks, (list, tuple)):
            weekly_stats = weekly_stats[weekly_stats["week"].isin(weeks)]
        else:
            weekly_stats = weekly_stats[weekly_stats["week"] == weeks]
    
    print(f"Successfully collected {len(weekly_stats)} NFL stat records")
    return weekly_stats
    
except Exception as e:
    print(f"NFL data collection failed: {e}")
    print(f"This is expected for {year} if season hasn't started yet")
    return pd.DataFrame()
'''

def safe_int_conversion(value, default=0):
"""Safely convert value to int"""
try:
if value is None or value == "":
return default
return int(float(value))
except (ValueError, TypeError):
return default

def safe_float_conversion(value, default=0.0):
"""Safely convert value to float"""
try:
if value is None or value == "":
return default
return float(value)
except (ValueError, TypeError):
return default

def match_sleeper_to_nfl(fantasy_players, nfl_stats):
"""Match Sleeper players to NFL statistics using multiple methods"""

'''
if nfl_stats.empty:
    print("No NFL stats to match - creating empty structure")
    return [], [], []

matched_records = []
unmatched_nfl = set(nfl_stats["player_name"].unique())
unmatched_sleeper = []

# Create lookup dictionaries for efficient matching
sleeper_by_name = {}
sleeper_by_parts = {}

for player in fantasy_players:
    name_key = str(player["player_name"]).lower().strip()
    sleeper_by_name[name_key] = player
    
    # Also index by "Last, First" format common in NFL data
    if player["first_name"] and player["last_name"]:
        last_first = f"{player['last_name']}, {player['first_name']}"
        sleeper_by_parts[last_first.lower()] = player

print(f"Matching {len(fantasy_players)} Sleeper players against {len(nfl_stats)} NFL records...")

# Perform matching
for _, nfl_row in nfl_stats.iterrows():
    nfl_name = str(nfl_row.get("player_name", ""))
    nfl_display = str(nfl_row.get("player_display_name", nfl_name))
    
    if not nfl_name:
        continue
        
    matched_player = None
    match_method = None
    
    # Try different matching strategies
    if nfl_name.lower() in sleeper_by_name:
        matched_player = sleeper_by_name[nfl_name.lower()]
        match_method = "exact_name"
    elif nfl_display.lower() in sleeper_by_name:
        matched_player = sleeper_by_name[nfl_display.lower()]
        match_method = "display_name"
    elif nfl_name.lower() in sleeper_by_parts:
        matched_player = sleeper_by_parts[nfl_name.lower()]
        match_method = "last_first"
    
    if matched_player:
        # Remove from unmatched set
        unmatched_nfl.discard(nfl_name)
        
        # Create performance record with safe conversions
        performance_record = {
            "sleeper_id": matched_player["sleeper_id"],
            "nfl_player_id": str(nfl_row.get("player_id", "")),
            "player_name": matched_player["player_name"],
            "nfl_name": nfl_name,
            "position": matched_player["position"],
            "team": matched_player["team"],
            "week": safe_int_conversion(nfl_row.get("week")),
            "season": safe_int_conversion(nfl_row.get("season", 2025)),
            "match_method": match_method,
            
            # Passing statistics
            "completions": safe_int_conversion(nfl_row.get("completions")),
            "attempts": safe_int_conversion(nfl_row.get("attempts")),
            "passing_yards": safe_int_conversion(nfl_row.get("passing_yards")),
            "passing_tds": safe_int_conversion(nfl_row.get("passing_tds")),
            "interceptions": safe_int_conversion(nfl_row.get("interceptions")),
            
            # Rushing statistics
            "carries": safe_int_conversion(nfl_row.get("carries")),
            "rushing_yards": safe_int_conversion(nfl_row.get("rushing_yards")),
            "rushing_tds": safe_int_conversion(nfl_row.get("rushing_tds")),
            
            # Receiving statistics
            "targets": safe_int_conversion(nfl_row.get("targets")),
            "receptions": safe_int_conversion(nfl_row.get("receptions")),
            "receiving_yards": safe_int_conversion(nfl_row.get("receiving_yards")),
            "receiving_tds": safe_int_conversion(nfl_row.get("receiving_tds")),
            
            # Fantasy points
            "fantasy_points": safe_float_conversion(nfl_row.get("fantasy_points")),
            "fantasy_points_ppr": safe_float_conversion(nfl_row.get("fantasy_points_ppr")),
            
            # Include ADP data for value analysis
            "adp_data": matched_player.get("adp_data"),
            
            "last_updated": datetime.now().isoformat()
        }
        
        matched_records.append(performance_record)
    else:
        # Track unmatched NFL players in fantasy positions
        position = str(nfl_row.get("position", ""))
        if position in ["QB", "RB", "WR", "TE", "K"]:
            unmatched_sleeper.append({
                "nfl_name": nfl_name,
                "position": position,
                "team": str(nfl_row.get("team", "")),
                "week": safe_int_conversion(nfl_row.get("week"))
            })

print(f"Successfully matched {len(matched_records)} performance records")
print(f"Unmatched NFL players: {len(unmatched_nfl)}")
print(f"Fantasy-relevant unmatched: {len(unmatched_sleeper)}")

return matched_records, list(unmatched_nfl), unmatched_sleeper
'''

def generate_narrative_insights(fantasy_players, matched_records):
"""Generate narrative insights for story templates"""
insights = {
"draft_analysis": {
"steals": [],
"busts": [],
"surprises": []
},
"value_trends": {
"overperformers": [],
"underperformers": [],
"consistent": []
},
"positional_analysis": {},
"adp_accuracy": {}
}

'''
if not matched_records:
    return insights

# Group performance by player
player_performances = {}
for record in matched_records:
    sleeper_id = record.get("sleeper_id", "")
    if sleeper_id:
        if sleeper_id not in player_performances:
            player_performances[sleeper_id] = []
        player_performances[sleeper_id].append(record)

# Calculate value metrics for each player with performance data
for player in fantasy_players:
    sleeper_id = player.get("sleeper_id", "")
    if not sleeper_id:
        continue
        
    player_records = player_performances.get(sleeper_id, [])
    
    if not player_records:
        continue
        
    value_metrics = calculate_adp_value_metrics(player, player_records)
    if not value_metrics:
        continue
        
    player_summary = {
        "name": player["player_name"],
        "position": player["position"],
        "team": player["team"],
        "metrics": value_metrics
    }
    
    # Categorize based on value status
    value_status = value_metrics.get("value_status")
    if value_status == "Draft Steal":
        insights["draft_analysis"]["steals"].append(player_summary)
    elif value_status == "Underperforming":
        insights["draft_analysis"]["busts"].append(player_summary)
        
    # Track over/underperformers (require at least 3 games)
    games_played = value_metrics.get("games_played", 0)
    try:
        games_played = int(games_played)
    except (ValueError, TypeError):
        games_played = 0
        
    if games_played >= 3:
        if value_status == "Draft Steal":
            insights["value_trends"]["overperformers"].append(player_summary)
        elif value_status == "Underperforming":
            insights["value_trends"]["underperformers"].append(player_summary)
        else:
            insights["value_trends"]["consistent"].append(player_summary)

# Sort by significance (safely handle missing keys)
def safe_sort_key_points(x):
    try:
        return float(x.get("metrics", {}).get("avg_points_standard", 0))
    except (ValueError, TypeError):
        return 0

def safe_sort_key_adp(x):
    try:
        return float(x.get("metrics", {}).get("adp_standard", 999))
    except (ValueError, TypeError):
        return 999

insights["draft_analysis"]["steals"].sort(key=safe_sort_key_points, reverse=True)
insights["draft_analysis"]["busts"].sort(key=safe_sort_key_adp)

print(f"Generated insights: {len(insights['draft_analysis']['steals'])} steals, {len(insights['draft_analysis']['busts'])} busts")
return insights
'''

def save_performance_data(matched_records):
"""Save performance data with deduplication"""
performance_file = "season_2025_performances.json"
existing_records = []

'''
# Load existing data if available
if os.path.exists(performance_file):
    try:
        with open(performance_file, "r") as f:
            existing_data = json.load(f)
            if isinstance(existing_data, list):
                existing_records = existing_data
            elif isinstance(existing_data, dict) and "performances" in existing_data:
                existing_records = existing_data["performances"]
        print(f"Loaded {len(existing_records)} existing performance records")
    except (json.JSONDecodeError, KeyError):
        print("Starting with fresh performance data")
        existing_records = []

# Create deduplication keys (sleeper_id + week)
existing_keys = set()
for record in existing_records:
    if isinstance(record, dict):
        sleeper_id = str(record.get("sleeper_id", ""))
        week = str(record.get("week", 0))
        key = f"{sleeper_id}_{week}"
        existing_keys.add(key)

# Add only new records
new_records = []
duplicates = 0

for record in matched_records:
    if not isinstance(record, dict):
        continue
        
    sleeper_id = str(record.get("sleeper_id", ""))
    week = str(record.get("week", 0))
    key = f"{sleeper_id}_{week}"
    
    if key not in existing_keys:
        new_records.append(record)
        existing_keys.add(key)
    else:
        duplicates += 1

# Combine and save all data
all_records = existing_records + new_records

# Save with metadata wrapper
output_data = {
    "metadata": {
        "season": 2025,
        "last_updated": datetime.now().isoformat(),
        "total_records": len(all_records),
        "new_records_added": len(new_records),
        "includes_adp_analysis": True
    },
    "performances": all_records
}

with open(performance_file, "w") as f:
    json.dump(output_data, f, indent=2, default=str)

print(f"Saved {len(all_records)} total performance records")
print(f"Added {len(new_records)} new records, skipped {duplicates} duplicates")

return len(new_records)
'''

def create_weekly_snapshots(matched_records):
"""Create individual week snapshots for Pipedream consumption"""
if not matched_records:
print("No performance data to snapshot")
return

'''
os.makedirs("weekly_snapshots", exist_ok=True)

# Group by week
weeks_data = {}
for record in matched_records:
    if not isinstance(record, dict):
        continue
        
    week = record.get("week")
    try:
        week = int(week)
    except (ValueError, TypeError):
        continue
        
    if week not in weeks_data:
        weeks_data[week] = []
    weeks_data[week].append(record)

# Save individual week files
for week, week_records in weeks_data.items():
    # Calculate ADP-based insights for this week (with safe access)
    draft_steals = []
    high_adp_busts = []
    
    for record in week_records:
        adp_data = record.get("adp_data")
        fantasy_points = safe_float_conversion(record.get("fantasy_points", 0))
        
        if adp_data and fantasy_points > 15:
            draft_steals.append(record)
            
        if (adp_data and isinstance(adp_data, dict) and 
            isinstance(adp_data.get("standard"), dict)):
            try:
                adp_val = float(adp_data["standard"].get("adp", 999))
                if adp_val <= 36 and fantasy_points < 8:
                    high_adp_busts.append(record)
            except (ValueError, TypeError):
                pass
    
    # Create snapshot with safe data access
    snapshot = {
        "week": week,
        "season": 2025,
        "generated_at": datetime.now().isoformat(),
        "player_count": len(week_records),
        "performances": week_records,
        "adp_insights": {
            "potential_steals": len(draft_steals),
            "high_pick_disappointments": len(high_adp_busts)
        },
        "summary": {
            "positions": {},
            "teams": {},
            "top_scorers": []
        }
    }
    
    # Add position/team summaries and top scorers with safe access
    valid_records = []
    for record in week_records:
        if isinstance(record, dict):
            pos = str(record.get("position", ""))
            team = str(record.get("team", ""))
            
            if pos:
                snapshot["summary"]["positions"][pos] = snapshot["summary"]["positions"].get(pos, 0) + 1
            if team:
                snapshot["summary"]["teams"][team] = snapshot["summary"]["teams"].get(team, 0) + 1
            
            valid_records.append(record)
    
    # Sort top scorers safely
    def safe_ppr_sort(record):
        try:
            return float(record.get("fantasy_points_ppr", 0))
        except (ValueError, TypeError):
            return 0
    
    snapshot["summary"]["top_scorers"] = sorted(
        valid_records, key=safe_ppr_sort, reverse=True
    )[:10]
    
    snapshot_file = f"weekly_snapshots/week_{week}_2025.json"
    with open(snapshot_file, "w") as f:
        json.dump(snapshot, f, indent=2, default=str)
    
    print(f"Created snapshot for Week {week}: {len(week_records)} players")
'''

def create_output_files(fantasy_players, matched_records, narrative_insights):
"""Create the data files expected by the workflow"""
os.makedirs("data", exist_ok=True)

'''
# Calculate metrics for reporting
total_players = len(fantasy_players)
unique_performers = set()
players_with_adp = 0

for record in matched_records:
    if isinstance(record, dict) and record.get("sleeper_id"):
        unique_performers.add(record["sleeper_id"])

for player in fantasy_players:
    if isinstance(player, dict) and player.get("adp_data"):
        players_with_adp += 1

players_with_data = len(unique_performers)
data_coverage = (players_with_data / total_players * 100) if total_players > 0 else 0
adp_coverage = (players_with_adp / total_players * 100) if total_players > 0 else 0

# data/players.json - Full player database with ADP data
players_data = {
    "metadata": {
        "total_players": total_players,
        "fantasy_relevant": total_players,
        "players_with_adp": players_with_adp,
        "last_updated": datetime.now().isoformat(),
        "data_coverage_pct": round(data_coverage, 1),
        "adp_coverage_pct": round(adp_coverage, 1)
    },
    "players": fantasy_players
}

with open("data/players.json", "w") as f:
    json.dump(players_data, f, indent=2)

# data/weekly_insights.json - Analysis for story generation with ADP insights
weeks_available = []
for record in matched_records:
    if isinstance(record, dict):
        week = record.get("week")
        try:
            week = int(week)
            if week not in weeks_available:
                weeks_available.append(week)
        except (ValueError, TypeError):
            pass

weeks_available.sort()

# Safe access to narrative insights
'''