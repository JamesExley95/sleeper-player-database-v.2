#!/usr/bin/env python3

import json
import pandas as pd
import nfl_data_py as nfl
from datetime import datetime
import os
import sys

def load_players():
try:
with open(“players_detailed.json”, “r”) as f:
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
relevant_positions = [“QB”, “RB”, “WR”, “TE”, “K”, “DEF”]
fantasy_players = []

```
for player_id, player_info in players_data.items():
    if not player_info.get("position") or player_info["position"] not in relevant_positions:
        continue
        
    status = player_info.get("status", "Active")
    if status in ["Inactive", "Reserve/Injured", "Reserve/PUP"]:
        continue
        
    if not player_info.get("team"):
        continue
        
    fantasy_players.append({
        "sleeper_id": player_id,
        "player_name": player_info.get("player_name", ""),
        "position": player_info["position"],
        "team": player_info["team"],
        "status": status,
        "first_name": player_info.get("first_name", ""),
        "last_name": player_info.get("last_name", ""),
        "years_exp": player_info.get("years_exp", 0)
    })

print(f"Filtered to {len(fantasy_players)} fantasy-relevant players")
return fantasy_players
```

def collect_weekly_stats(year=2025, weeks=None):
try:
print(f”Collecting weekly stats for {year}…”)

```
    weekly_stats = nfl.import_weekly_data([year], columns=[
        "player_id", "player_name", "player_display_name", 
        "position", "position_group", "team", "week",
        "completions", "attempts", "passing_yards", "passing_tds", "interceptions",
        "carries", "rushing_yards", "rushing_tds", 
        "targets", "receptions", "receiving_yards", "receiving_tds",
        "fantasy_points", "fantasy_points_ppr"
    ])
    
    if weeks is not None:
        if isinstance(weeks, (list, tuple)):
            weekly_stats = weekly_stats[weekly_stats["week"].isin(weeks)]
        else:
            weekly_stats = weekly_stats[weekly_stats["week"] == weeks]
    
    print(f"Collected {len(weekly_stats)} stat records")
    return weekly_stats
    
except Exception as e:
    print(f"Error collecting weekly stats: {e}")
    return pd.DataFrame()
```

def main():
print(“Starting NFL data collection…”)

```
players_data = load_players()
fantasy_players = get_fantasy_relevant_players(players_data)
weekly_stats = collect_weekly_stats(2025)

if weekly_stats.empty:
    print("No NFL stats collected - likely no 2025 data available yet")
    print("Creating empty performance file for future use")
    
    empty_data = {
        "metadata": {
            "season": 2025,
            "last_updated": datetime.now().isoformat(),
            "total_records": 0,
            "message": "No 2025 NFL data available yet"
        },
        "performances": []
    }
    
    with open("season_2025_performances.json", "w") as f:
        json.dump(empty_data, f, indent=2)
        
    print("Empty performance file created successfully")
    return

print("Data collection completed successfully")
```

if **name** == “**main**”:
main()