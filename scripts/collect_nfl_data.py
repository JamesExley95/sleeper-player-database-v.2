#!/usr/bin/env python3

import json
import os
import sys
import traceback
from datetime import datetime

def debug_environment():
print(”=== ENVIRONMENT DEBUG ===”)
print(f”Python version: {sys.version}”)
print(f”Working directory: {os.getcwd()}”)
print(f”Python path: {sys.path[:3]}”)

```
# Check available modules
modules_to_check = ["pandas", "nfl_data_py", "json", "datetime"]
for module in modules_to_check:
    try:
        __import__(module)
        print(f"Module {module}: AVAILABLE")
    except ImportError as e:
        print(f"Module {module}: MISSING - {e}")
```

def debug_file_structure():
print(”\n=== FILE STRUCTURE DEBUG ===”)

```
# List all files in current directory
print("Current directory contents:")
for item in sorted(os.listdir(".")):
    if os.path.isfile(item):
        size = os.path.getsize(item)
        print(f"  FILE: {item} ({size} bytes)")
    else:
        print(f"  DIR:  {item}/")

# Check for expected files
expected_files = ["players_detailed.json", "requirements.txt", "README.md"]
for filename in expected_files:
    if os.path.exists(filename):
        size = os.path.getsize(filename)
        print(f"Expected file {filename}: EXISTS ({size} bytes)")
    else:
        print(f"Expected file {filename}: MISSING")
```

def debug_players_file():
print(”\n=== PLAYERS FILE DEBUG ===”)

```
if not os.path.exists("players_detailed.json"):
    print("ERROR: players_detailed.json not found")
    return None

try:
    # Check file encoding and content
    with open("players_detailed.json", "rb") as f:
        raw_bytes = f.read(100)  # First 100 bytes
        print(f"First 100 bytes: {raw_bytes}")
    
    # Try to load JSON
    with open("players_detailed.json", "r", encoding="utf-8") as f:
        players_data = json.load(f)
    
    print(f"JSON loaded successfully: {type(players_data)}")
    print(f"Total entries: {len(players_data)}")
    
    # Sample a few entries
    sample_keys = list(players_data.keys())[:3]
    print(f"Sample keys: {sample_keys}")
    
    for key in sample_keys:
        player = players_data[key]
        print(f"  {key}: {player.get('player_name', 'NO_NAME')} ({player.get('position', 'NO_POS')}, {player.get('team', 'NO_TEAM')})")
    
    return players_data
    
except json.JSONDecodeError as e:
    print(f"JSON decode error: {e}")
    return None
except UnicodeDecodeError as e:
    print(f"Unicode decode error: {e}")
    return None
except Exception as e:
    print(f"Unexpected error: {e}")
    traceback.print_exc()
    return None
```

def debug_player_filtering(players_data):
print(”\n=== PLAYER FILTERING DEBUG ===”)

```
if not players_data:
    print("No players data to analyze")
    return

# Analyze positions
positions = {}
teams = {}
statuses = {}
missing_data = {"no_position": 0, "no_team": 0, "no_name": 0}

for player_id, player_info in players_data.items():
    # Count positions
    pos = player_info.get("position", "MISSING")
    positions[pos] = positions.get(pos, 0) + 1
    
    # Count teams
    team = player_info.get("team", "MISSING")
    teams[team] = teams.get(team, 0) + 1
    
    # Count statuses
    status = player_info.get("status", "MISSING")
    statuses[status] = statuses.get(status, 0) + 1
    
    # Count missing data
    if not player_info.get("position"):
        missing_data["no_position"] += 1
    if not player_info.get("team"):
        missing_data["no_team"] += 1
    if not player_info.get("player_name"):
        missing_data["no_name"] += 1

print("Position distribution:")
for pos, count in sorted(positions.items(), key=lambda x: x[1], reverse=True)[:10]:
    print(f"  {pos}: {count}")

print("Team distribution (top 10):")
for team, count in sorted(teams.items(), key=lambda x: x[1], reverse=True)[:10]:
    print(f"  {team}: {count}")

print("Status distribution:")
for status, count in sorted(statuses.items(), key=lambda x: x[1], reverse=True):
    print(f"  {status}: {count}")

print("Missing data:")
for field, count in missing_data.items():
    print(f"  {field}: {count}")

# Calculate fantasy-relevant players
relevant_positions = ["QB", "RB", "WR", "TE", "K", "DEF"]
fantasy_count = 0
for player_id, player_info in players_data.items():
    if (player_info.get("position") in relevant_positions and 
        player_info.get("team") and 
        player_info.get("status", "Active") not in ["Inactive", "Reserve/Injured", "Reserve/PUP"]):
        fantasy_count += 1

print(f"\nFantasy-relevant players (estimated): {fantasy_count}")
```

def debug_nfl_data():
print(”\n=== NFL DATA DEBUG ===”)

```
try:
    import pandas as pd
    import nfl_data_py as nfl
    print("NFL data modules imported successfully")
    
    # Test data availability for different years
    for year in [2024, 2025]:
        print(f"\nTesting {year} data availability...")
        try:
            # Try to get minimal data
            test_data = nfl.import_weekly_data([year], columns=["player_name", "position", "week"], rows=5)
            print(f"  {year}: {len(test_data)} sample records available")
            
            if not test_data.empty:
                weeks = sorted(test_data["week"].unique())
                print(f"  Available weeks: {weeks}")
                sample_players = test_data["player_name"].head(3).tolist()
                print(f"  Sample players: {sample_players}")
            
        except Exception as e:
            print(f"  {year}: No data available - {e}")
    
except ImportError as e:
    print(f"NFL data modules not available: {e}")
except Exception as e:
    print(f"NFL data test failed: {e}")
    traceback.print_exc()
```

def debug_file_permissions():
print(”\n=== FILE PERMISSIONS DEBUG ===”)

```
# Test write permissions
test_files = ["test_write.json", "season_2025_performances.json", "weekly_snapshots/test.json"]

for test_file in test_files:
    try:
        # Create directory if needed
        dir_path = os.path.dirname(test_file)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
            print(f"Created directory: {dir_path}")
        
        # Test write
        with open(test_file, "w") as f:
            json.dump({"test": "data", "timestamp": datetime.now().isoformat()}, f)
        
        # Test read
        with open(test_file, "r") as f:
            data = json.load(f)
        
        print(f"Write/read test for {test_file}: SUCCESS")
        
        # Clean up
        os.remove(test_file)
        
    except Exception as e:
        print(f"Write/read test for {test_file}: FAILED - {e}")
```

def main():
print(“BYLINE DATA PIPELINE DEBUG SCRIPT”)
print(”=” * 50)
print(f”Timestamp: {datetime.now().isoformat()}”)

```
try:
    debug_environment()
    debug_file_structure()
    players_data = debug_players_file()
    debug_player_filtering(players_data)
    debug_nfl_data()
    debug_file_permissions()
    
    print("\n=== SUMMARY ===")
    print("Debug script completed successfully")
    print("Check output above for any issues that need resolution")
    
except Exception as e:
    print(f"\nDEBUG SCRIPT FAILED: {e}")
    traceback.print_exc()
    sys.exit(1)
```

if **name** == “**main**”:
main()