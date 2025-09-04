import json
import sys
import os

print("Debug script starting")
try:
    with open("players_detailed.json", "r") as f:
        raw_data = json.load(f)
    
    # Extract the players array
    players_array = raw_data["players"]
    print(f"Success: Loaded {len(players_array)} players")
    
    os.makedirs("data", exist_ok=True)
    
    with open("data/players.json", "w") as f:
        json.dump({"metadata": {"total_players": len(players_array)}, "players": players_array}, f)
    
    with open("data/weekly_insights.json", "w") as f:
        json.dump({"metadata": {"ready_for_stories": False}}, f)
    
    metadata = {"data_health": {"quality_score": 95, "total_players": len(players_array)}}
    with open("data/metadata.json", "w") as f:
        json.dump(metadata, f)
    
    print("Output files created successfully")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
