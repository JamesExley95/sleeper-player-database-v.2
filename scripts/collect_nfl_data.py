import json
import sys
import os

print("Debug script starting")
try:
    with open("players_detailed.json", "r") as f:
        data = json.load(f)
    print(f"Success: Loaded {len(data)} players")
    
    print("Creating data directory...")
    os.makedirs("data", exist_ok=True)
    
    print("Writing players.json...")
    with open("data/players.json", "w") as f:
        json.dump({"metadata": {"total_players": len(data)}, "players": data}, f)
    print("players.json written successfully")
    
    print("Writing weekly_insights.json...")
    with open("data/weekly_insights.json", "w") as f:
        json.dump({"metadata": {"ready_for_stories": False}}, f)
    print("weekly_insights.json written successfully")
    
    print("Writing metadata.json...")
    metadata = {"data_health": {"quality_score": 95, "total_players": len(data)}}
    print(f"Metadata to write: {metadata}")
    with open("data/metadata.json", "w") as f:
        json.dump(metadata, f)
    print("metadata.json written successfully")
    
    print("Verifying files were created...")
    for filename in ["data/players.json", "data/weekly_insights.json", "data/metadata.json"]:
        if os.path.exists(filename):
            size = os.path.getsize(filename)
            print(f"{filename}: {size} bytes")
        else:
            print(f"{filename}: MISSING")
    
    print("Output files created successfully")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
