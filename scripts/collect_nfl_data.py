import json
import sys

print("Debug script starting")
try:
    with open("players_detailed.json", "r") as f:
        data = json.load(f)
    print(f"Success: Loaded {len(data)}
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
