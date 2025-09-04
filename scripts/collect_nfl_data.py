import json
import sys

print("Debug script starting")
try:
    with open("players_detailed.json", "r") as f:
        print("File opened successfully")
        raw_content = f.read(500)  # First 500 characters
        print(f"First 500 chars: {raw_content}")
        
    with open("players_detailed.json", "r") as f:
        data = json.load(f)
        print(f"JSON loaded: {type(data)}")
        print(f"Total keys: {len(data)}")
        
        # Show first 5 keys
        sample_keys = list(data.keys())[:5]
        print(f"Sample keys: {sample_keys}")
        
        # Check if it's actually loading the full file
        if len(data) < 1000:
            print("WARNING: Fewer than 1000 players loaded - possible parsing issue")
            
except Exception as e:
    print(f"Error during JSON loading: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
