#!/usr/bin/env python3
"""
Generate ADP Database for Byline Content MVP
Fetches ADP data from Fantasy Football Calculator API for all scoring formats
Creates adp_database.json that integrates with the production NFL data collection script
"""

import json
import os
import sys
import requests
from datetime import datetime
import time

# FFC API endpoints for all 3 scoring formats
FFC_API_ENDPOINTS = {
    "standard": "https://fantasyfootballcalculator.com/api/v1/adp/standard",
    "half_ppr": "https://fantasyfootballcalculator.com/api/v1/adp/half-ppr", 
    "ppr": "https://fantasyfootballcalculator.com/api/v1/adp/ppr"
}

def load_sleeper_players():
    """Load Sleeper players for name matching"""
    try:
        with open("players_detailed.json", "r") as f:
            raw_data = json.load(f)
        
        if "players" not in raw_data:
            print("Error: Invalid players_detailed.json structure")
            return None
            
        players_array = raw_data["players"]
        print(f"Loaded {len(players_array)} Sleeper players for matching")
        return players_array
        
    except FileNotFoundError:
        print("Error: players_detailed.json not found")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing Sleeper data: {e}")
        return None

def fetch_ffc_adp_data(format_name):
    """Fetch ADP data from Fantasy Football Calculator API"""
    url = FFC_API_ENDPOINTS.get(format_name)
    if not url:
        print(f"Unknown format: {format_name}")
        return None
    
    try:
        print(f"Fetching {format_name} ADP data from FFC...")
        
        # Add headers to mimic browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Validate response structure
        if not isinstance(data, dict) or "players" not in data:
            print(f"Invalid response structure for {format_name}")
            return None
            
        players_data = data["players"]
        print(f"Successfully fetched {len(players_data)} {format_name} ADP records")
        return players_data
        
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch {format_name} ADP data: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Invalid JSON response for {format_name}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error fetching {format_name} data: {e}")
        return None

def normalize_player_name(name):
    """Normalize player names for matching"""
    if not name:
        return ""
    
    # Convert to lowercase and strip whitespace
    normalized = str(name).lower().strip()
    
    # Remove common suffixes
    suffixes = [" jr.", " sr.", " iii", " ii", " iv"]
    for suffix in suffixes:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)].strip()
    
    # Handle common name variations
    name_replacements = {
        "d.j.": "dj",
        "t.j.": "tj", 
        "j.j.": "jj",
        "d'": "d",
        "'": ""
    }
    
    for old, new in name_replacements.items():
        normalized = normalized.replace(old, new)
    
    return normalized

def create_player_lookup(sleeper_players):
    """Create lookup dictionaries for player matching"""
    name_lookup = {}
    alt_name_lookup = {}
    
    for player in sleeper_players:
        if not isinstance(player, dict):
            continue
            
        player_name = player.get("name", "")
        if not player_name:
            continue
            
        # Primary lookup by normalized name
        normalized_name = normalize_player_name(player_name)
        if normalized_name:
            name_lookup[normalized_name] = player
        
        # Alternative lookups
        first_name = player.get("first_name", "")
        last_name = player.get("last_name", "")
        
        if first_name and last_name:
            # "Last, First" format common in some data sources
            last_first = normalize_player_name(f"{last_name}, {first_name}")
            alt_name_lookup[last_first] = player
            
            # "First Last" format
            first_last = normalize_player_name(f"{first_name} {last_name}")
            alt_name_lookup[first_last] = player
    
    print(f"Created lookup for {len(name_lookup)} primary names, {len(alt_name_lookup)} alternative names")
    return name_lookup, alt_name_lookup

def match_adp_to_sleeper(adp_data, sleeper_lookup, alt_lookup):
    """Match ADP data to Sleeper players"""
    matched_players = {}
    unmatched_adp = []
    
    for adp_player in adp_data:
        if not isinstance(adp_player, dict):
            continue
            
        # Get player name from ADP data
        adp_name = adp_player.get("name", "") or adp_player.get("player_name", "")
        if not adp_name:
            continue
            
        # Get ADP value
        adp_value = adp_player.get("adp") or adp_player.get("average_pick")
        if not adp_value:
            continue
            
        try:
            adp_value = float(adp_value)
        except (ValueError, TypeError):
            continue
        
        # Try to match with Sleeper players
        normalized_adp_name = normalize_player_name(adp_name)
        matched_sleeper = None
        match_method = None
        
        # Primary lookup
        if normalized_adp_name in sleeper_lookup:
            matched_sleeper = sleeper_lookup[normalized_adp_name]
            match_method = "exact"
        elif normalized_adp_name in alt_lookup:
            matched_sleeper = alt_lookup[normalized_adp_name]
            match_method = "alternative"
        else:
            # Fuzzy matching for missed cases
            adp_words = set(normalized_adp_name.split())
            for sleeper_name, sleeper_player in sleeper_lookup.items():
                sleeper_words = set(sleeper_name.split())
                
                # If they share at least 2 significant words
                if len(adp_words & sleeper_words) >= 2 and len(adp_words) >= 2:
                    matched_sleeper = sleeper_player
                    match_method = "fuzzy"
                    break
        
        if matched_sleeper:
            sleeper_name = matched_sleeper.get("name", "")
            if sleeper_name:
                matched_players[sleeper_name] = {
                    "adp": adp_value,
                    "sleeper_data": matched_sleeper,
                    "adp_name": adp_name,
                    "match_method": match_method
                }
        else:
            unmatched_adp.append({
                "name": adp_name,
                "adp": adp_value
            })
    
    print(f"Matched {len(matched_players)} players, {len(unmatched_adp)} unmatched")
    return matched_players, unmatched_adp

def combine_adp_formats(standard_matches, half_ppr_matches, ppr_matches):
    """Combine ADP data from all formats into final structure"""
    combined_players = {}
    
    # Start with all players from any format
    all_player_names = set()
    all_player_names.update(standard_matches.keys())
    all_player_names.update(half_ppr_matches.keys())
    all_player_names.update(ppr_matches.keys())
    
    for player_name in all_player_names:
        player_data = {
            "name": player_name,
            "standard": {},
            "half_ppr": {}, 
            "ppr": {}
        }
        
        # Add standard ADP if available
        if player_name in standard_matches:
            standard_data = standard_matches[player_name]
            player_data["standard"] = {"adp": standard_data["adp"]}
            # Use sleeper data from first available match
            if "sleeper_data" not in player_data:
                player_data["sleeper_data"] = standard_data["sleeper_data"]
        
        # Add half PPR ADP if available
        if player_name in half_ppr_matches:
            half_ppr_data = half_ppr_matches[player_name]
            player_data["half_ppr"] = {"adp": half_ppr_data["adp"]}
            # Use sleeper data if we don't have it yet
            if "sleeper_data" not in player_data:
                player_data["sleeper_data"] = half_ppr_data["sleeper_data"]
        
        # Add PPR ADP if available
        if player_name in ppr_matches:
            ppr_data = ppr_matches[player_name]
            player_data["ppr"] = {"adp": ppr_data["adp"]}
            # Use sleeper data if we don't have it yet
            if "sleeper_data" not in player_data:
                player_data["sleeper_data"] = ppr_data["sleeper_data"]
        
        combined_players[player_name] = player_data
    
    print(f"Combined data for {len(combined_players)} players across all formats")
    return combined_players

def save_adp_database(combined_players):
    """Save ADP database in the format expected by production script"""
    
    # Create the structure expected by collect_nfl_data.py
    adp_database = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "source": "Fantasy Football Calculator API",
            "formats": ["standard", "half_ppr", "ppr"],
            "total_players": len(combined_players),
            "version": "1.0"
        },
        "players": {}
    }
    
    # Convert to the expected format
    for player_name, player_data in combined_players.items():
        # Use player name as key (matching production script expectation)
        adp_database["players"][player_name] = {
            "name": player_name,
            "standard": player_data.get("standard", {}),
            "half_ppr": player_data.get("half_ppr", {}),
            "ppr": player_data.get("ppr", {})
        }
    
    # Save to root directory (where production script expects it)
    output_file = "adp_database.json"
    
    try:
        with open(output_file, "w") as f:
            json.dump(adp_database, f, indent=2)
        
        file_size_kb = os.path.getsize(output_file) / 1024
        print(f"Successfully saved ADP database:")
        print(f"  - {output_file} ({file_size_kb:.1f} KB)")
        print(f"  - {len(adp_database['players'])} players with ADP data")
        
        # Show format coverage
        standard_count = sum(1 for p in adp_database["players"].values() if p.get("standard", {}).get("adp"))
        half_ppr_count = sum(1 for p in adp_database["players"].values() if p.get("half_ppr", {}).get("adp"))
        ppr_count = sum(1 for p in adp_database["players"].values() if p.get("ppr", {}).get("adp"))
        
        print(f"  - Standard ADP: {standard_count} players")
        print(f"  - Half PPR ADP: {half_ppr_count} players")
        print(f"  - PPR ADP: {ppr_count} players")
        
        return True
        
    except Exception as e:
        print(f"Error saving ADP database: {e}")
        return False

def main():
    """Main execution function"""
    print("=== ADP Database Generator for Byline Content ===")
    print("Fetching data from Fantasy Football Calculator API...")
    print()
    
    try:
        # Load Sleeper players for matching
        sleeper_players = load_sleeper_players()
        if not sleeper_players:
            print("Cannot proceed without Sleeper player data")
            return False
        
        # Create lookup dictionaries
        sleeper_lookup, alt_lookup = create_player_lookup(sleeper_players)
        
        # Fetch ADP data for all formats
        format_matches = {}
        
        for format_name in ["standard", "half_ppr", "ppr"]:
            print(f"\nProcessing {format_name} format...")
            
            # Fetch ADP data
            adp_data = fetch_ffc_adp_data(format_name)
            if not adp_data:
                print(f"Warning: Could not fetch {format_name} ADP data")
                format_matches[format_name] = {}
                continue
            
            # Add small delay between API calls to be respectful
            time.sleep(1)
            
            # Match to Sleeper players
            matches, unmatched = match_adp_to_sleeper(adp_data, sleeper_lookup, alt_lookup)
            format_matches[format_name] = matches
            
            if unmatched and len(unmatched) < 20:  # Show first few unmatched
                print(f"Sample unmatched {format_name} players:")
                for player in unmatched[:5]:
                    print(f"  - {player['name']} (ADP: {player['adp']})")
        
        # Combine all formats
        print(f"\nCombining data from all formats...")
        combined_players = combine_adp_formats(
            format_matches.get("standard", {}),
            format_matches.get("half_ppr", {}),
            format_matches.get("ppr", {})
        )
        
        if not combined_players:
            print("No ADP data successfully processed")
            return False
        
        # Save database
        if save_adp_database(combined_players):
            print(f"\nADP database generation completed successfully!")
            print(f"Your production script can now use ADP analysis features.")
            return True
        else:
            print(f"\nFailed to save ADP database")
            return False
            
    except Exception as e:
        print(f"ADP database generation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
