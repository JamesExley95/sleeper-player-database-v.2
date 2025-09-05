#!/usr/bin/env python3
# scripts/refresh_player_database.py

import json
import requests
import sys
from datetime import datetime

def fetch_sleeper_players():
    """Fetch all NFL players from Sleeper API"""
    try:
        print("Fetching all NFL players from Sleeper API...")
        
        headers = {'User-Agent': 'Byline-Content-MVP/1.0'}
        response = requests.get("https://api.sleeper.app/v1/players/nfl", 
                              headers=headers, timeout=60)
        response.raise_for_status()
        
        all_players = response.json()
        print(f"Successfully fetched {len(all_players):,} players from Sleeper API")
        
        return all_players
        
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to fetch from Sleeper API: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Unexpected error fetching players: {e}")
        sys.exit(1)

def filter_fantasy_players(all_players):
    """Filter to fantasy-relevant players only"""
    
    # Active NFL teams for 2024/2025 season
    active_nfl_teams = {
        "ARI", "ATL", "BAL", "BUF", "CAR", "CHI", "CIN", "CLE", "DAL", "DEN",
        "DET", "GB", "HOU", "IND", "JAX", "KC", "LV", "LAC", "LAR", "MIA",
        "MIN", "NE", "NO", "NYG", "NYJ", "PHI", "PIT", "SEA", "SF", "TB",
        "TEN", "WAS"
    }
    
    # Statuses indicating inactive players
    inactive_statuses = {
        "Inactive", "Reserve/Injured", "Reserve/PUP", "Suspended", "Retired",
        "Reserve/Suspended", "Practice Squad/Injured", "Reserve/Did not report",
        "Reserve/Military", "Reserve/Left squad", "Exempt/Commissioner Permission",
        "Reserve/Non-football injury", "Reserve/Physically Unable to Perform",
        "Reserve/Future", "Reserve/Non-Football Illness"
    }
    
    fantasy_players = []
    exclusion_stats = {
        "no_fantasy_positions": 0,
        "inactive_status": 0,
        "no_team": 0,
        "invalid_team": 0,
        "missing_data": 0,
        "likely_retired": 0,
        "invalid_position": 0
    }
    
    print("Filtering players to fantasy-relevant only...")
    
    for player_id, player_data in all_players.items():
        if not isinstance(player_data, dict):
            continue
            
        # Must have fantasy positions defined by Sleeper
        fantasy_positions = player_data.get("fantasy_positions", [])
        if not fantasy_positions or not isinstance(fantasy_positions, list):
            exclusion_stats["no_fantasy_positions"] += 1
            continue
            
        # Validate position is truly fantasy relevant
        valid_fantasy_positions = {"QB", "RB", "WR", "TE", "K", "DEF"}
        if not any(pos in valid_fantasy_positions for pos in fantasy_positions):
            exclusion_stats["invalid_position"] += 1
            continue
            
        # Must have active status
        status = player_data.get("status", "Active")
        if status in inactive_statuses:
            exclusion_stats["inactive_status"] += 1
            continue
            
        # Must have valid NFL team
        team = player_data.get("team")
        if not team:
            exclusion_stats["no_team"] += 1
            continue
            
        if team not in active_nfl_teams:
            exclusion_stats["invalid_team"] += 1
            continue
            
        # Must have basic required data
        first_name = player_data.get("first_name", "")
        last_name = player_data.get("last_name", "")
        
        if not first_name or not last_name:
            exclusion_stats["missing_data"] += 1
            continue
            
        # Exclude likely retired players (>= 18 years experience)
        years_exp = player_data.get("years_exp", 0)
        try:
            if int(years_exp) >= 18:
                exclusion_stats["likely_retired"] += 1
                continue
        except (ValueError, TypeError):
            pass
            
        # Use Sleeper's search fields for better name matching
        search_full_name = player_data.get("search_full_name", "")
        if not search_full_name:
            # Fallback: construct from first/last name
            search_full_name = f"{first_name}{last_name}".lower().replace(" ", "")
            
        player_name = f"{first_name} {last_name}".strip()
        
        # Create standardized player object
        fantasy_player = {
            "sleeper_id": str(player_id),
            "player_name": player_name,
            "first_name": first_name,
            "last_name": last_name,
            "position": fantasy_positions[0],  # Primary fantasy position
            "fantasy_positions": fantasy_positions,
            "team": team,
            "status": status,
            "years_exp": int(years_exp) if years_exp else 0,
            "height": player_data.get("height", ""),
            "weight": player_data.get("weight", ""),
            "age": player_data.get("age", 0),
            # Include search fields for better matching
            "search_full_name": search_full_name,
            "search_first_name": player_data.get("search_first_name", first_name.lower()),
            "search_last_name": player_data.get("search_last_name", last_name.lower()),
            # Additional useful fields
            "depth_chart_position": player_data.get("depth_chart_position"),
            "number": player_data.get("number"),
            "college": player_data.get("college", "")
        }
        
        fantasy_players.append(fantasy_player)
    
    # Print filtering summary
    total_processed = len(all_players)
    total_kept = len(fantasy_players)
    total_excluded = sum(exclusion_stats.values())
    
    print(f"\nFiltering results:")
    print(f"  Total players processed: {total_processed:,}")
    print(f"  Fantasy relevant players: {total_kept:,}")
    print(f"  Total excluded: {total_excluded:,}")
    print(f"\nExclusion breakdown:")
    for reason, count in exclusion_stats.items():
        if count > 0:
            percentage = (count / total_processed) * 100
            print(f"  {reason}: {count:,} ({percentage:.1f}%)")
    
    return fantasy_players

def save_player_database(fantasy_players):
    """Save filtered players to database file"""
    
    # Create metadata
    metadata = {
        "last_updated": datetime.now().isoformat(),
        "source": "sleeper_api_direct",
        "total_players": len(fantasy_players),
        "refresh_frequency": "monthly",
        "api_endpoint": "https://api.sleeper.app/v1/players/nfl",
        "collection_method": "fantasy_positions_filter",
        "data_version": "1.0"
    }
    
    # Create final database structure
    database = {
        "metadata": metadata,
        "players": fantasy_players
    }
    
    # Save to file
    output_file = "player_database_clean.json"
    
    try:
        with open(output_file, 'w') as f:
            json.dump(database, f, indent=2, default=str)
        
        print(f"\nPlayer database saved successfully!")
        print(f"  File: {output_file}")
        print(f"  Players: {len(fantasy_players):,}")
        print(f"  File size: ~{len(json.dumps(database)) / 1024 / 1024:.1f}MB")
        
    except Exception as e:
        print(f"ERROR: Failed to save player database: {e}")
        sys.exit(1)

def validate_database():
    """Basic validation of the created database"""
    
    try:
        with open("player_database_clean.json", 'r') as f:
            data = json.load(f)
        
        players = data.get("players", [])
        metadata = data.get("metadata", {})
        
        print(f"\nDatabase validation:")
        print(f"  Total players: {len(players):,}")
        print(f"  Last updated: {metadata.get('last_updated', 'Unknown')}")
        
        # Check player distribution by position
        position_counts = {}
        team_counts = {}
        
        for player in players:
            pos = player.get("position", "Unknown")
            team = player.get("team", "Unknown")
            
            position_counts[pos] = position_counts.get(pos, 0) + 1
            team_counts[team] = team_counts.get(team, 0) + 1
        
        print(f"\nPosition distribution:")
        for pos, count in sorted(position_counts.items()):
            print(f"  {pos}: {count}")
        
        print(f"\nTeams represented: {len(team_counts)}")
        
        # Basic sanity checks
        if len(players) < 500:
            print("WARNING: Fewer than 500 players - this seems low")
        if len(players) > 2000:
            print("WARNING: More than 2000 players - this seems high")
        if len(team_counts) < 30:
            print("WARNING: Fewer than 30 teams represented")
            
        print("Validation complete!")
        
    except Exception as e:
        print(f"ERROR: Database validation failed: {e}")
        sys.exit(1)

def main():
    """Main execution function"""
    print("=== SLEEPER PLAYER DATABASE REFRESH ===")
    print(f"Started at: {datetime.now().isoformat()}")
    
    try:
        # Step 1: Fetch all players from Sleeper
        all_players = fetch_sleeper_players()
        
        # Step 2: Filter to fantasy-relevant players
        fantasy_players = filter_fantasy_players(all_players)
        
        # Step 3: Save to database file
        save_player_database(fantasy_players)
        
        # Step 4: Validate the database
        validate_database()
        
        print(f"\n=== REFRESH COMPLETE ===")
        print(f"Finished at: {datetime.now().isoformat()}")
        
    except KeyboardInterrupt:
        print("\nRefresh interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: Refresh failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
