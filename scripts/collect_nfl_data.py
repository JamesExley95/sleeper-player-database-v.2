#!/usr/bin/env python3

import json
import os
import sys
import requests
from datetime import datetime, timedelta

# Import with error handling
try:
    import pandas as pd
    import nfl_data_py as nfl
except ImportError as e:
    print(f"Required module not available: {e}")
    sys.exit(1)

def load_sleeper_players():
    """Load players from cached database with fallback to API"""
    
    cached_file = "player_database_clean.json"
    
    # Try to load from cached database first
    if os.path.exists(cached_file):
        try:
            with open(cached_file, "r") as f:
                database = json.load(f)
            
            players_list = database.get("players", [])
            metadata = database.get("metadata", {})
            
            # Check cache age
            last_updated = metadata.get("last_updated", "")
            if last_updated:
                try:
                    updated_date = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                    age_days = (datetime.now() - updated_date).days
                    
                    if age_days > 35:
                        print(f"WARNING: Player database is {age_days} days old - consider running monthly refresh")
                    else:
                        print(f"Using cached player database ({age_days} days old)")
                        
                except Exception:
                    print("Using cached player database (age unknown)")
            
            print(f"Loaded {len(players_list)} players from cached database")
            return players_list
            
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Error reading cached database: {e}")
            print("Falling back to direct API call...")
            
    else:
        print("No cached player database found - falling back to direct API call")
    
    # Fallback: Direct API call
    try:
        print("Fetching players directly from Sleeper API...")
        headers = {'User-Agent': 'Byline-Content-MVP/1.0'}
        response = requests.get("https://api.sleeper.app/v1/players/nfl", 
                              headers=headers, timeout=30)
        response.raise_for_status()
        
        all_players = response.json()
        print(f"Fetched {len(all_players)} players from API")
        
        # Apply same filtering as refresh script
        filtered_players = filter_sleeper_players_inline(all_players)
        print(f"Filtered to {len(filtered_players)} fantasy-relevant players")
        
        return filtered_players
        
    except Exception as e:
        print(f"CRITICAL ERROR: Both cached database and API fallback failed: {e}")
        sys.exit(1)

def filter_sleeper_players_inline(all_players):
    """Inline filtering function for API fallback"""
    
    active_teams = {
        "ARI", "ATL", "BAL", "BUF", "CAR", "CHI", "CIN", "CLE", "DAL", "DEN",
        "DET", "GB", "HOU", "IND", "JAX", "KC", "LV", "LAC", "LAR", "MIA",
        "MIN", "NE", "NO", "NYG", "NYJ", "PHI", "PIT", "SEA", "SF", "TB",
        "TEN", "WAS"
    }
    
    inactive_statuses = {
        "Inactive", "Reserve/Injured", "Reserve/PUP", "Suspended", "Retired"
    }
    
    filtered = []
    
    for player_id, player_data in all_players.items():
        if not isinstance(player_data, dict):
            continue
            
        fantasy_positions = player_data.get("fantasy_positions", [])
        if not fantasy_positions:
            continue
            
        valid_positions = {"QB", "RB", "WR", "TE", "K", "DEF"}
        if not any(pos in valid_positions for pos in fantasy_positions):
            continue
            
        team = player_data.get("team")
        if not team or team not in active_teams:
            continue
            
        status = player_data.get("status", "Active")
        if status in inactive_statuses:
            continue
            
        first_name = player_data.get("first_name", "")
        last_name = player_data.get("last_name", "")
        if not first_name or not last_name:
            continue
            
        # Convert to expected format
        player = {
            "sleeper_id": str(player_id),
            "player_name": f"{first_name} {last_name}",
            "first_name": first_name,
            "last_name": last_name,
            "position": fantasy_positions[0],
            "team": team,
            "status": status,
            "years_exp": int(player_data.get("years_exp", 0)),
            "height": player_data.get("height", ""),
            "weight": player_data.get("weight", ""),
            "search_full_name": player_data.get("search_full_name", "").lower()
        }
        
        filtered.append(player)
    
    return filtered

def load_adp_data():
    """Collect live ADP data from Fantasy Football Calculator API"""
    print("Collecting live ADP data from Fantasy Football Calculator...")
    
    # API endpoints for different scoring formats
    api_endpoints = {
        "standard": "https://fantasyfootballcalculator.com/api/v1/adp/standard?teams=12&year=2025",
        "ppr": "https://fantasyfootballcalculator.com/api/v1/adp/ppr?teams=12&year=2025",
        "half_ppr": "https://fantasyfootballcalculator.com/api/v1/adp/half-ppr?teams=12&year=2025"
    }
    
    # Fallback URLs to try if primary URLs fail
    fallback_endpoints = {
        "standard": [
            "https://fantasyfootballcalculator.com/api/v1/adp/standard?year=2025",
            "https://fantasyfootballcalculator.com/api/v1/adp/standard?teams=12&year=2024",
            "https://fantasyfootballcalculator.com/api/v1/adp/standard"
        ],
        "ppr": [
            "https://fantasyfootballcalculator.com/api/v1/adp/ppr?year=2025",
            "https://fantasyfootballcalculator.com/api/v1/adp/ppr?teams=12&year=2024",
            "https://fantasyfootballcalculator.com/api/v1/adp/ppr"
        ],
        "half_ppr": [
            "https://fantasyfootballcalculator.com/api/v1/adp/half-ppr?year=2025",
            "https://fantasyfootballcalculator.com/api/v1/adp/half-ppr?teams=12&year=2024",
            "https://fantasyfootballcalculator.com/api/v1/adp/half-ppr"
        ]
    }
    
    adp_database = {
        "last_updated": datetime.now().isoformat(),
        "source": "fantasyfootballcalculator.com",
        "players": {}
    }
    
    successful_formats = 0
    
    for format_name, api_url in api_endpoints.items():
        success = False
        urls_to_try = [api_url] + fallback_endpoints.get(format_name, [])
        
        for attempt, url in enumerate(urls_to_try):
            try:
                print(f"  Fetching {format_name} ADP (attempt {attempt + 1})...")
                
                headers = {'User-Agent': 'Byline-Content-MVP/1.0'}
                response = requests.get(url, headers=headers, timeout=15)
                response.raise_for_status()
                
                api_data = response.json()
                players_data = api_data.get("players", [])
                
                if not players_data:
                    print(f"    ⚠️  {format_name}: No players data returned from {url}")
                    continue
                
                # Process the data
                for player_data in players_data:
                    player_name = player_data.get("name", "").strip()
                    if not player_name:
                        continue
                    
                    # Initialize player entry if first time seeing them
                    if player_name not in adp_database["players"]:
                        adp_database["players"][player_name] = {
                            "name": player_name,
                            "position": player_data.get("position", ""),
                            "team": player_data.get("team", "")
                        }
                    
                    # Add this format's ADP data
                    adp_database["players"][player_name][format_name] = {
                        "adp": float(player_data.get("adp", 999)),
                        "rank": int(player_data.get("rank", 999)),
                        "times_drafted": int(player_data.get("times_drafted", 0))
                    }
                
                successful_formats += 1
                print(f"    ✅ {format_name}: {len(players_data)} players collected from {url}")
                success = True
                break
                
            except requests.exceptions.RequestException as e:
                print(f"    ❌ {format_name}: API request failed ({url}) - {str(e)}")
                continue
            except json.JSONDecodeError as e:
                print(f"    ❌ {format_name}: Invalid JSON response ({url}) - {str(e)}")
                continue
            except Exception as e:
                print(f"    ❌ {format_name}: Unexpected error ({url}) - {str(e)}")
                continue
        
        if not success:
            print(f"    ❌ {format_name}: All URL attempts failed")
    
    # Calculate average ADP across formats for each player
    for player_name, player_data in adp_database["players"].items():
        adp_values = []
        for format_name in ["standard", "ppr", "half_ppr"]:
            format_data = player_data.get(format_name, {})
            if isinstance(format_data, dict):
                adp_val = format_data.get("adp", 999)
                if adp_val < 999:
                    adp_values.append(adp_val)
        
        if adp_values:
            player_data["average_adp"] = round(sum(adp_values) / len(adp_values), 1)
        else:
            player_data["average_adp"] = 999
    
    total_players = len(adp_database["players"])
    print(f"ADP collection complete: {total_players} players across {successful_formats} formats")
    
    if successful_formats > 0:
        # Save ADP database for debugging
        with open("adp_database.json", "w") as f:
            json.dump(adp_database, f, indent=2)
        print(f"ADP database saved to adp_database.json")
        
        return adp_database, adp_database["players"]
    else:
        print("Warning: ADP collection failed - proceeding without ADP data")
        return None, {}

def generate_name_variations(name):
    """Generate comprehensive list of name variations for matching"""
    if not name:
        return []
    
    name = name.strip()
    variations = set()
    
    # Original name
    variations.add(name.lower())
    
    # Remove periods
    no_periods = name.replace(".", "")
    variations.add(no_periods.lower())
    
    # Remove apostrophes  
    no_apostrophes = name.replace("'", "")
    variations.add(no_apostrophes.lower())
    
    # Remove Jr/Sr suffixes
    no_suffix = name
    for suffix in [" jr", " jr.", " sr", " sr.", " ii", " iii", " iv"]:
        no_suffix = no_suffix.replace(suffix, "").replace(suffix.upper(), "")
    variations.add(no_suffix.lower().strip())
    
    # Handle first initial + last name patterns
    name_parts = name.split()
    if len(name_parts) >= 2:
        first = name_parts[0]
        last = " ".join(name_parts[1:])
        
        # "Christian McCaffrey" -> "C. McCaffrey", "C McCaffrey"
        if len(first) > 1:
            variations.add(f"{first[0]}. {last}".lower())
            variations.add(f"{first[0]} {last}".lower())
        
        # "C. McCaffrey" -> "Christian McCaffrey" (reverse)
        if len(first) == 2 and first.endswith('.'):
            # Common first name mappings
            first_name_map = {
                'C.': ['Christian', 'Chris', 'Calvin', 'Cameron', 'Cam'],
                'A.': ['Adrian', 'Antonio', 'Anthony', 'Aaron', 'Andre'],
                'D.': ['David', 'Daniel', 'Dak', 'DK', 'Davante'],
                'J.': ['James', 'Josh', 'Justin', 'Jaylen', 'Jalen'],
                'T.': ['Tyler', 'Tua', 'Travis', 'Tyreek'],
                'K.': ['Kyle', 'Kyler', 'Kenneth'],
                'M.': ['Mike', 'Michael', 'Mark', 'Matt', 'Matthew'],
                'R.': ['Robert', 'Ryan', 'Russell', 'Romeo'],
                'S.': ['Sam', 'Samuel', 'Saquon', 'Stefan']
            }
            
            if first in first_name_map:
                for full_first in first_name_map[first]:
                    variations.add(f"{full_first} {last}".lower())
    
    # Handle nickname patterns
    nickname_map = {
        'dk': 'D.K.',
        'aj': 'A.J.',
        'cj': 'C.J.',
        'tj': 'T.J.',
        'jj': 'J.J.',
        'rj': 'R.J.',
        'bj': 'B.J.',
        'pj': 'P.J.',
        'ceedee': 'CeeDee',
        'cmc': 'Christian McCaffrey',
        'cmac': 'Christian McCaffrey'
    }
    
    # Apply nickname mappings
    name_lower = name.lower()
    for nick, full in nickname_map.items():
        if nick in name_lower:
            variations.add(name_lower.replace(nick, full.lower()))
        if full.lower() in name_lower:
            variations.add(name_lower.replace(full.lower(), nick))
    
    # Remove extra whitespace and empty strings
    clean_variations = set()
    for var in variations:
        clean_var = ' '.join(var.split())  # Normalize whitespace
        if clean_var:
            clean_variations.add(clean_var)
    
    return list(clean_variations)

def create_normalized_name_lookup(adp_players):
    """Create comprehensive lookup dictionary with aggressive name normalization"""
    adp_lookup = {}
    
    for player_name, adp_player in adp_players.items():
        if not isinstance(adp_player, dict):
            continue
            
        original_name = str(adp_player.get("name", "")).strip()
        if not original_name:
            continue
            
        # Generate ALL possible name variations
        name_variations = generate_name_variations(original_name)
        
        # Add all variations to lookup
        for variation in name_variations:
            if variation and variation not in adp_lookup:
                adp_lookup[variation] = adp_player
    
    return adp_lookup

def enhanced_match_players_to_adp(fantasy_players, adp_players):
    """Enhanced player matching with comprehensive name normalization"""
    if not adp_players:
        print("No ADP data available for matching")
        return fantasy_players
    
    print("Creating enhanced name lookup...")
    adp_lookup = create_normalized_name_lookup(adp_players)
    
    print(f"Generated {len(adp_lookup)} name variations from {len(adp_players)} ADP players")
    
    matched_count = 0
    unmatched_sleeper = []
    unmatched_adp = set(adp_players.keys())
    
    # Debug: Show sample ADP names
    print("\nSample ADP player names:")
    for i, name in enumerate(list(adp_players.keys())[:10]):
        print(f"  {name}")
    
    print(f"\nMatching {len(fantasy_players)} Sleeper players...")
    
    for player in fantasy_players:
        sleeper_name = str(player["player_name"]).strip()
        first_name = str(player.get("first_name", "")).strip()
        last_name = str(player.get("last_name", "")).strip()
        
        # Generate variations for Sleeper player
        sleeper_variations = []
        
        # Primary name variations
        sleeper_variations.extend(generate_name_variations(sleeper_name))
        
        # First + Last name combinations
        if first_name and last_name:
            full_name = f"{first_name} {last_name}"
            sleeper_variations.extend(generate_name_variations(full_name))
        
        # Use search_full_name if available (from cached database)
        search_full_name = player.get("search_full_name", "")
        if search_full_name:
            sleeper_variations.extend(generate_name_variations(search_full_name))
        
        # Try to find match
        adp_match = None
        matched_variation = None
        
        for variation in sleeper_variations:
            if variation in adp_lookup:
                adp_match = adp_lookup[variation]
                matched_variation = variation
                break
        
        if adp_match:
            # Add ADP data to player
            player["adp_data"] = {
                "source": "fantasyfootballcalculator.com",
                "standard": adp_match.get("standard", {}),
                "half_ppr": adp_match.get("half_ppr", {}),
                "ppr": adp_match.get("ppr", {}),
                "average_adp": adp_match.get("average_adp", 999),
                "matched_via": matched_variation
            }
            matched_count += 1
            
            # Remove from unmatched ADP set
            original_name = adp_match.get("name", "")
            unmatched_adp.discard(original_name)
            
        else:
            player["adp_data"] = None
            unmatched_sleeper.append({
                "sleeper_name": sleeper_name,
                "position": player.get("position", ""),
                "variations_tried": sleeper_variations[:3]
            })
    
    print(f"\nMatching Results:")
    print(f"  Matched: {matched_count}/{len(fantasy_players)} ({matched_count/len(fantasy_players)*100:.1f}%)")
    print(f"  Unmatched Sleeper: {len(unmatched_sleeper)}")
    print(f"  Unmatched ADP: {len(unmatched_adp)}")
    
    # Show some unmatched examples for debugging
    if unmatched_sleeper:
        print(f"\nSample unmatched Sleeper players:")
        for player in unmatched_sleeper[:5]:
            print(f"  {player['sleeper_name']} ({player['position']})")
            print(f"    Tried: {player['variations_tried']}")
    
    if unmatched_adp:
        print(f"\nSample unmatched ADP players:")
        for name in list(unmatched_adp)[:5]:
            print(f"  {name}")
    
    return fantasy_players

def match_players_to_adp(fantasy_players, adp_players):
    """Updated function call - uses enhanced matching"""
    return enhanced_match_players_to_adp(fantasy_players, adp_players)

def collect_nfl_stats(year=2025, weeks=None):
    """Collect NFL weekly statistics via nfl_data_py"""
    try:
        print(f"Attempting to collect NFL stats for {year}...")
        
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

def create_output_files(fantasy_players, matched_records, narrative_insights):
    """Create the data files expected by the workflow"""
    os.makedirs("data", exist_ok=True)
    
    # Calculate metrics for reporting
    total_players = len(fantasy_players)
    players_with_adp = 0
    
    for player in fantasy_players:
        if isinstance(player, dict) and player.get("adp_data"):
            players_with_adp += 1
    
    adp_coverage = (players_with_adp / total_players * 100) if total_players > 0 else 0
    
    # data/players.json - Full player database with ADP data
    players_data = {
        "metadata": {
            "total_players": total_players,
            "fantasy_relevant": total_players,
            "players_with_adp": players_with_adp,
            "last_updated": datetime.now().isoformat(),
            "adp_coverage_pct": round(adp_coverage, 1)
        },
        "players": fantasy_players
    }
    
    with open("data/players.json", "w") as f:
        json.dump(players_data, f, indent=2)
    
    # data/weekly_insights.json - Analysis for story generation
    insights_data = {
        "metadata": {
            "ready_for_stories": len(matched_records) > 0,
            "total_performances": len(matched_records),
            "includes_adp_analysis": players_with_adp > 0,
            "last_updated": datetime.now().isoformat()
        },
        "narrative_insights": narrative_insights or {},
        "insights": {
            "top_scorers": [],
            "draft_steals": [],
            "draft_busts": [],
            "position_breakdown": {},
            "team_breakdown": {}
        }
    }
    
    with open("data/weekly_insights.json", "w") as f:
        json.dump(insights_data, f, indent=2)
    
    # data/metadata.json - System health metrics
    base_score = 70
    adp_bonus = min(15, (adp_coverage / 100) * 15)
    quality_score = min(100, base_score + adp_bonus)
    
    metadata = {
        "data_health": {
            "quality_score": int(quality_score),
            "total_players": total_players,
            "adp_coverage_pct": round(adp_coverage, 1),
            "performance_records": len(matched_records),
            "narrative_insights_available": False
        },
        "collection_info": {
            "last_updated": datetime.now().isoformat(),
            "season": 2025,
            "source": "Sleeper_Cached + FantasyFootballCalculator_ADP"
        }
    }
    
    with open("data/metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Created output files: {total_players} players, {len(matched_records)} performances, {players_with_adp} with ADP")
    return total_players

def main():
    """Main execution function"""
    print("Starting NFL data collection with ADP analysis for Byline Content MVP...")
    
    try:
        # Load player database (cached or direct API)
        sleeper_players = load_sleeper_players()
        
        # Load live ADP data
        adp_data, adp_players = load_adp_data()
        
        # Match players to ADP data
        fantasy_players = match_players_to_adp(sleeper_players, adp_players)
        
        # Attempt NFL data collection
        nfl_stats = collect_nfl_stats(2025)
        
        # For now, create empty matched records since NFL season hasn't started
        matched_records = []
        narrative_insights = {}
        
        # Create workflow output files
        total_players = create_output_files(fantasy_players, matched_records, narrative_insights)
        
        # Summary report
        print(f"\n=== COLLECTION COMPLETE ===")
        print(f"Fantasy players processed: {len(fantasy_players)}")
        
        players_with_adp = len([p for p in fantasy_players if isinstance(p, dict) and p.get("adp_data")])
        print(f"Players with ADP data: {players_with_adp}")
        print(f"NFL records collected: {len(nfl_stats)}")
        print(f"Matched performances: {len(matched_records)}")
        print(f"New records added: 0")
        print(f"Draft steals identified: 0")
        print(f"Draft busts identified: 0")
        
        # Show ADP coverage stats
        if players_with_adp > 0:
            adp_coverage_pct = (players_with_adp / len(fantasy_players)) * 100
            print(f"\nADP Integration Success:")
            print(f"  Coverage: {players_with_adp}/{len(fantasy_players)} players ({adp_coverage_pct:.1f}%)")
            print(f"  Quality boost: 70 → {70 + min(15, adp_coverage_pct * 0.15):.0f} points")
        
        print("Data collection completed successfully")
        
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
