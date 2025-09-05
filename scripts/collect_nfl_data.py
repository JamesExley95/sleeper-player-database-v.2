#!/usr/bin/env python3

import json
import os
import sys
import requests  # Added for ADP API calls
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
    """Inline filtering function for API fallback (simplified version)"""
    
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
    
    # API endpoints for different scoring formats - try multiple parameter combinations
    api_endpoints = {
        "standard": "https://fantasyfootballcalculator.com/api/v1/adp/standard?teams=12&year=2025",
        "ppr": "https://fantasyfootballcalculator.com/api/v1/adp/ppr?teams=12&year=2025",
        "half_ppr": "https://fantasyfootballcalculator.com/api/v1/adp/half-ppr?teams=12&year=2025"
    }
    
    # Fallback URLs to try if 2025 doesn't work
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

def save_performance_data(matched_records):
    """Save performance data with deduplication"""
    performance_file = "season_2025_performances.json"
    existing_records = []
    
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

def create_weekly_snapshots(matched_records):
    """Create individual week snapshots for Pipedream consumption"""
    if not matched_records:
        print("No performance data to snapshot")
        return
        
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

def create_output_files(fantasy_players, matched_records, narrative_insights):
    """Create the data files expected by the workflow"""
    os.makedirs("data", exist_ok=True)
    
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
    steals_count = 0
    busts_count = 0
    if isinstance(narrative_insights, dict):
        draft_analysis = narrative_insights.get("draft_analysis", {})
        if isinstance(draft_analysis, dict):
            steals = draft_analysis.get("steals", [])
            busts = draft_analysis.get("busts", [])
            steals_count = len(steals) if isinstance(steals, list) else 0
            busts_count = len(busts) if isinstance(busts, list) else 0
    
    insights_data = {
        "metadata": {
            "ready_for_stories": len(matched_records) > 0 and steals_count > 0,
            "weeks_available": weeks_available,
            "total_performances": len(matched_records),
            "includes_adp_analysis": players_with_adp > 0,
            "last_updated": datetime.now().isoformat()
        },
        "narrative_insights": narrative_insights,
        "insights": {
            "top_scorers": [],
            "draft_steals": [],
            "draft_busts": [],
            "position_breakdown": {},
            "team_breakdown": {}
        }
    }
    
    # Safely populate top scorers
    def safe_ppr_sort(record):
        try:
            return float(record.get("fantasy_points_ppr", 0))
        except (ValueError, TypeError):
            return 0
    
    valid_records = [r for r in matched_records if isinstance(r, dict)]
    insights_data["insights"]["top_scorers"] = sorted(
        valid_records, key=safe_ppr_sort, reverse=True
    )[:20]
    
    # Safely populate draft insights
    if isinstance(narrative_insights, dict):
        draft_analysis = narrative_insights.get("draft_analysis", {})
        if isinstance(draft_analysis, dict):
            steals = draft_analysis.get("steals", [])
            busts = draft_analysis.get("busts", [])
            
            if isinstance(steals, list):
                insights_data["insights"]["draft_steals"] = steals[:10]
            if isinstance(busts, list):
                insights_data["insights"]["draft_busts"] = busts[:10]
    
    # Calculate breakdowns
    for record in matched_records:
        if isinstance(record, dict):
            pos = str(record.get("position", ""))
            team = str(record.get("team", ""))
            
            if pos:
                insights_data["insights"]["position_breakdown"][pos] = insights_data["insights"]["position_breakdown"].get(pos, 0) + 1
            if team:
                insights_data["insights"]["team_breakdown"][team] = insights_data["insights"]["team_breakdown"].get(team, 0) + 1
    
    with open("data/weekly_insights.json", "w") as f:
        json.dump(insights_data, f, indent=2)
    
    # data/metadata.json - System health metrics with enhanced ADP scoring
    base_score = 70  # Your current score
    adp_bonus = min(15, (adp_coverage / 100) * 15)  # Up to 15 points for ADP coverage
    data_bonus = min(10, (data_coverage / 100) * 10)  # Up to 10 points for data coverage
    quality_score = min(100, base_score + adp_bonus + data_bonus)
    
    metadata = {
        "data_health": {
            "quality_score": int(quality_score),
            "total_players": total_players,
            "data_coverage_pct": round(data_coverage, 1),
            "adp_coverage_pct": round(adp_coverage, 1),
            "performance_records": len(matched_records),
            "weeks_with_data": len(weeks_available),
            "narrative_insights_available": steals_count > 0
        },
        "collection_info": {
            "last_updated": datetime.now().isoformat(),
            "season": 2025,
            "source": "Sleeper_Cached + nfl_data_py + FantasyFootballCalculator_ADP"
        }
    }
    
    with open("data/metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Created output files: {total_players} players, {len(matched_records)} performances, {players_with_adp} with ADP")
    return total_players

def main():
    """Main execution function with enhanced ADP integration"""
    print("Starting NFL data collection with ADP analysis for Byline Content MVP...")
    
    try:
        # Load player database (cached or direct API)
        sleeper_players = load_sleeper_players()
        
        # Load live ADP data
        adp_data, adp_players = load_adp_data()
        
        # Match players to ADP data (enhanced matching)
        fantasy_players = match_players_to_adp(sleeper_players, adp_players)
        
        # Attempt NFL data collection
        nfl_stats = collect_nfl_stats(2025)
        
        # Match players to NFL performance data
        matched_records, unmatched_nfl, unmatched_sleeper = match_sleeper_to_nfl(fantasy_players, nfl_stats)
        
        # Generate narrative insights for story templates
        narrative_insights = generate_narrative_insights(fantasy_players, matched_records)
        
        # Save performance data
        if matched_records:
            new_records = save_performance_data(matched_records)
            create_weekly_snapshots(matched_records)
        else:
            print("No NFL performance data available - creating empty structure")
            new_records = 0
        
        # Create workflow output files
        total_players = create_output_files(fantasy_players, matched_records, narrative_insights)
        
        # Enhanced summary report
        print(f"\n=== COLLECTION COMPLETE ===")
        print(f"Fantasy players processed: {len(fantasy_players)}")
        
        players_with_adp = len([p for p in fantasy_players if isinstance(p, dict) and p.get("adp_data")])
        print(f"Players with ADP data: {players_with_adp}")
        print(f"NFL records collected: {len(nfl_stats)}")
        print(f"Matched performances: {len(matched_records)}")
        print(f"New records added: {new_records}")
        
        # Safe access to narrative insights for summary
        steals_count = 0
        busts_count = 0
        if isinstance(narrative_insights, dict):
            draft_analysis = narrative_insights.get("draft_analysis", {})
            if isinstance(draft_analysis, dict):
                steals = draft_analysis.get("steals", [])
                busts = draft_analysis.get("busts", [])
                steals_count = len(steals) if isinstance(steals, list) else 0
                busts_count = len(busts) if isinstance(busts, list) else 0
        
        print(f"Draft steals identified: {steals_count}")
        print(f"Draft busts identified: {busts_count}")
        
        if matched_records:
            weeks = []
            for record in matched_records:
                if isinstance(record, dict):
                    week = record.get("week")
                    try:
                        week = int(week)
                        if week not in weeks:
                            weeks.append(week)
                    except (ValueError, TypeError):
                        pass
            weeks.sort()
            print(f"Weeks with data: {weeks}")
        
        # Show ADP coverage stats
        if players_with_adp > 0:
            adp_coverage_pct = (players_with_adp / len(fantasy_players)) * 100
            print(f"\nADP Integration Success:")
            print(f"  Coverage: {players_with_adp}/{len(fantasy_players)} players ({adp_coverage_pct:.1f}%)")
            print(f"  Quality boost: 70 → {70 + min(15, adp_coverage_pct * 0.15):.0f} points")
        
        # Show sample unmatched for debugging
        if unmatched_sleeper:
            print(f"\nSample unmatched NFL players:")
            for i, player in enumerate(unmatched_sleeper[:5]):
                if isinstance(player, dict):
                    name = player.get("nfl_name", "Unknown")
                    position = player.get("position", "")
                    team = player.get("team", "")
                    print(f"  {name} ({position}, {team})")
        
        print("Data collection completed successfully")
        
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() Success! Process the data
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
        # Save ADP database for future use/debugging
        with open("adp_database.json", "w") as f:
            json.dump(adp_database, f, indent=2)
        print(f"ADP database saved to adp_database.json")
        
        return adp_database, adp_database["players"]
    else:
        print("Warning: ADP collection failed - proceeding without ADP data")
        return None, {}

def generate_name_variations(name):
    """
    Generate comprehensive list of name variations for matching
    """
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
    """
    Create comprehensive lookup dictionary with aggressive name normalization
    """
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
    """
    Enhanced player matching with comprehensive name normalization
    """
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
                "matched_via": matched_variation  # Debug info
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
                "variations_tried": sleeper_variations[:3]  # First 3 for debugging
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

def calculate_draft_grade(value_status, games_played):
    """Calculate overall draft grade A-F"""
    try:
        games = int(games_played)
        if games < 3:
            return "Incomplete"
    except (ValueError, TypeError):
        return "Incomplete"
    
    grade_map = {
        "Draft Steal": "A",
        "Meeting Expectations": "B", 
        "Underperforming": "D"
    }
    
    return grade_map.get(value_status, "C")

def calculate_adp_value_metrics(player, performance_records):
    """Calculate value metrics comparing performance to ADP expectations"""
    adp_data = player.get("adp_data")
    if not adp_data or not isinstance(adp_data, dict):
        return {}
    
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

def match_sleeper_to_nfl(fantasy_players, nfl_stats):
    """Match Sleeper players to NFL statistics using multiple methods"""
    
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
                
                #
