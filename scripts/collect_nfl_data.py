#!/usr/bin/env python3

import json
import pandas as pd
import nfl_data_py as nfl
from datetime import datetime
import os
import sys

def load_sleeper_players():
    """Load and process Sleeper player database"""
    try:
        with open("players_detailed.json", "r") as f:
            raw_data = json.load(f)
        
        players_array = raw_data["players"]
        print(f"Loaded {len(players_array)} total players from Sleeper database")
        return players_array
        
    except FileNotFoundError:
        print("Error: players_detailed.json not found")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing player data: {e}")
        sys.exit(1)

def filter_fantasy_players(players_array):
    """Filter to fantasy-relevant players with active status"""
    fantasy_positions = ["QB", "RB", "WR", "TE", "K", "DEF"]
    inactive_statuses = ["Inactive", "Reserve/Injured", "Reserve/PUP", "Suspended"]
    
    fantasy_players = []
    
    for player in players_array:
        position = player.get("position")
        team = player.get("team")
        status = player.get("status", "Active")
        
        # Must have fantasy-relevant position
        if position not in fantasy_positions:
            continue
            
        # Must have active NFL team
        if not team:
            continue
            
        # Must not be inactive/injured
        if status in inactive_statuses:
            continue
            
        # Convert to consistent format for NFL matching
        fantasy_player = {
            "sleeper_id": player["player_id"],
            "player_name": player["name"],
            "first_name": player.get("first_name", ""),
            "last_name": player.get("last_name", ""),
            "position": position,
            "team": team,
            "status": status,
            "years_exp": player.get("years_exp", 0),
            "height": player.get("height", ""),
            "weight": player.get("weight", "")
        }
        
        fantasy_players.append(fantasy_player)
    
    print(f"Filtered to {len(fantasy_players)} fantasy-relevant players")
    return fantasy_players

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
        name_key = player["player_name"].lower().strip()
        sleeper_by_name[name_key] = player
        
        # Also index by "Last, First" format common in NFL data
        if player["first_name"] and player["last_name"]:
            last_first = f"{player['last_name']}, {player['first_name']}"
            sleeper_by_parts[last_first.lower()] = player
    
    print(f"Matching {len(fantasy_players)} Sleeper players against {len(nfl_stats)} NFL records...")
    
    # Perform matching
    for _, nfl_row in nfl_stats.iterrows():
        nfl_name = nfl_row["player_name"]
        nfl_display = nfl_row.get("player_display_name", nfl_name)
        
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
            
            # Create performance record
            performance_record = {
                "sleeper_id": matched_player["sleeper_id"],
                "nfl_player_id": str(nfl_row.get("player_id", "")),
                "player_name": matched_player["player_name"],
                "nfl_name": nfl_name,
                "position": matched_player["position"],
                "team": matched_player["team"],
                "week": int(nfl_row["week"]),
                "season": int(nfl_row.get("season", 2025)),
                "match_method": match_method,
                
                # Passing statistics
                "completions": int(nfl_row.get("completions", 0) or 0),
                "attempts": int(nfl_row.get("attempts", 0) or 0),
                "passing_yards": int(nfl_row.get("passing_yards", 0) or 0),
                "passing_tds": int(nfl_row.get("passing_tds", 0) or 0),
                "interceptions": int(nfl_row.get("interceptions", 0) or 0),
                
                # Rushing statistics
                "carries": int(nfl_row.get("carries", 0) or 0),
                "rushing_yards": int(nfl_row.get("rushing_yards", 0) or 0),
                "rushing_tds": int(nfl_row.get("rushing_tds", 0) or 0),
                
                # Receiving statistics
                "targets": int(nfl_row.get("targets", 0) or 0),
                "receptions": int(nfl_row.get("receptions", 0) or 0),
                "receiving_yards": int(nfl_row.get("receiving_yards", 0) or 0),
                "receiving_tds": int(nfl_row.get("receiving_tds", 0) or 0),
                
                # Fantasy points
                "fantasy_points": float(nfl_row.get("fantasy_points", 0) or 0),
                "fantasy_points_ppr": float(nfl_row.get("fantasy_points_ppr", 0) or 0),
                
                "last_updated": datetime.now().isoformat()
            }
            
            matched_records.append(performance_record)
        else:
            # Track unmatched NFL players in fantasy positions
            if nfl_row.get("position") in ["QB", "RB", "WR", "TE", "K"]:
                unmatched_sleeper.append({
                    "nfl_name": nfl_name,
                    "position": nfl_row.get("position"),
                    "team": nfl_row.get("team", ""),
                    "week": nfl_row.get("week", 0)
                })
    
    print(f"Successfully matched {len(matched_records)} performance records")
    print(f"Unmatched NFL players: {len(unmatched_nfl)}")
    print(f"Unmatched fantasy-relevant: {len(unmatched_sleeper)}")
    
    return matched_records, list(unmatched_nfl), unmatched_sleeper

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
        key = f"{record.get('sleeper_id', '')}_{record.get('week', 0)}"
        existing_keys.add(key)
    
    # Add only new records
    new_records = []
    duplicates = 0
    
    for record in matched_records:
        key = f"{record['sleeper_id']}_{record['week']}"
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
            "new_records_added": len(new_records)
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
        week = record["week"]
        if week not in weeks_data:
            weeks_data[week] = []
        weeks_data[week].append(record)
    
    # Save individual week files
    for week, week_records in weeks_data.items():
        snapshot = {
            "week": week,
            "season": 2025,
            "generated_at": datetime.now().isoformat(),
            "player_count": len(week_records),
            "performances": week_records,
            "summary": {
                "positions": {},
                "teams": {},
                "top_scorers": sorted(week_records, key=lambda x: x["fantasy_points_ppr"], reverse=True)[:10]
            }
        }
        
        # Add position/team summaries
        for record in week_records:
            pos = record["position"]
            team = record["team"]
            snapshot["summary"]["positions"][pos] = snapshot["summary"]["positions"].get(pos, 0) + 1
            snapshot["summary"]["teams"][team] = snapshot["summary"]["teams"].get(team, 0) + 1
        
        snapshot_file = f"weekly_snapshots/week_{week}_2025.json"
        with open(snapshot_file, "w") as f:
            json.dump(snapshot, f, indent=2, default=str)
        
        print(f"Created snapshot for Week {week}: {len(week_records)} players")

def create_output_files(fantasy_players, matched_records):
    """Create the data files expected by the workflow"""
    os.makedirs("data", exist_ok=True)
    
    # Calculate metrics for reporting
    total_players = len(fantasy_players)
    players_with_data = len(set(r["sleeper_id"] for r in matched_records))
    data_coverage = (players_with_data / total_players * 100) if total_players > 0 else 0
    
    # data/players.json - Full player database
    players_data = {
        "metadata": {
            "total_players": total_players,
            "fantasy_relevant": total_players,
            "last_updated": datetime.now().isoformat(),
            "data_coverage_pct": round(data_coverage, 1)
        },
        "players": fantasy_players
    }
    
    with open("data/players.json", "w") as f:
        json.dump(players_data, f, indent=2)
    
    # data/weekly_insights.json - Analysis for story generation
    weeks_available = sorted(set(r["week"] for r in matched_records)) if matched_records else []
    
    insights_data = {
        "metadata": {
            "ready_for_stories": len(matched_records) > 0,
            "weeks_available": weeks_available,
            "total_performances": len(matched_records),
            "last_updated": datetime.now().isoformat()
        },
        "insights": {
            "top_scorers": sorted(matched_records, key=lambda x: x["fantasy_points_ppr"], reverse=True)[:20] if matched_records else [],
            "position_breakdown": {},
            "team_breakdown": {}
        }
    }
    
    # Calculate breakdowns
    if matched_records:
        for record in matched_records:
            pos = record["position"]
            team = record["team"]
            insights_data["insights"]["position_breakdown"][pos] = insights_data["insights"]["position_breakdown"].get(pos, 0) + 1
            insights_data["insights"]["team_breakdown"][team] = insights_data["insights"]["team_breakdown"].get(team, 0) + 1
    
    with open("data/weekly_insights.json", "w") as f:
        json.dump(insights_data, f, indent=2)
    
    # data/metadata.json - System health metrics
    quality_score = min(95, 70 + (data_coverage / 100 * 25))  # 70-95 based on data coverage
    
    metadata = {
        "data_health": {
            "quality_score": int(quality_score),
            "total_players": total_players,
            "data_coverage_pct": round(data_coverage, 1),
            "performance_records": len(matched_records),
            "weeks_with_data": len(weeks_available)
        },
        "collection_info": {
            "last_updated": datetime.now().isoformat(),
            "season": 2025,
            "source": "Sleeper + nfl_data_py"
        }
    }
    
    with open("data/metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Created output files: {total_players} players, {len(matched_records)} performances")
    return total_players

def main():
    """Main execution function"""
    print("Starting NFL data collection for Byline Content MVP...")
    
    try:
        # Load player database
        sleeper_players = load_sleeper_players()
        
        # Filter to fantasy-relevant players
        fantasy_players = filter_fantasy_players(sleeper_players)
        
        # Attempt NFL data collection
        nfl_stats = collect_nfl_stats(2025)
        
        # Match players to NFL performance data
        matched_records, unmatched_nfl, unmatched_sleeper = match_sleeper_to_nfl(fantasy_players, nfl_stats)
        
        # Save performance data
        if matched_records:
            new_records = save_performance_data(matched_records)
            create_weekly_snapshots(matched_records)
        else:
            print("No NFL performance data available - creating empty structure")
            new_records = 0
        
        # Create workflow output files
        total_players = create_output_files(fantasy_players, matched_records)
        
        # Summary report
        print(f"\n=== COLLECTION COMPLETE ===")
        print(f"Fantasy players processed: {len(fantasy_players)}")
        print(f"NFL records collected: {len(nfl_stats)}")
        print(f"Matched performances: {len(matched_records)}")
        print(f"New records added: {new_records}")
        
        if matched_records:
            weeks = sorted(set(r["week"] for r in matched_records))
            print(f"Weeks with data: {weeks}")
        
        # Show sample unmatched for debugging
        if unmatched_sleeper:
            print(f"\nSample unmatched NFL players:")
            for player in unmatched_sleeper[:5]:
                print(f"  {player['nfl_name']} ({player['position']}, {player['team']})")
        
        print("Data collection completed successfully")
        
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
