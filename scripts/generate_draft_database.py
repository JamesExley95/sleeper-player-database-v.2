#!/usr/bin/env python3
"""
Enhanced Draft Database Generator
Combines Sleeper player database with FFC ADP data for comprehensive draft analysis
"""

import json
import pandas as pd
from datetime import datetime
import os
import sys

class DraftDatabaseGenerator:
    def __init__(self):
        self.data_dir = "data"
        self.current_season = 2025
        self.output_file = f"{self.data_dir}/draft_database_{self.current_season}.json"
        
    def load_sleeper_players(self):
        """Load Sleeper player database"""
        try:
            sleeper_file = f"{self.data_dir}/players.json"
            
            if not os.path.exists(sleeper_file):
                print("Sleeper players file not found")
                return {}
                
            with open(sleeper_file, 'r') as f:
                sleeper_data = json.load(f)
                
            print(f"Loaded {len(sleeper_data)} players from Sleeper database")
            return sleeper_data
            
        except Exception as e:
            print(f"Error loading Sleeper players: {e}")
            return {}
            
    def load_adp_data(self):
        """Load consolidated ADP data"""
        try:
            adp_file = f"{self.data_dir}/adp_consolidated_{self.current_season}.json"
            
            if not os.path.exists(adp_file):
                print("Consolidated ADP data not found")
                return {}
                
            with open(adp_file, 'r') as f:
                adp_data = json.load(f)
                
            players_adp = adp_data.get('players', {})
            print(f"Loaded ADP data for {len(players_adp)} players")
            return players_adp
            
        except Exception as e:
            print(f"Error loading ADP data: {e}")
            return {}
            
    def create_player_mapping(self, sleeper_data, adp_data):
        """Create mapping between Sleeper and FFC players"""
        mapping = {}
        matched_count = 0
        
        # Create name-based lookup for ADP data
        adp_lookup = {}
        for ffc_id, player_data in adp_data.items():
            name = player_data.get('name', '').strip().lower()
            team = player_data.get('team', '').strip().upper()
            position = player_data.get('position', '').strip().upper()
            
            key = f"{name}|{team}|{position}"
            adp_lookup[key] = {
                'ffc_id': ffc_id,
                'data': player_data
            }
            
        # Match Sleeper players to ADP data
        for sleeper_id, sleeper_player in sleeper_data.items():
            if not isinstance(sleeper_player, dict):
                continue
                
            sleeper_name = sleeper_player.get('full_name', '').strip().lower()
            sleeper_team = sleeper_player.get('team', '').strip().upper()
            sleeper_pos = sleeper_player.get('position', '').strip().upper()
            
            # Skip non-fantasy positions
            if sleeper_pos not in ['QB', 'RB', 'WR', 'TE', 'K', 'DEF']:
                continue
                
            # Try exact match first
            lookup_key = f"{sleeper_name}|{sleeper_team}|{sleeper_pos}"
            
            if lookup_key in adp_lookup:
                mapping[sleeper_id] = adp_lookup[lookup_key]
                matched_count += 1
                continue
                
            # Try name variations for common mismatches
            name_variants = [
                sleeper_name.replace(' jr.', '').replace(' sr.', '').replace(' iii', '').replace(' ii', ''),
                sleeper_name.replace('.', ''),
                sleeper_name.replace("'", "")
            ]
            
            # Try team-agnostic match (for recent trades)
            for variant in name_variants:
                for pos_check in [sleeper_pos, 'DEF' if sleeper_pos == 'DST' else sleeper_pos]:
                    # Check all teams for this name/position combo
                    for key, adp_info in adp_lookup.items():
                        if key.startswith(f"{variant}|") and key.endswith(f"|{pos_check}"):
                            mapping[sleeper_id] = adp_info
                            matched_count += 1
                            break
                    if sleeper_id in mapping:
                        break
                if sleeper_id in mapping:
                    break
                    
        print(f"Matched {matched_count} players between Sleeper and ADP data")
        return mapping
        
    def generate_draft_database(self):
        """Generate comprehensive draft database"""
        try:
            print("Generating draft database...")
            
            # Load data sources
            sleeper_data = self.load_sleeper_players()
            adp_data = self.load_adp_data()
            
            if not sleeper_data or not adp_data:
                print("Missing required data sources")
                return False
                
            # Create player mapping
            player_mapping = self.create_player_mapping(sleeper_data, adp_data)
            
            # Build comprehensive draft database
            draft_database = {
                'meta': {
                    'generated_at': datetime.now().isoformat(),
                    'season': self.current_season,
                    'total_sleeper_players': len(sleeper_data),
                    'total_adp_players': len(adp_data),
                    'matched_players': len(player_mapping),
                    'match_rate': round(len(player_mapping) / len(adp_data) * 100, 2)
                },
                'players': {}
            }
            
            # Process matched players
            for sleeper_id, adp_match in player_mapping.items():
                sleeper_player = sleeper_data.get(sleeper_id, {})
                adp_player = adp_match['data']
                
                # Create comprehensive player record
                player_record = {
                    'sleeper_id': sleeper_id,
                    'ffc_id': adp_match['ffc_id'],
                    'name': sleeper_player.get('full_name', ''),
                    'first_name': sleeper_player.get('first_name', ''),
                    'last_name': sleeper_player.get('last_name', ''),
                    'position': sleeper_player.get('position', ''),
                    'team': sleeper_player.get('team', ''),
                    'number': sleeper_player.get('number'),
                    'age': sleeper_player.get('age'),
                    'height': sleeper_player.get('height', ''),
                    'weight': sleeper_player.get('weight', ''),
                    'college': sleeper_player.get('college', ''),
                    'years_exp': sleeper_player.get('years_exp'),
                    'status': sleeper_player.get('status', ''),
                    'injury_status': sleeper_player.get('injury_status'),
                    'fantasy_positions': sleeper_player.get('fantasy_positions', []),
                    'bye_week': adp_player.get('bye_week', 0),
                    'adp_data': adp_player.get('adp', {}),
                    'draft_analysis': self.calculate_draft_analysis(adp_player),
                    'last_updated': datetime.now().isoformat()
                }
                
                draft_database['players'][sleeper_id] = player_record
                
            # Add ADP-only players (not in Sleeper database)
            for ffc_id, adp_player in adp_data.items():
                # Check if already matched
                already_matched = any(
                    mapping['ffc_id'] == ffc_id 
                    for mapping in player_mapping.values()
                )
                
                if not already_matched:
                    # Create record for ADP-only player
                    player_record = {
                        'sleeper_id': None,
                        'ffc_id': ffc_id,
                        'name': adp_player.get('name', ''),
                        'position': adp_player.get('position', ''),
                        'team': adp_player.get('team', ''),
                        'bye_week': adp_player.get('bye_week', 0),
                        'adp_data': adp_player.get('adp', {}),
                        'draft_analysis': self.calculate_draft_analysis(adp_player),
                        'data_source': 'ffc_only',
                        'last_updated': datetime.now().isoformat()
                    }
                    
                    # Use FFC ID as key for unmatched players
                    draft_database['players'][f"ffc_{ffc_id}"] = player_record
                    
            # Save draft database
            with open(self.output_file, 'w') as f:
                json.dump(draft_database, f, indent=2)
                
            print(f"Generated draft database with {len(draft_database['players'])} total players")
            print(f"Match rate: {draft_database['meta']['match_rate']}%")
            print(f"Saved to: {self.output_file}")
            
            return True
            
        except Exception as e:
            print(f"Error generating draft database: {e}")
            return False
            
    def calculate_draft_analysis(self, adp_player):
        """Calculate draft analysis metrics"""
        try:
            ppr_adp = adp_player.get('adp', {}).get('ppr', {})
            
            if not ppr_adp:
                return {}
                
            adp_value = ppr_adp.get('adp', 0)
            high = ppr_adp.get('high', 0)
            low = ppr_adp.get('low', 0)
            stdev = ppr_adp.get('stdev', 0)
            times_drafted = ppr_adp.get('times_drafted', 0)
            
            # Calculate analysis metrics
            analysis = {
                'draft_round': int((adp_value - 1) // 12) + 1 if adp_value > 0 else 0,
                'round_position': int(((adp_value - 1) % 12) + 1) if adp_value > 0 else 0,
                'volatility_tier': self.get_volatility_tier(stdev),
                'consensus_strength': self.get_consensus_strength(times_drafted),
                'range_analysis': {
                    'best_case': high,
                    'worst_case': low,
                    'range_size': low - high if low > high else 0,
                    'upside_picks': max(0, int(adp_value - high)),
                    'downside_risk': max(0, int(low - adp_value))
                },
                'draft_strategy': self.get_draft_strategy(adp_value, stdev, high, low)
            }
            
            return analysis
            
        except Exception as e:
            print(f"Error calculating draft analysis: {e}")
            return {}
            
    def get_volatility_tier(self, stdev):
        """Categorize player volatility based on standard deviation"""
        if stdev < 2:
            return "Very Stable"
        elif stdev < 4:
            return "Stable"
        elif stdev < 6:
            return "Moderate"
        elif stdev < 8:
            return "Volatile"
        else:
            return "Very Volatile"
            
    def get_consensus_strength(self, times_drafted):
        """Evaluate consensus strength based on sample size"""
        if times_drafted > 1000:
            return "Very Strong"
        elif times_drafted > 500:
            return "Strong"
        elif times_drafted > 200:
            return "Moderate"
        elif times_drafted > 50:
            return "Weak"
        else:
            return "Very Weak"
            
    def get_draft_strategy(self, adp, stdev, high, low):
        """Recommend draft strategy based on ADP characteristics"""
        strategies = []
        
        # Stability-based strategy
        if stdev < 2:
            strategies.append("Safe pick - draft at ADP")
        elif stdev > 6:
            strategies.append("High risk/reward - consider reach or wait")
            
        # Range-based strategy
        range_size = low - high if low > high else 0
        if range_size > 20:
            strategies.append("Wide range - value dependent on league settings")
        elif range_size < 5:
            strategies.append("Tight consensus - draft near ADP")
            
        # Round-based strategy
        draft_round = int((adp - 1) // 12) + 1 if adp > 0 else 0
        if draft_round <= 3:
            strategies.append("Early pick - minimize risk")
        elif draft_round >= 10:
            strategies.append("Late pick - swing for upside")
            
        return strategies if strategies else ["Standard draft approach"]
        
    def create_position_rankings(self):
        """Create position-specific rankings and tiers"""
        try:
            # Load draft database
            if not os.path.exists(self.output_file):
                print("Draft database not found - run generation first")
                return False
                
            with open(self.output_file, 'r') as f:
                draft_db = json.load(f)
                
            players = draft_db.get('players', {})
            
            # Group by position
            position_rankings = {}
            
            for player_id, player_data in players.items():
                position = player_data.get('position', '')
                
                if position not in position_rankings:
                    position_rankings[position] = []
                    
                # Get PPR ADP for ranking
                ppr_adp = player_data.get('adp_data', {}).get('ppr', {}).get('adp', 999)
                
                position_rankings[position].append({
                    'player_id': player_id,
                    'name': player_data.get('name', ''),
                    'team': player_data.get('team', ''),
                    'adp': ppr_adp,
                    'volatility': player_data.get('draft_analysis', {}).get('volatility_tier', ''),
                    'draft_round': player_data.get('draft_analysis', {}).get('draft_round', 0)
                })
                
            # Sort each position by ADP
            for position in position_rankings:
                position_rankings[position].sort(key=lambda x: x['adp'])
                
                # Add position rank
                for i, player in enumerate(position_rankings[position], 1):
                    player['position_rank'] = i
                    
            # Save position rankings
            rankings_file = f"{self.data_dir}/position_rankings_{self.current_season}.json"
            with open(rankings_file, 'w') as f:
                json.dump(position_rankings, f, indent=2)
                
            print(f"Created position rankings for {len(position_rankings)} positions")
            return True
            
        except Exception as e:
            print(f"Error creating position rankings: {e}")
            return False

def main():
    """Main execution function"""
    generator = DraftDatabaseGenerator()
    
    print("Starting draft database generation...")
    
    # Generate comprehensive draft database
    success = generator.generate_draft_database()
    
    if success:
        # Create position-specific rankings
        rankings_success = generator.create_position_rankings()
        
        print("\nDraft database generation completed!")
        print(f"Database: {'✓' if success else '✗'}")
        print(f"Position rankings: {'✓' if rankings_success else '✗'}")
        
        return success and rankings_success
    else:
        print("Failed to generate draft database")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
