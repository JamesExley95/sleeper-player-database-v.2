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
            sleeper_pos = sleeper_player.get('position
