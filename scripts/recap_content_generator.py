#!/usr/bin/env python3
"""
Enhanced Recap Content Generator
Generates AI-powered fantasy football content using ADP and performance data
"""

import json
import pandas as pd
from datetime import datetime, timedelta
import os
import sys
import statistics

class RecapContentGenerator:
    def __init__(self):
        self.data_dir = "data"
        self.current_season = 2025
        self.content_dir = f"{self.data_dir}/generated_content"
        self.ensure_directories()
        
    def ensure_directories(self):
        """Create necessary directories"""
        os.makedirs(self.content_dir, exist_ok=True)
        
    def load_draft_database(self):
        """Load comprehensive draft database"""
        try:
            draft_file = f"{self.data_dir}/draft_database_{self.current_season}.json"
            
            if not os.path.exists(draft_file):
                print("Draft database not found")
                return {}
                
            with open(draft_file, 'r') as f:
                draft_data = json.load(f)
                
            return draft_data.get('players', {})
            
        except Exception as e:
            print(f"Error loading draft database: {e}")
            return {}
            
    def load_performance_data(self):
        """Load weekly performance data"""
        try:
            performance_file = f"{self.data_dir}/season_{self.current_season}_performances.json"
            
            if not os.path.exists(performance_file):
                print("Performance data not found")
                return {}
                
            with open(performance_file, 'r') as f:
                performance_data = json.load(f)
                
            return performance_data
            
        except Exception as e:
            print(f"Error loading performance data: {e}")
            return {}
            
    def load_adp_historical(self):
        """Load historical ADP tracking data"""
        try:
            historical_file = f"{self.data_dir}/adp_historical_tracking_{self.current_season}.json"
            
            if not os.path.exists(historical_file):
                print("Historical ADP data not found")
                return {}
                
            with open(historical_file, 'r') as f:
                historical_data = json.load(f)
                
            return historical_data
            
        except Exception as e:
            print(f"Error loading historical ADP data: {e}")
            return {}
            
    def get_current_week(self):
        """Determine current NFL week"""
        # Simplified week calculation - adjust based on actual season
        season_start = datetime(2025, 9, 4)
        current_date = datetime.now()
        
        if current_date < season_start:
            return 0
            
        days_since_start = (current_date - season_start).days
        week = min((days_since_start // 7) + 1, 18)
        return week
        
    def generate_adp_volatility_analysis(self):
        """Generate ADP volatility and risk analysis content"""
        try:
            print("Generating ADP volatility analysis...")
            
            draft_db = self.load_draft_database()
            
            if not draft_db:
                return None
                
            # Analyze volatility by position and round
            volatility_analysis = {
                'high_volatility_players': [],
                'stable_picks': [],
                'position_volatility': {},
                'round_analysis': {}
            }
            
            position_volatility = {}
            round_volatility = {}
            
            for player_id, player_data in draft_db.items():
                position = player_data.get('position', '')
                adp_data = player_data.get('adp_data', {}).get('ppr', {})
                draft_analysis = player_data.get('draft_analysis', {})
                
                if not adp_data:
                    continue
                    
                adp = adp_data.get('adp', 0)
                stdev = adp_data.get('stdev', 0)
                times_drafted = adp_data.get('times_drafted', 0)
                draft_round = draft_analysis.get('draft_round', 0)
                
                # Skip players with insufficient data
                if times_drafted < 50 or adp == 0:
                    continue
                    
                # Track position volatility
                if position not in position_volatility:
                    position_volatility[position] = []
                position_volatility[position].append(stdev)
                
                # Track round volatility
                if draft_round not in round_volatility:
                    round_volatility[draft_round] = []
                round_volatility[draft_round].append(stdev)
                
                # Identify high volatility players (top 50 picks with high stdev)
                if adp <= 50 and stdev > 3:
                    volatility_analysis['high_volatility_players'].append({
                        'name': player_data.get('name', ''),
                        'position': position,
                        'team': player_data.get('team', ''),
                        'adp': round(adp, 1),
                        'stdev': round(stdev, 1),
                        'volatility_tier': draft_analysis.get('volatility_tier', ''),
                        'range': f"{adp_data.get('high', 0)}-{adp_data.get('low', 0)}"
                    })
                    
                # Identify stable picks (low stdev)
                if adp <= 100 and stdev < 2:
                    volatility_analysis['stable_picks'].append({
                        'name': player_data.get('name', ''),
                        'position': position,
                        'team': player_data.get('team', ''),
                        'adp': round(adp, 1),
                        'stdev': round(stdev, 1),
                        'times_drafted': times_drafted
                    })
                    
            # Calculate position volatility averages
            for position, stdevs in position_volatility.items():
                volatility_analysis['position_volatility'][position] = {
                    'avg_stdev': round(statistics.mean(stdevs), 2),
                    'player_count': len(stdevs),
                    'volatility_rank': 0  # Will be calculated after sorting
                }
                
            # Rank positions by volatility
            sorted_positions = sorted(
                volatility_analysis['position_volatility'].items(),
                key=lambda x: x[1]['avg_stdev'],
                reverse=True
            )
            
            for rank, (position, data) in enumerate(sorted_positions, 1):
                volatility_analysis['position_volatility'][position]['volatility_rank'] = rank
                
            # Sort high volatility and stable picks
            volatility_analysis['high_volatility_players'].sort(key=lambda x: x['stdev'], reverse=True)
            volatility_analysis['stable_picks'].sort(key=lambda x: x['stdev'])
            
            # Generate content
            content = self.format_volatility_content(volatility_analysis)
            
            # Save analysis
            output_file = f"{self.content_dir}/adp_volatility_analysis.json"
            with open(output_file, 'w') as f:
                json.dump({
                    'generated_at': datetime.now().isoformat(),
                    'analysis': volatility_analysis,
                    'content': content
                }, f, indent=2)
                
            print(f"Generated ADP volatility analysis: {len(volatility_analysis['high_volatility_players'])} volatile players")
            return content
            
        except Exception as e:
            print(f"Error generating volatility analysis: {e}")
            return None
            
    def format_volatility_content(self, analysis):
        """Format volatility analysis into readable content"""
        content = {
            'headline': "Fantasy Draft Risk Assessment: Most Volatile vs Safest Picks",
            'summary': "",
            'key_insights': [],
            'sections': {}
        }
        
        # Generate summary
        high_vol_count = len(analysis['high_volatility_players'])
        stable_count = len(analysis['stable_picks'])
        
        content['summary'] = f"""
        Analysis of {high_vol_count + stable_count} top fantasy players reveals significant 
        volatility differences in 2025 drafts. {high_vol_count} early-round picks show 
        high volatility (3+ pick standard deviation), while {stable_count} players 
        demonstrate remarkable draft consensus.
        """
        
        # Key insights
        if analysis['high_volatility_players']:
            most_volatile = analysis['high_volatility_players'][0]
            content['key_insights'].append(
                f"{most_volatile['name']} ({most_volatile['position']}) is the most volatile "
                f"early pick with {most_volatile['stdev']} pick standard deviation"
            )
            
        if analysis['stable_picks']:
            most_stable = analysis['stable_picks'][0]
            content['key_insights'].append(
                f"{most_stable['name']} ({most_stable['position']}) is the safest pick "
                f"with only {most_stable['stdev']} pick standard deviation"
            )
            
        # Position volatility insight
        sorted_pos = sorted(
            analysis['position_volatility'].items(),
            key=lambda x: x[1]['avg_stdev'],
            reverse=True
        )
        
        if sorted_pos:
            most_volatile_pos = sorted_pos[0]
            content['key_insights'].append(
                f"{most_volatile_pos[0]} is the most volatile position "
                f"({most_volatile_pos[1]['avg_stdev']} avg standard deviation)"
            )
            
        # Detailed sections
        content['sections'] = {
            'high_risk_players': {
                'title': "High-Risk Early Round Picks",
                'description': "Players with significant draft position volatility",
                'players': analysis['high_volatility_players'][:10]
            },
            'safe_picks': {
                'title': "Consensus Safe Picks",
                'description': "Players with consistent draft positions",
                'players': analysis['stable_picks'][:10]
            },
            'position_analysis': {
                'title': "Position Volatility Rankings",
                'description': "Average draft volatility by position",
                'data': analysis['position_volatility']
            }
        }
        
        return content
        
    def generate_position_scarcity_analysis(self):
        """Generate position scarcity and timing analysis"""
        try:
            print("Generating position scarcity analysis...")
            
            draft_db = self.load_draft_database()
            
            if not draft_db:
                return None
                
            # Analyze position distribution by rounds
            position_by_round = {}
            early_round_positions = {}
            
            for player_id, player_data in draft_db.items():
                position = player_data.get('position', '')
                draft_analysis = player_data.get('draft_analysis', {})
                draft_round = draft_analysis.get('draft_round', 0)
                adp_data = player_data.get('adp_data', {}).get('ppr', {})
                adp = adp_data.get('adp', 0)
                
                if draft_round == 0 or adp == 0:
                    continue
                    
                # Track position distribution by round
                if draft_round not in position_by_round:
                    position_by_round[draft_round] = {}
                    
                if position not in position_by_round[draft_round]:
                    position_by_round[draft_round][position] = []
                    
                position_by_round[draft_round][position].append({
                    'name': player_data.get('name', ''),
                    'adp': adp,
                    'team': player_data.get('team', '')
                })
                
                # Track early round positions (first 5 rounds)
                if draft_round <= 5:
                    if position not in early_round_positions:
                        early_round_positions[position] = []
                        
                    early_round_positions[position].append({
                        'name': player_data.get('name', ''),
                        'adp': adp,
                        'round': draft_round,
                        'team': player_data.get('team', '')
                    })
                    
            # Calculate scarcity metrics
            scarcity_analysis = {
                'position_depth': {},
                'round_composition': {},
                'early_round_distribution': {},
                'scarcity_rankings': []
            }
            
            # Position depth analysis
            for position, players in early_round_positions.items():
                players_sorted = sorted(players, key=lambda x: x['adp'])
                
                scarcity_analysis['position_depth'][position] = {
                    'early_round_count': len(players),
                    'first_player_adp': players_sorted[0]['adp'] if players_sorted else 999,
                    'top_5_adps': [p['adp'] for p in players_sorted[:5]],
                    'avg_early_adp': round(statistics.mean([p['adp'] for p in players]), 1) if players else 0
                }
                
            # Round composition
            for round_num, positions in position_by_round.items():
                total_players = sum(len(players) for players in positions.values())
                
                scarcity_analysis['round_composition'][round_num] = {
                    'total_players': total_players,
                    'position_breakdown': {
                        pos: {
                            'count': len(players),
                            'percentage': round(len(players) / total_players * 100, 1) if total_players > 0 else 0
                        }
                        for pos, players in positions.items()
                    }
                }
                
            # Generate content
            content = self.format_scarcity_content(scarcity_analysis)
            
            # Save analysis
            output_file = f"{self.content_dir}/position_scarcity_analysis.json"
            with open(output_file, 'w') as f:
                json.dump({
                    'generated_at': datetime.now().isoformat(),
                    'analysis': scarcity_analysis,
                    'content': content
                }, f, indent=2)
                
            print("Generated position scarcity analysis")
            return content
            
        except Exception as e:
            print(f"Error generating scarcity analysis: {e}")
            return None
            
    def format_scarcity_content(self, analysis):
        """Format scarcity analysis into readable content"""
        content = {
            'headline': "2025 Fantasy Draft: Position Scarcity and Optimal Timing Guide",
            'summary': "",
            'key_insights': [],
            'sections': {}
        }
        
        # Generate insights from position depth
        depth_analysis = analysis['position_depth']
        
        # Find scarcest position (fewest early round players)
        scarcest_pos = min(depth_analysis.items(), key=lambda x: x[1]['early_round_count'])
        deepest_pos = max(depth_analysis.items(), key=lambda x: x[1]['early_round_count'])
        
        content['key_insights'].extend([
            f"{scarcest_pos[0]} is the scarcest position with only {scarcest_pos[1]['early_round_count']} early-round options",
            f"{deepest_pos[0]} offers the most depth with {deepest_pos[1]['early_round_count']} early-round players",
            f"First {scarcest_pos[0]} goes at pick {scarcest_pos[1]['first_player_adp']:.1f} on average"
        ])
        
        # Round composition insights
        round_1_composition = analysis['round_composition'].get(1, {}).get('position_breakdown', {})
        if round_1_composition:
            dominant_pos = max(round_1_composition.items(), key=lambda x: x[1]['percentage'])
            content['key_insights'].append(
                f"{dominant_pos[0]} dominates Round 1 with {dominant_pos[1]['percentage']}% of picks"
            )
            
        content['sections'] = {
            'position_timing': {
                'title': "Optimal Position Timing",
                'description': "When to target each position for maximum value",
                'data': depth_analysis
            },
            'round_breakdown': {
                'title': "Draft Round Composition",
                'description': "Position distribution by round",
                'data': analysis['round_composition']
            }
        }
        
        return content
        
    def generate_weekly_recap(self, week=None):
        """Generate weekly performance recap content"""
        try:
            if week is None:
                week = self.get_current_week()
                
            if week == 0:
                print("No weekly data available in preseason")
                return None
                
            print(f"Generating Week {week} recap...")
            
            # Load performance data
            performance_data = self.load_performance_data()
            draft_db = self.load_draft_database()
            
            if not performance_data or not draft_db:
                return None
                
            # Analyze week performance vs ADP
            week_analysis = {
                'week': week,
                'top_performers': [],
                'disappointments': [],
                'adp_vs_performance': [],
                'position_summary': {}
            }
            
            # Process weekly performances
            for player_id, player_perf in performance_data.items():
                weekly_perfs = player_perf.get('weekly_performances', {})
                week_data = weekly_perfs.get(str(week), {})
                
                if not week_data:
                    continue
                    
                # Get draft data for comparison
                draft_player = draft_db.get(player_id, {})
                adp_data = draft_player.get('adp_data', {}).get('ppr', {})
                adp = adp_data.get('adp', 999)
                
                if adp > 150:  # Skip very late picks
                    continue
                    
                fantasy_points = week_data.get('fantasy_points_ppr', 0)
                position = player_perf.get('position', '')
                name = player_perf.get('player_name', draft_player.get('name', ''))
                team = player_perf.get('team', draft_player.get('team', ''))
                
                player_summary = {
                    'name': name,
                    'position': position,
                    'team': team,
                    'adp': round(adp, 1),
                    'fantasy_points': round(fantasy_points, 1),
                    'draft_round': int((adp - 1) // 12) + 1 if adp > 0 else 0
                }
                
                # Categorize performance
                if fantasy_points >= 20:  # High scoring week
                    week_analysis['top_performers'].append(player_summary)
                elif adp <= 50 and fantasy_points < 5:  # Early pick disappointment
                    week_analysis['disappointments'].append(player_summary)
                    
                week_analysis['adp_vs_performance'].append(player_summary)
                
            # Sort results
            week_analysis['top_performers'].sort(key=lambda x: x['fantasy_points'], reverse=True)
            week_analysis['disappointments'].sort(key=lambda x: x['adp'])
            
            # Generate content
            content = self.format_weekly_content(week_analysis)
            
            # Save recap
            output_file = f"{self.content_dir}/week_{week}_recap.json"
            with open(output_file, 'w') as f:
                json.dump({
                    'generated_at': datetime.now().isoformat(),
                    'analysis': week_analysis,
                    'content': content
                }, f, indent=2)
                
            print(f"Generated Week {week} recap with {len(week_analysis['top_performers'])} top performers")
            return content
            
        except Exception as e:
            print(f"Error generating weekly recap: {e}")
            return None
            
    def format_weekly_content(self, analysis):
        """Format weekly analysis into readable content"""
        week = analysis['week']
        
        content = {
            'headline': f"Week {week} Fantasy Recap: Stars, Busts, and ADP Reality Check",
            'summary': "",
            'key_insights': [],
            'sections': {}
        }
        
        top_performers = analysis['top_performers'][:10]
        disappointments = analysis['disappointments'][:5]
        
        # Generate summary
        if top_performers:
            top_scorer = top_performers[0]
            content['summary'] = f"""
            Week {week} delivered explosive performances and crushing disappointments. 
            {top_scorer['name']} led all scorers with {top_scorer['fantasy_points']} points, 
            while several early-round picks failed to deliver on their draft capital.
            """
            
        # Key insights
        if top_performers:
            late_round_gems = [p for p in top_performers if p['draft_round'] >= 8]
            if late_round_gems:
                gem = late_round_gems[0]
                content['key_insights'].append(
                    f"Late-round gem: {gem['name']} (Round {gem['draft_round']}) "
                    f"exploded for {gem['fantasy_points']} points"
                )
                
        if disappointments:
            biggest_bust = disappointments[0]
            content['key_insights'].append(
                f"Biggest disappointment: {biggest_bust['name']} "
                f"(ADP {biggest_bust['adp']}) managed only {biggest_bust['fantasy_points']} points"
            )
            
        # Weekly sections
        content['sections'] = {
            'top_performers': {
                'title': f"Week {week} Top Performers",
                'description': "Players who delivered elite fantasy production",
                'players': top_performers
            },
            'disappointments': {
                'title': "Early Pick Disappointments",
                'description': "High-ADP players who underperformed expectations",
                'players': disappointments
            }
        }
        
        return content
        
    def generate_all_content(self):
        """Generate all available content types"""
        try:
            print("Generating all content types...")
            
            generated_content = {
                'generation_timestamp': datetime.now().isoformat(),
                'content_types': {}
            }
            
            # Generate ADP volatility analysis
            volatility_content = self.generate_adp_volatility_analysis()
            if volatility_content:
                generated_content['content_types']['adp_volatility'] = volatility_content
                
            # Generate position scarcity analysis
            scarcity_content = self.generate_position_scarcity_analysis()
            if scarcity_content:
                generated_content['content_types']['position_scarcity'] = scarcity_content
                
            # Generate weekly recap if in season
            current_week = self.get_current_week()
            if current_week > 0:
                weekly_content = self.generate_weekly_recap(current_week)
                if weekly_content:
                    generated_content['content_types']['weekly_recap'] = weekly_content
                    
            # Save master content file
            master_file = f"{self.content_dir}/master_content_{datetime.now().strftime('%Y%m%d')}.json"
            with open(master_file, 'w') as f:
                json.dump(generated_content, f, indent=2)
                
            content_count = len(generated_content['content_types'])
            print(f"Generated {content_count} content types")
            
            return generated_content
            
        except Exception as e:
            print(f"Error generating content: {e}")
            return None

def main():
    """Main execution function"""
    generator = RecapContentGenerator()
    
    print("Starting content generation...")
    
    # Generate all available content
    content = generator.generate_all_content()
    
    if content:
        content_types = list(content.get('content_types', {}).keys())
        print(f"\nContent generation completed!")
        print(f"Generated content types: {', '.join(content_types)}")
        return True
    else:
        print("Failed to generate content")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
