#!/usr/bin/env python3
"""
Comprehensive Data Validation Script
Validates all data sources and ensures system integrity
"""

import json
import os
import sys
from datetime import datetime

class DataValidator:
    def __init__(self):
        self.data_dir = "data"
        self.current_season = 2025
        self.validation_results = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'unknown',
            'validations': {},
            'errors': [],
            'warnings': []
        }
        
    def validate_sleeper_database(self):
        """Validate Sleeper player database"""
        try:
            print("Validating Sleeper player database...")
            
            players_file = f"{self.data_dir}/players.json"
            
            if not os.path.exists(players_file):
                self.validation_results['errors'].append("Sleeper players.json file missing")
                return False
                
            with open(players_file, 'r') as f:
                players_data = json.load(f)
                
            if not isinstance(players_data, dict):
                self.validation_results['errors'].append("Invalid players.json format")
                return False
                
            # Validate player data structure
            valid_players = 0
            total_players = len(players_data)
            
            for player_id, player_data in players_data.items():
                if not isinstance(player_data, dict):
                    continue
                    
                # Check required fields
                required_fields = ['full_name', 'position']
                if all(field in player_data for field in required_fields):
                    valid_players += 1
                    
            validation_rate = (valid_players / total_players * 100) if total_players > 0 else 0
            
            self.validation_results['validations']['sleeper_database'] = {
                'status': 'pass' if validation_rate >= 90 else 'fail',
                'total_players': total_players,
                'valid_players': valid_players,
                'validation_rate': round(validation_rate, 2)
            }
            
            if validation_rate < 90:
                self.validation_results['warnings'].append(f"Low Sleeper data quality: {validation_rate}%")
                
            print(f"Sleeper database validation: {validation_rate}% valid ({valid_players}/{total_players})")
            return validation_rate >= 80  # Lower threshold for minimum viability
            
        except Exception as e:
            self.validation_results['errors'].append(f"Sleeper database validation error: {e}")
            return False
            
    def validate_adp_data(self):
        """Validate ADP data collection"""
        try:
            print("Validating ADP data...")
            
            adp_file = f"{self.data_dir}/adp_consolidated_{self.current_season}.json"
            
            if not os.path.exists(adp_file):
                self.validation_results['errors'].append("Consolidated ADP data missing")
                return False
                
            with open(adp_file, 'r') as f:
                adp_data = json.load(f)
                
            players = adp_data.get('players', {})
            meta = adp_data.get('meta', {})
            
            if not players:
                self.validation_results['errors'].append("No ADP player data found")
                return False
                
            # Validate ADP data quality
            valid_adp_players = 0
            ppr_players = 0
            
            for player_id, player_data in players.items():
                adp_info = player_data.get('adp', {})
                ppr_adp = adp_info.get('ppr', {})
                
                if ppr_adp.get('adp', 0) > 0:
                    ppr_players += 1
                    
                if (ppr_adp.get('times_drafted', 0) >= 10 and
                    ppr_adp.get('adp', 0) > 0 and
                    player_data.get('name', '')):
                    valid_adp_players += 1
                    
            adp_quality = (valid_adp_players / len(players) * 100) if players else 0
            
            self.validation_results['validations']['adp_data'] = {
                'status': 'pass' if adp_quality >= 80 else 'fail',
                'total_players': len(players),
                'valid_adp_players': valid_adp_players,
                'ppr_players': ppr_players,
                'quality_rate': round(adp_quality, 2),
                'source_meta': meta
            }
            
            if adp_quality < 80:
                self.validation_results['warnings'].append(f"Low ADP data quality: {adp_quality}%")
                
            print(f"ADP data validation: {adp_quality}% quality ({valid_adp_players}/{len(players)})")
            return adp_quality >= 70
            
        except Exception as e:
            self.validation_results['errors'].append(f"ADP data validation error: {e}")
            return False
            
    def validate_draft_database(self):
        """Validate draft database integration"""
        try:
            print("Validating draft database...")
            
            draft_file = f"{self.data_dir}/draft_database_{self.current_season}.json"
            
            if not os.path.exists(draft_file):
                self.validation_results['warnings'].append("Draft database not yet generated")
                return True  # Not critical for basic operation
                
            with open(draft_file, 'r') as f:
                draft_data = json.load(f)
                
            players = draft_data.get('players', {})
            meta = draft_data.get('meta', {})
            
            # Check integration quality
            sleeper_matched = 0
            adp_integrated = 0
            
            for player_id, player_data in players.items():
                if player_data.get('sleeper_id'):
                    sleeper_matched += 1
                    
                if player_data.get('adp_data', {}).get('ppr', {}).get('adp', 0) > 0:
                    adp_integrated += 1
                    
            match_rate = meta.get('match_rate', 0)
            
            self.validation_results['validations']['draft_database'] = {
                'status': 'pass' if match_rate >= 70 else 'fail',
                'total_players': len(players),
                'sleeper_matched': sleeper_matched,
                'adp_integrated': adp_integrated,
                'match_rate': match_rate
            }
            
            print(f"Draft database validation: {match_rate}% match rate")
            return match_rate >= 60
            
        except Exception as e:
            self.validation_results['errors'].append(f"Draft database validation error: {e}")
            return False
            
    def validate_performance_data(self):
        """Validate performance tracking data"""
        try:
            print("Validating performance data...")
            
            performance_file = f"{self.data_dir}/season_{self.current_season}_performances.json"
            
            if not os.path.exists(performance_file):
                self.validation_results['warnings'].append("No performance data yet - may be preseason")
                return True  # Not critical in preseason
                
            with open(performance_file, 'r') as f:
                performance_data = json.load(f)
                
            if not performance_data:
                self.validation_results['warnings'].append("Empty performance data")
                return True
                
            # Check data quality
            players_with_data = 0
            total_weeks_tracked = 0
            
            for player_id, player_data in performance_data.items():
                weekly_perfs = player_data.get('weekly_performances', {})
                
                if weekly_perfs:
                    players_with_data += 1
                    total_weeks_tracked += len(weekly_perfs)
                    
            avg_weeks_per_player = (total_weeks_tracked / players_with_data) if players_with_data > 0 else 0
            
            self.validation_results['validations']['performance_data'] = {
                'status': 'pass',
                'total_players': len(performance_data),
                'players_with_data': players_with_data,
                'avg_weeks_per_player': round(avg_weeks_per_player, 1),
                'total_week_records': total_weeks_tracked
            }
            
            print(f"Performance data validation: {players_with_data} players, {total_weeks_tracked} total week records")
            return True
            
        except Exception as e:
            self.validation_results['errors'].append(f"Performance data validation error: {e}")
            return False
            
    def validate_file_structure(self):
        """Validate required file structure"""
        try:
            print("Validating file structure...")
            
            required_files = [
                'players.json',
                f'adp_consolidated_{self.current_season}.json'
            ]
            
            optional_files = [
                f'draft_database_{self.current_season}.json',
                f'season_{self.current_season}_performances.json',
                'weekly_snapshots.json',
                'performance_insights.json'
            ]
            
            missing_required = []
            missing_optional = []
            
            for filename in required_files:
                filepath = f"{self.data_dir}/{filename}"
                if not os.path.exists(filepath):
                    missing_required.append(filename)
                    
            for filename in optional_files:
                filepath = f"{self.data_dir}/{filename}"
                if not os.path.exists(filepath):
                    missing_optional.append(filename)
                    
            self.validation_results['validations']['file_structure'] = {
                'status': 'pass' if not missing_required else 'fail',
                'required_files_present': len(required_files) - len(missing_required),
                'optional_files_present': len(optional_files) - len(missing_optional),
                'missing_required': missing_required,
                'missing_optional': missing_optional
            }
            
            if missing_required:
                self.validation_results['errors'].extend([f"Missing required file: {f}" for f in missing_required])
                
            if missing_optional:
                self.validation_results['warnings'].extend([f"Missing optional file: {f}" for f in missing_optional])
                
            print(f"File structure validation: {len(missing_required)} missing required, {len(missing_optional)} missing optional")
            return len(missing_required) == 0
            
        except Exception as e:
            self.validation_results['errors'].append(f"File structure validation error: {e}")
            return False
            
    def validate_data_freshness(self):
        """Validate data freshness and update timestamps"""
        try:
            print("Validating data freshness...")
            
            current_time = datetime.now()
            freshness_issues = []
            
            # Check key files for staleness
            files_to_check = [
                ('players.json', 7),  # Should be updated weekly
                (f'adp_consolidated_{self.current_season}.json', 1),  # Should be updated daily
            ]
            
            for filename, max_age_days in files_to_check:
                filepath = f"{self.data_dir}/{filename}"
                
                if os.path.exists(filepath):
                    file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                    age_days = (current_time - file_time).days
                    
                    if age_days > max_age_days:
                        freshness_issues.append(f"{filename} is {age_days} days old (max: {max_age_days})")
                        
            self.validation_results['validations']['data_freshness'] = {
                'status': 'pass' if not freshness_issues else 'warning',
                'freshness_issues': freshness_issues,
                'check_timestamp': current_time.isoformat()
            }
            
            if freshness_issues:
                self.validation_results['warnings'].extend(freshness_issues)
                
            print(f"Data freshness validation: {len(freshness_issues)} freshness issues")
            return True
            
        except Exception as e:
            self.validation_results['errors'].append(f"Data freshness validation error: {e}")
            return False
            
    def generate_validation_report(self):
        """Generate comprehensive validation report"""
        try:
            # Determine overall status
            has_errors = len(self.validation_results['errors']) > 0
            critical_failures = 0
            
            for validation_name, validation_data in self.validation_results['validations'].items():
                if validation_data.get('status') == 'fail':
                    if validation_name in ['sleeper_database', 'adp_data', 'file_structure']:
                        critical_failures += 1
                        
            if has_errors or critical_failures > 0:
                self.validation_results['overall_status'] = 'fail'
            elif len(self.validation_results['warnings']) > 0:
                self.validation_results['overall_status'] = 'warning'
            else:
                self.validation_results['overall_status'] = 'pass'
                
            # Save validation report
            report_file = f"{self.data_dir}/validation_report.json"
            with open(report_file, 'w') as f:
                json.dump(self.validation_results, f, indent=2)
                
            # Print summary
            print(f"\n=== VALIDATION SUMMARY ===")
            print(f"Overall Status: {self.validation_results['overall_status'].upper()}")
            print(f"Errors: {len(self.validation_results['errors'])}")
            print(f"Warnings: {len(self.validation_results['warnings'])}")
            print(f"Validations: {len(self.validation_results['validations'])}")
            
            if self.validation_results['errors']:
                print(f"\nERRORS:")
                for error in self.validation_results['errors']:
                    print(f"  - {error}")
                    
            if self.validation_results['warnings']:
                print(f"\nWARNINGS:")
                for warning in self.validation_results['warnings']:
                    print(f"  - {warning}")
                    
            print(f"\nDetailed report saved to: {report_file}")
            
            return self.validation_results['overall_status'] != 'fail'
            
        except Exception as e:
            print(f"Error generating validation report: {e}")
            return False

def main():
    """Main validation execution"""
    validator = DataValidator()
    
    print("Starting comprehensive data validation...")
    
    # Run all validations
    validations = [
        validator.validate_file_structure(),
        validator.validate_sleeper_database(),
        validator.validate_adp_data(),
        validator.validate_draft_database(),
        validator.validate_performance_data(),
        validator.validate_data_freshness()
    ]
    
    # Generate final report
    report_success = validator.generate_validation_report()
    
    # Return success status
    overall_success = report_success and validator.validation_results['overall_status'] != 'fail'
    
    return overall_success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
