"""
Fabric Workspace Configuration Manager

Description:
    Utility to manage and validate Microsoft Fabric workspace configurations
    across different environments (dev, test, prod)

Features:
    - Load configuration from JSON files
    - Validate configuration structure
    - Environment-specific settings
    - Configuration templating

Usage:
    python workspace_config.py --env production --config config.json
    python workspace_config.py --validate --config config.json

Prerequisites:
    - Python 3.8+
    - JSON configuration files
"""

import json
import argparse
import os
from typing import Dict, Any, List
from pathlib import Path

class WorkspaceConfigManager:
    """Manage Fabric workspace configurations"""
    
    REQUIRED_FIELDS = [
        'workspace_name',
        'workspace_id',
        'environment'
    ]
    
    def __init__(self, config_path: str):
        """
        Initialize configuration manager
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = Path(config_path)
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            print(f"✓ Configuration loaded from: {self.config_path}")
            return config
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {str(e)}")
    
    def validate(self) -> List[str]:
        """
        Validate configuration structure
        
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Check required fields
        for field in self.REQUIRED_FIELDS:
            if field not in self.config:
                errors.append(f"Missing required field: {field}")
        
        # Validate environment
        valid_environments = ['development', 'test', 'production']
        if 'environment' in self.config:
            if self.config['environment'] not in valid_environments:
                errors.append(
                    f"Invalid environment: {self.config['environment']}. "
                    f"Must be one of: {', '.join(valid_environments)}"
                )
        
        # Validate lakehouse configuration if present
        if 'lakehouses' in self.config:
            if not isinstance(self.config['lakehouses'], list):
                errors.append("'lakehouses' must be a list")
        
        if errors:
            print("✗ Configuration validation failed:")
            for error in errors:
                print(f"  - {error}")
        else:
            print("✓ Configuration is valid")
        
        return errors
    
    def get_setting(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration setting
        
        Args:
            key: Setting key (supports nested keys with dot notation)
            default: Default value if key not found
            
        Returns:
            Setting value or default
        """
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def display_config(self):
        """Display configuration in a readable format"""
        print("\n" + "="*60)
        print(" WORKSPACE CONFIGURATION")
        print("="*60)
        print(json.dumps(self.config, indent=2))
        print("="*60 + "\n")
    
    @staticmethod
    def get_template_structure() -> Dict[str, Any]:
        """
        Get the standard configuration template structure
        
        Returns:
            Template dictionary
        """
        return {
            "workspace_name": "your-workspace-name",
            "workspace_id": "your-workspace-id",
            "environment": "development",
            "lakehouses": [
                {
                    "name": "lakehouse1",
                    "id": "lakehouse-id"
                }
            ],
            "data_sources": {
                "source1": {
                    "type": "azure_storage",
                    "connection_string": "your-connection-string"
                }
            },
            "settings": {
                "retention_days": 30,
                "auto_refresh": True
            }
        }
    
    def export_template(self, output_path: str):
        """
        Export a configuration template
        
        Args:
            output_path: Path for template file
        """
        template = self.get_template_structure()
        
        with open(output_path, 'w') as f:
            json.dump(template, f, indent=2)
        
        print(f"✓ Template exported to: {output_path}")

def create_config_template(output_path: str):
    """Create a configuration template file"""
    template = WorkspaceConfigManager.get_template_structure()
    
    with open(output_path, 'w') as f:
        json.dump(template, f, indent=2)
    
    print(f"✓ Template exported to: {output_path}")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Microsoft Fabric Workspace Configuration Manager'
    )
    parser.add_argument(
        '--config',
        type=str,
        help='Path to configuration file'
    )
    parser.add_argument(
        '--validate',
        action='store_true',
        help='Validate configuration'
    )
    parser.add_argument(
        '--display',
        action='store_true',
        help='Display configuration'
    )
    parser.add_argument(
        '--create-template',
        type=str,
        metavar='OUTPUT_PATH',
        help='Create a configuration template'
    )
    parser.add_argument(
        '--env',
        type=str,
        choices=['development', 'test', 'production'],
        help='Environment'
    )
    
    args = parser.parse_args()
    
    # Create template
    if args.create_template:
        create_config_template(args.create_template)
        return
    
    # Require config file for other operations
    if not args.config:
        parser.error("--config is required (unless using --create-template)")
    
    # Load configuration
    manager = WorkspaceConfigManager(args.config)
    
    # Validate
    if args.validate:
        errors = manager.validate()
        if errors:
            exit(1)
    
    # Display
    if args.display:
        manager.display_config()
    
    # If no action specified, just validate and display
    if not (args.validate or args.display):
        manager.validate()
        manager.display_config()

if __name__ == "__main__":
    main()
