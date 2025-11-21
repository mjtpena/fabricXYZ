# Deployment Tools

Utilities for deploying and managing Microsoft Fabric artifacts across environments.

## Available Tools

### workspace_config.py

Configuration management tool for Fabric workspaces.

**Features:**
- Load and validate workspace configurations
- Environment-specific settings (dev/test/prod)
- Configuration templates
- JSON-based configuration files

**Usage:**

```bash
# Create a configuration template
python workspace_config.py --create-template config_template.json

# Validate a configuration
python workspace_config.py --config config.json --validate

# Display configuration
python workspace_config.py --config config.json --display

# Validate and display
python workspace_config.py --config config.json
```

**Configuration Format:**

```json
{
  "workspace_name": "my-workspace",
  "workspace_id": "workspace-guid",
  "environment": "development",
  "lakehouses": [
    {
      "name": "lakehouse1",
      "id": "lakehouse-guid"
    }
  ],
  "data_sources": {
    "source1": {
      "type": "azure_storage",
      "connection_string": "connection-string"
    }
  }
}
```

## Requirements

- Python 3.8 or higher
- Standard library only (no external dependencies for basic tools)

## Best Practices

1. **Version Control**: Keep configuration files in version control
2. **Secrets Management**: Never commit connection strings or secrets
3. **Environment Separation**: Use separate configs for each environment
4. **Validation**: Always validate configs before deployment
5. **Documentation**: Document any custom configuration fields

## Security Notes

- Store sensitive values (connection strings, API keys) in Azure Key Vault
- Use environment variables for secrets in CI/CD pipelines
- Regularly rotate credentials
- Limit access to production configurations
