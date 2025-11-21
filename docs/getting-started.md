# Getting Started with fabricXYZ

This guide will help you get started with the fabricXYZ toolbox collection.

## Overview

fabricXYZ is a collection of resources for Microsoft Fabric including:
- **Accelerators**: Complete project templates
- **Snippets**: Reusable code samples
- **Toolbox**: Utility scripts and tools

## Prerequisites

Before using the resources in this repository, ensure you have:

1. **Microsoft Fabric Access**
   - Active Microsoft Fabric workspace
   - Appropriate permissions (Contributor or Admin)
   - Understanding of Fabric concepts (Lakehouse, Warehouse, etc.)

2. **Development Environment**
   - Python 3.8 or higher (for Python-based tools)
   - Access to Fabric notebooks
   - Git (for cloning the repository)

3. **Knowledge Requirements**
   - Basic understanding of PySpark
   - Familiarity with Delta Lake format
   - SQL knowledge for data transformations

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/mjtpena/fabricXYZ.git
cd fabricXYZ
```

### 2. Browse Available Resources

- Check `/accelerators` for complete project templates
- Look in `/snippets` for code samples
- Explore `/toolbox` for utility scripts

### 3. Use an Accelerator

Example: Using the Lakehouse Starter

```bash
cd accelerators/lakehouse-starter
# Read the README.md for specific instructions
```

### 4. Use a Code Snippet

Example: Using PySpark snippets in a Fabric notebook

1. Open a Fabric notebook
2. Copy the snippet code from `snippets/pyspark/`
3. Customize parameters for your environment
4. Run the code

### 5. Use a Toolbox Utility

Example: Managing workspace configuration

```bash
cd toolbox/deployment
python workspace_config.py --create-template my_config.json
# Edit my_config.json with your details
python workspace_config.py --config my_config.json --validate
```

## Common Workflows

### Setting Up a New Lakehouse

1. Use the `lakehouse-starter` accelerator as a template
2. Customize the table definitions in `scripts/create_tables.sql`
3. Modify the configuration in `config/connections.json`
4. Deploy to your Fabric workspace

### Adding Data Ingestion

1. Browse `snippets/data-ingestion/`
2. Find a snippet that matches your data source
3. Copy to your notebook
4. Update connection details
5. Test the ingestion

### Data Transformation

1. Check `snippets/pyspark/` for transformation patterns
2. Use `data_quality_validation.py` to ensure data quality
3. Apply transformations using Delta Lake operations

## Best Practices

### For Accelerators

- Always read the accelerator's README first
- Customize configurations before deployment
- Test in a development environment
- Document any modifications you make

### For Snippets

- Understand what the snippet does before using it
- Adapt to your specific use case
- Add error handling for production use
- Test with sample data first

### For Toolbox Items

- Review tool documentation
- Validate configurations
- Test in non-production first
- Keep backups when using data modification tools

## Repository Structure

```
fabricXYZ/
├── README.md                   # Main documentation
├── CONTRIBUTING.md             # Contribution guidelines
├── LICENSE                     # MIT License
├── accelerators/              # Project templates
│   ├── README.md
│   └── lakehouse-starter/
├── snippets/                   # Code snippets
│   ├── README.md
│   ├── pyspark/
│   ├── notebooks/
│   └── data-ingestion/
├── toolbox/                    # Utility tools
│   ├── README.md
│   └── deployment/
└── docs/                       # Documentation
    ├── README.md
    └── getting-started.md      # This file
```

## Next Steps

1. **Explore Accelerators**: Start with a template that matches your use case
2. **Learn from Snippets**: Study the code examples to understand patterns
3. **Customize**: Adapt resources to your specific requirements
4. **Contribute**: Share your improvements back to the community

## Getting Help

- Check the README files in each directory
- Review inline documentation in code files
- Open an issue on GitHub for bugs or questions
- Consult [Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)

## Additional Resources

- [Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Microsoft Fabric Community](https://community.fabric.microsoft.com/)

## Tips for Success

1. **Start Small**: Begin with simple snippets before using full accelerators
2. **Understand the Code**: Don't just copy-paste, understand what it does
3. **Test Thoroughly**: Always test in dev/test before production
4. **Document Changes**: Keep track of modifications you make
5. **Stay Updated**: Check back for new additions to the repository

Happy building with Microsoft Fabric! 🚀
