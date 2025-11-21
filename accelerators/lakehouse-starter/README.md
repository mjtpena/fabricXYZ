# Lakehouse Starter Accelerator

A complete starter template for creating a Microsoft Fabric Lakehouse solution with data ingestion, transformation, and reporting capabilities.

## Overview

This accelerator provides a foundation for building a Lakehouse architecture in Microsoft Fabric. It includes:

- Sample data ingestion pipeline
- Bronze, Silver, Gold data architecture
- PySpark transformation examples
- Sample notebook for data exploration
- Configuration templates

## Architecture

```
Data Sources → Bronze Layer → Silver Layer → Gold Layer → Reports/Analytics
                (Raw Data)   (Cleaned)      (Aggregated)
```

## Prerequisites

- Microsoft Fabric workspace
- Contributor or Admin permissions
- Basic understanding of PySpark and SQL

## Setup Instructions

### 1. Create Lakehouse

1. Open your Microsoft Fabric workspace
2. Create a new Lakehouse named `starter_lakehouse`
3. Note the workspace and lakehouse IDs

### 2. Configure Connections

1. Update `config/connections.json` with your connection details
2. Configure data source credentials if needed

### 3. Deploy Notebooks

1. Upload notebooks from the `notebooks/` directory to your Fabric workspace
2. Configure notebook dependencies

### 4. Run Initial Load

1. Open `00_setup.ipynb` notebook
2. Execute all cells to create the initial structure
3. Run `01_bronze_ingestion.ipynb` to load sample data

## Structure

```
lakehouse-starter/
├── README.md                   # This file
├── config/
│   └── connections.json       # Connection configurations
├── notebooks/
│   ├── 00_setup.ipynb        # Initial setup
│   ├── 01_bronze_ingestion.ipynb
│   ├── 02_silver_transformation.ipynb
│   └── 03_gold_aggregation.ipynb
└── scripts/
    └── create_tables.sql     # SQL table definitions
```

## Data Layers

### Bronze Layer
- Raw data ingestion
- Minimal transformations
- Schema-on-read approach

### Silver Layer
- Cleaned and validated data
- Type conversions
- Business rule application

### Gold Layer
- Aggregated metrics
- Analytics-ready datasets
- Optimized for reporting

## Customization

Modify the following for your needs:

1. **Data Sources**: Update ingestion notebooks with your sources
2. **Transformations**: Customize Silver layer logic in transformation notebooks
3. **Aggregations**: Modify Gold layer metrics and dimensions
4. **Scheduling**: Set up pipeline schedules in Fabric

## Best Practices

- Use incremental loading where possible
- Implement data quality checks at each layer
- Partition large tables appropriately
- Monitor performance and optimize queries
- Document schema changes

## Troubleshooting

### Common Issues

**Issue**: Connection failures
- **Solution**: Verify credentials and network connectivity

**Issue**: Slow transformations
- **Solution**: Check data volumes, add partitioning, optimize Spark settings

**Issue**: Schema conflicts
- **Solution**: Ensure consistent schema evolution practices

## Next Steps

1. Customize the sample notebooks for your data sources
2. Add data quality validation
3. Implement error handling and logging
4. Create Power BI reports on Gold layer
5. Set up automated pipelines

## Resources

- [Microsoft Fabric Lakehouse Documentation](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-overview)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)

## Contributing

Found improvements? Please contribute back to the main repository!
