"""
Export DataFrame to Parquet Format

Description:
    Export a Spark DataFrame to Parquet format with optional compression
    and partitioning support

Prerequisites:
    - Lakehouse must be attached to the notebook
    - Sufficient storage space available

Parameters:
    - df: Spark DataFrame to export
    - output_path: Target path for Parquet files
    - partition_by: Column(s) to partition by (optional)
    - compression: Compression codec (snappy, gzip, lz4, zstd)

Example Usage:
    export_to_parquet(
        df=sales_df,
        output_path="Files/exports/sales",
        partition_by=["year", "month"],
        compression="snappy"
    )
"""

from pyspark.sql import DataFrame
from typing import List, Optional

def export_to_parquet(
    df: DataFrame,
    output_path: str,
    partition_by: Optional[List[str]] = None,
    compression: str = "snappy",
    mode: str = "overwrite"
):
    """
    Export DataFrame to Parquet format
    
    Args:
        df: Spark DataFrame to export
        output_path: Target path for output
        partition_by: List of columns to partition by
        compression: Compression codec
        mode: Write mode (overwrite, append, error, ignore)
    """
    valid_compressions = ['snappy', 'gzip', 'lz4', 'zstd', 'none']
    if compression not in valid_compressions:
        raise ValueError(f"Invalid compression: {compression}. Must be one of {valid_compressions}")
    
    writer = df.write.format("parquet").mode(mode)
    writer = writer.option("compression", compression)
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    
    try:
        writer.save(output_path)
        print(f"Successfully exported {df.count()} rows to {output_path}")
        print(f"Compression: {compression}")
        if partition_by:
            print(f"Partitioned by: {', '.join(partition_by)}")
    except Exception as e:
        print(f"Error exporting to {output_path}: {str(e)}")
        raise

def export_to_parquet_with_schema(
    df: DataFrame,
    output_path: str,
    schema_path: str,
    compression: str = "snappy"
):
    """
    Export DataFrame to Parquet and save schema separately
    
    Args:
        df: Spark DataFrame to export
        output_path: Target path for Parquet files
        schema_path: Path to save schema JSON
        compression: Compression codec
    """
    import json
    
    # Export data
    export_to_parquet(df, output_path, compression=compression)
    
    # Save schema
    schema_json = df.schema.json()
    with open(schema_path, 'w') as f:
        f.write(schema_json)
    
    print(f"Schema saved to: {schema_path}")

# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    # Create sample DataFrame
    data = [
        ("001", "2024", "01", 100.0),
        ("002", "2024", "01", 150.0),
        ("003", "2024", "02", 200.0)
    ]
    columns = ["id", "year", "month", "amount"]
    sample_df = spark.createDataFrame(data, columns)
    
    # Export with partitioning
    export_to_parquet(
        df=sample_df,
        output_path="Files/exports/sample_data",
        partition_by=["year", "month"],
        compression="snappy"
    )
