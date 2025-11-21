"""
Write DataFrame to Delta Table with Partitioning

Description:
    Write a Spark DataFrame to a Delta table with optional partitioning
    and write mode specification

Prerequisites:
    - Lakehouse must be attached to the notebook
    - Sufficient storage space available

Parameters:
    - df: Spark DataFrame to write
    - table_name: Target table name
    - partition_by: Column(s) to partition by (optional)
    - mode: Write mode (overwrite, append, error, ignore)

Example Usage:
    write_delta_table(
        df=sales_df,
        table_name="silver.sales",
        partition_by=["transaction_date"],
        mode="append"
    )
"""

from pyspark.sql import DataFrame
from typing import List, Optional

def write_delta_table(
    df: DataFrame,
    table_name: str,
    partition_by: Optional[List[str]] = None,
    mode: str = "append"
):
    """
    Write DataFrame to Delta table
    
    Args:
        df: Spark DataFrame to write
        table_name: Target table name
        partition_by: List of columns to partition by
        mode: Write mode (overwrite, append, error, ignore)
    """
    
    writer = df.write.format("delta").mode(mode)
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    
    try:
        writer.saveAsTable(table_name)
        print(f"Successfully wrote {df.count()} rows to {table_name}")
        if partition_by:
            print(f"Partitioned by: {', '.join(partition_by)}")
    except Exception as e:
        print(f"Error writing to table {table_name}: {str(e)}")
        raise

# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import current_timestamp
    
    spark = SparkSession.builder.getOrCreate()
    
    # Create sample DataFrame
    data = [
        ("001", "2024-01-01", 100.0),
        ("002", "2024-01-01", 150.0),
        ("003", "2024-01-02", 200.0)
    ]
    columns = ["id", "date", "amount"]
    sample_df = spark.createDataFrame(data, columns)
    
    # Add processing timestamp
    sample_df = sample_df.withColumn("processed_at", current_timestamp())
    
    # Write to Delta table
    write_delta_table(
        df=sample_df,
        table_name="bronze.sample_data",
        partition_by=["date"],
        mode="overwrite"
    )
