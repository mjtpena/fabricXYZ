"""
Read Delta Table from Lakehouse

Description:
    Read a Delta table from Microsoft Fabric Lakehouse into a Spark DataFrame

Prerequisites:
    - Lakehouse must be attached to the notebook
    - Table must exist in the Lakehouse

Parameters:
    - table_name: Name of the table to read
    - lakehouse_name: Optional lakehouse name (uses default if not specified)

Returns:
    Spark DataFrame containing the table data

Example Usage:
    df = read_delta_table("silver.customers")
    df.show(10)
"""

from pyspark.sql import SparkSession

def read_delta_table(table_name, lakehouse_name=None):
    """
    Read a Delta table from Fabric Lakehouse
    
    Args:
        table_name (str): Name of the table (e.g., 'schema.table')
        lakehouse_name (str, optional): Lakehouse name
        
    Returns:
        DataFrame: Spark DataFrame with table data
    """
    spark = SparkSession.builder.getOrCreate()
    
    if lakehouse_name:
        full_path = f"{lakehouse_name}.{table_name}"
    else:
        full_path = table_name
    
    try:
        df = spark.read.format("delta").table(full_path)
        print(f"Successfully read table: {full_path}")
        print(f"Row count: {df.count()}")
        return df
    except Exception as e:
        print(f"Error reading table {full_path}: {str(e)}")
        raise

# Example usage
if __name__ == "__main__":
    # Read a table
    df = read_delta_table("silver.sales")
    
    # Display schema
    df.printSchema()
    
    # Show sample data
    df.show(5)
