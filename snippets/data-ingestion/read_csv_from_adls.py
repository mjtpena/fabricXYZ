"""
Read CSV from Azure Data Lake Storage (ADLS)

Description:
    Read CSV files from ADLS Gen2 into a Spark DataFrame with various options

Prerequisites:
    - ADLS connection configured in Fabric workspace
    - Appropriate access permissions

Parameters:
    - storage_account: Storage account name
    - container: Container name
    - file_path: Path to CSV file(s)
    - options: CSV reader options (header, delimiter, etc.)

Returns:
    Spark DataFrame with CSV data

Example Usage:
    df = read_csv_from_adls(
        storage_account="mystorageaccount",
        container="raw-data",
        file_path="sales/2024/*.csv",
        options={'header': True, 'inferSchema': True}
    )
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any

def read_csv_from_adls(
    storage_account: str,
    container: str,
    file_path: str,
    options: Dict[str, Any] = None
) -> DataFrame:
    """
    Read CSV files from Azure Data Lake Storage
    
    Args:
        storage_account: Azure storage account name
        container: Container name
        file_path: Path to file(s), supports wildcards
        options: CSV reader options
        
    Returns:
        DataFrame with CSV data
    """
    spark = SparkSession.builder.getOrCreate()
    
    # Default options
    default_options = {
        'header': True,
        'inferSchema': True,
        'delimiter': ',',
        'encoding': 'UTF-8'
    }
    
    # Merge with provided options
    if options:
        default_options.update(options)
    
    # Construct ADLS path
    # Note: In Fabric, you might use abfss:// protocol
    adls_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{file_path}"
    
    try:
        # Read CSV with options
        df = spark.read.options(**default_options).csv(adls_path)
        
        row_count = df.count()
        col_count = len(df.columns)
        print(f"Successfully read CSV from: {adls_path}")
        print(f"Rows: {row_count}, Columns: {col_count}")
        
        return df
        
    except Exception as e:
        print(f"Error reading CSV from {adls_path}: {str(e)}")
        raise

def read_csv_with_schema(
    storage_account: str,
    container: str,
    file_path: str,
    schema: str
) -> DataFrame:
    """
    Read CSV with explicit schema definition
    
    Args:
        storage_account: Azure storage account name
        container: Container name
        file_path: Path to file(s)
        schema: Schema string in DDL format
        
    Returns:
        DataFrame with CSV data
    """
    spark = SparkSession.builder.getOrCreate()
    
    adls_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{file_path}"
    
    try:
        df = spark.read \
            .schema(schema) \
            .option('header', True) \
            .csv(adls_path)
        
        print(f"Successfully read CSV with schema from: {adls_path}")
        return df
        
    except Exception as e:
        print(f"Error reading CSV: {str(e)}")
        raise

# Example usage
if __name__ == "__main__":
    # Example 1: Read with inferred schema
    df1 = read_csv_from_adls(
        storage_account="mystorageaccount",
        container="raw-data",
        file_path="sales/sales_2024.csv",
        options={
            'header': True,
            'inferSchema': True,
            'delimiter': ','
        }
    )
    df1.printSchema()
    df1.show(5)
    
    # Example 2: Read with explicit schema
    schema_ddl = """
        transaction_id STRING,
        transaction_date DATE,
        customer_id STRING,
        amount DECIMAL(10,2)
    """
    
    df2 = read_csv_with_schema(
        storage_account="mystorageaccount",
        container="raw-data",
        file_path="sales/*.csv",
        schema=schema_ddl
    )
    df2.printSchema()
    df2.show(5)
