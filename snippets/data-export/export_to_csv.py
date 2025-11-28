"""
Export DataFrame to CSV Format

Description:
    Export a Spark DataFrame to CSV format with configurable options

Prerequisites:
    - Lakehouse must be attached to the notebook
    - Sufficient storage space available

Parameters:
    - df: Spark DataFrame to export
    - output_path: Target path for CSV files
    - options: CSV writer options (header, delimiter, etc.)
    - coalesce: Number of output files (optional)

Example Usage:
    export_to_csv(
        df=report_df,
        output_path="Files/exports/reports",
        options={'header': True, 'delimiter': ','},
        coalesce=1
    )
"""

from pyspark.sql import DataFrame
from typing import Dict, Any, Optional

def export_to_csv(
    df: DataFrame,
    output_path: str,
    options: Optional[Dict[str, Any]] = None,
    coalesce: Optional[int] = None,
    mode: str = "overwrite"
):
    """
    Export DataFrame to CSV format
    
    Args:
        df: Spark DataFrame to export
        output_path: Target path for output
        options: CSV writer options
        coalesce: Number of output files
        mode: Write mode (overwrite, append, error, ignore)
    """
    # Default options
    default_options = {
        'header': True,
        'delimiter': ',',
        'encoding': 'UTF-8',
        'quote': '"',
        'escape': '\\'
    }
    
    # Merge with provided options
    if options:
        default_options.update(options)
    
    # Apply coalesce if specified
    df_to_write = df.coalesce(coalesce) if coalesce else df
    
    try:
        writer = df_to_write.write.format("csv").mode(mode)
        
        for key, value in default_options.items():
            writer = writer.option(key, value)
        
        writer.save(output_path)
        
        print(f"Successfully exported {df.count()} rows to {output_path}")
        if coalesce:
            print(f"Coalesced to {coalesce} file(s)")
    except Exception as e:
        print(f"Error exporting to {output_path}: {str(e)}")
        raise

def export_single_csv(
    df: DataFrame,
    output_path: str,
    filename: str = "output.csv",
    options: Optional[Dict[str, Any]] = None
):
    """
    Export DataFrame to a single CSV file
    
    Args:
        df: Spark DataFrame to export
        output_path: Directory path for output
        filename: Name of the output file
        options: CSV writer options
    """
    import os
    
    # Export with coalesce=1
    temp_path = f"{output_path}_temp"
    export_to_csv(df, temp_path, options, coalesce=1)
    
    # Note: In Fabric, you may need to use notebookutils for file operations
    print(f"CSV exported to: {temp_path}")
    print(f"Note: Rename the part file to {filename} using file utilities")

# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    # Create sample DataFrame
    data = [
        ("001", "Alice", "Sales", 50000),
        ("002", "Bob", "Marketing", 45000),
        ("003", "Charlie", "Engineering", 60000)
    ]
    columns = ["id", "name", "department", "salary"]
    sample_df = spark.createDataFrame(data, columns)
    
    # Export to CSV
    export_to_csv(
        df=sample_df,
        output_path="Files/exports/employees",
        options={
            'header': True,
            'delimiter': ','
        },
        coalesce=1
    )
