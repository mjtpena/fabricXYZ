"""
Notebook Utilities for Microsoft Fabric

Description:
    Collection of utility functions for working with Fabric notebooks

Features:
    - Display formatted tables
    - Progress tracking
    - Logging helpers
    - Parameter extraction
    - HTML rendering

Example Usage:
    from notebook_utils import display_dataframe_summary, log_step
    
    log_step("Starting data processing")
    display_dataframe_summary(df, "Sales Data")
"""

from pyspark.sql import DataFrame
from typing import Any, Dict, List
from datetime import datetime
import json

def display_dataframe_summary(df: DataFrame, title: str = "DataFrame Summary"):
    """
    Display a formatted summary of a DataFrame
    
    Args:
        df: Spark DataFrame to summarize
        title: Title for the summary display
    """
    print("\n" + "="*60)
    print(f" {title}")
    print("="*60)
    
    print(f"\nRow Count: {df.count():,}")
    print(f"Column Count: {len(df.columns)}")
    
    print("\nSchema:")
    df.printSchema()
    
    print("\nSample Data (first 5 rows):")
    df.show(5, truncate=False)
    
    print("\nColumn Statistics:")
    df.describe().show()
    
    print("="*60 + "\n")

def log_step(message: str, level: str = "INFO"):
    """
    Log a step with timestamp
    
    Args:
        message: Log message
        level: Log level (INFO, WARNING, ERROR)
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")

def create_progress_tracker(total_steps: int):
    """
    Create a simple progress tracker
    
    Args:
        total_steps: Total number of steps
        
    Returns:
        Tracker function
    """
    completed = [0]
    
    def track(step_name: str):
        completed[0] += 1
        percentage = (completed[0] / total_steps) * 100
        print(f"[{completed[0]}/{total_steps}] ({percentage:.1f}%) - {step_name}")
    
    return track

def display_dict_as_table(data: Dict[str, Any], title: str = "Configuration"):
    """
    Display a dictionary as a formatted table
    
    Args:
        data: Dictionary to display
        title: Table title
    """
    print("\n" + "="*60)
    print(f" {title}")
    print("="*60)
    
    max_key_length = max(len(str(k)) for k in data.keys())
    
    for key, value in data.items():
        if isinstance(value, (dict, list)):
            value_str = json.dumps(value, indent=2)
        else:
            value_str = str(value)
        
        print(f"{str(key).ljust(max_key_length)} : {value_str}")
    
    print("="*60 + "\n")

def get_notebook_parameters() -> Dict[str, Any]:
    """
    Get notebook parameters (for parameterized notebooks)
    
    Returns:
        Dictionary of parameters
    """
    # In Fabric, you can access parameters using the taskUtils
    # This is a placeholder implementation
    try:
        # Example: from notebookutils import mssparkutils
        # return mssparkutils.notebook.getParameters()
        return {}
    except Exception as e:
        log_step(f"Could not retrieve parameters: {str(e)}", "WARNING")
        return {}

def measure_execution_time(func):
    """
    Decorator to measure function execution time
    
    Args:
        func: Function to measure
        
    Returns:
        Wrapped function
    """
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        log_step(f"Starting: {func.__name__}")
        
        result = func(*args, **kwargs)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        log_step(f"Completed: {func.__name__} (Duration: {duration:.2f}s)")
        
        return result
    
    return wrapper

def display_error_report(errors: List[Dict[str, Any]]):
    """
    Display a formatted error report
    
    Args:
        errors: List of error dictionaries
    """
    if not errors:
        print("No errors to report.")
        return
    
    print("\n" + "="*60)
    print(f" ERROR REPORT ({len(errors)} errors)")
    print("="*60)
    
    for i, error in enumerate(errors, 1):
        print(f"\nError #{i}:")
        print(f"  Type: {error.get('type', 'Unknown')}")
        print(f"  Message: {error.get('message', 'No message')}")
        print(f"  Timestamp: {error.get('timestamp', 'Unknown')}")
        if 'details' in error:
            print(f"  Details: {error['details']}")
    
    print("\n" + "="*60 + "\n")

# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    # Example 1: DataFrame summary
    data = [(1, "Alice", 30), (2, "Bob", 25), (3, "Charlie", 35)]
    df = spark.createDataFrame(data, ["id", "name", "age"])
    display_dataframe_summary(df, "Sample Data")
    
    # Example 2: Progress tracking
    tracker = create_progress_tracker(5)
    tracker("Load data")
    tracker("Transform data")
    tracker("Validate data")
    tracker("Write results")
    tracker("Complete")
    
    # Example 3: Display configuration
    config = {
        "environment": "production",
        "batch_size": 1000,
        "partitions": 10,
        "retry_attempts": 3
    }
    display_dict_as_table(config, "Processing Configuration")
    
    # Example 4: Measure execution time
    @measure_execution_time
    def process_data():
        import time
        time.sleep(2)  # Simulate processing
        return "Done"
    
    result = process_data()
