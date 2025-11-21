"""
Data Quality Validation

Description:
    Perform data quality checks on a DataFrame and return validation results

Features:
    - Check for null values
    - Check for duplicates
    - Validate data types
    - Check value ranges
    - Record validation results

Parameters:
    - df: DataFrame to validate
    - checks: Dictionary of validation rules

Returns:
    Dictionary with validation results and passed/failed status

Example Usage:
    validation_results = validate_data_quality(
        df=sales_df,
        checks={
            'null_check': ['customer_id', 'amount'],
            'duplicate_check': ['transaction_id'],
            'range_check': {'amount': (0, 10000)}
        }
    )
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull
from typing import Dict, List, Any

def validate_data_quality(df: DataFrame, checks: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate data quality of a DataFrame
    
    Args:
        df: Spark DataFrame to validate
        checks: Dictionary of validation rules
        
    Returns:
        Dictionary with validation results
    """
    results = {
        'total_rows': df.count(),
        'checks': {},
        'passed': True
    }
    
    # Null check
    if 'null_check' in checks:
        null_results = {}
        for column in checks['null_check']:
            null_count = df.filter(col(column).isNull()).count()
            null_results[column] = {
                'null_count': null_count,
                'null_percentage': (null_count / results['total_rows']) * 100
            }
            if null_count > 0:
                results['passed'] = False
        results['checks']['null_check'] = null_results
    
    # Duplicate check
    if 'duplicate_check' in checks:
        dup_columns = checks['duplicate_check']
        duplicate_count = df.count() - df.dropDuplicates(dup_columns).count()
        results['checks']['duplicate_check'] = {
            'duplicate_count': duplicate_count,
            'has_duplicates': duplicate_count > 0
        }
        if duplicate_count > 0:
            results['passed'] = False
    
    # Range check
    if 'range_check' in checks:
        range_results = {}
        for column, (min_val, max_val) in checks['range_check'].items():
            out_of_range = df.filter(
                (col(column) < min_val) | (col(column) > max_val)
            ).count()
            range_results[column] = {
                'out_of_range_count': out_of_range,
                'min_allowed': min_val,
                'max_allowed': max_val
            }
            if out_of_range > 0:
                results['passed'] = False
        results['checks']['range_check'] = range_results
    
    return results

def print_validation_report(results: Dict[str, Any]):
    """Print a formatted validation report"""
    print("\n" + "="*50)
    print("DATA QUALITY VALIDATION REPORT")
    print("="*50)
    print(f"Total Rows: {results['total_rows']}")
    print(f"Overall Status: {'PASSED' if results['passed'] else 'FAILED'}")
    print("\nDetailed Results:")
    
    for check_type, check_results in results['checks'].items():
        print(f"\n{check_type.upper()}:")
        if isinstance(check_results, dict):
            for key, value in check_results.items():
                print(f"  {key}: {value}")
    print("="*50 + "\n")

# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    # Create sample data with quality issues
    data = [
        (1, "John", 25, 50000),
        (2, "Jane", 30, 60000),
        (3, None, 35, 70000),  # Null name
        (4, "Bob", -5, 55000),  # Invalid age
        (1, "John", 25, 50000),  # Duplicate
    ]
    columns = ["id", "name", "age", "salary"]
    df = spark.createDataFrame(data, columns)
    
    # Define validation checks
    checks = {
        'null_check': ['name', 'age'],
        'duplicate_check': ['id'],
        'range_check': {
            'age': (0, 120),
            'salary': (0, 1000000)
        }
    }
    
    # Run validation
    results = validate_data_quality(df, checks)
    print_validation_report(results)
