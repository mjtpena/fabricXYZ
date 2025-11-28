"""
Data Masking Utilities

Description:
    Functions for masking sensitive data in DataFrames before sharing or exporting

Prerequisites:
    - PySpark environment
    - Understanding of data sensitivity requirements

Features:
    - Email masking
    - Phone number masking
    - Credit card masking
    - Custom pattern masking

Example Usage:
    masked_df = mask_email(df, "email_column")
    masked_df = mask_multiple_columns(df, masking_rules)
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, regexp_replace, lit, concat, substring
from typing import Dict, List

def mask_email(df: DataFrame, column_name: str) -> DataFrame:
    """
    Mask email addresses (show first 2 chars and domain)
    
    Args:
        df: Input DataFrame
        column_name: Name of column containing emails
        
    Returns:
        DataFrame with masked emails
        
    Example:
        john.doe@example.com -> jo***@example.com
    """
    return df.withColumn(
        column_name,
        regexp_replace(
            col(column_name),
            r'^(.{2})([^@]*)(@.*)$',
            r'$1***$3'
        )
    )

def mask_phone(df: DataFrame, column_name: str) -> DataFrame:
    """
    Mask phone numbers (show last 4 digits only)
    
    Args:
        df: Input DataFrame
        column_name: Name of column containing phone numbers
        
    Returns:
        DataFrame with masked phone numbers
        
    Example:
        555-123-4567 -> ***-***-4567
    """
    return df.withColumn(
        column_name,
        regexp_replace(
            col(column_name),
            r'.*(.{4})$',
            r'***-***-$1'
        )
    )

def mask_credit_card(df: DataFrame, column_name: str) -> DataFrame:
    """
    Mask credit card numbers (show last 4 digits only)
    
    Args:
        df: Input DataFrame
        column_name: Name of column containing credit card numbers
        
    Returns:
        DataFrame with masked credit card numbers
        
    Example:
        4111111111111111 -> ************1111
    """
    return df.withColumn(
        column_name,
        regexp_replace(
            col(column_name),
            r'.*(....)',
            r'************$1'
        )
    )

def mask_ssn(df: DataFrame, column_name: str) -> DataFrame:
    """
    Mask Social Security Numbers (show last 4 digits only)
    
    Args:
        df: Input DataFrame
        column_name: Name of column containing SSN
        
    Returns:
        DataFrame with masked SSN
        
    Example:
        123-45-6789 -> ***-**-6789
    """
    return df.withColumn(
        column_name,
        regexp_replace(
            col(column_name),
            r'^\d{3}-?\d{2}-?(\d{4})$',
            r'***-**-$1'
        )
    )

def mask_with_pattern(
    df: DataFrame,
    column_name: str,
    pattern: str,
    replacement: str
) -> DataFrame:
    """
    Mask column using custom regex pattern
    
    Args:
        df: Input DataFrame
        column_name: Name of column to mask
        pattern: Regex pattern to match
        replacement: Replacement string
        
    Returns:
        DataFrame with masked column
    """
    return df.withColumn(
        column_name,
        regexp_replace(col(column_name), pattern, replacement)
    )

def mask_multiple_columns(
    df: DataFrame,
    masking_rules: Dict[str, str]
) -> DataFrame:
    """
    Apply masking to multiple columns based on rules
    
    Args:
        df: Input DataFrame
        masking_rules: Dictionary mapping column names to masking types
                      Supported types: 'email', 'phone', 'credit_card', 'ssn'
        
    Returns:
        DataFrame with all specified columns masked
        
    Example:
        masking_rules = {
            'customer_email': 'email',
            'phone_number': 'phone',
            'card_number': 'credit_card'
        }
    """
    masking_functions = {
        'email': mask_email,
        'phone': mask_phone,
        'credit_card': mask_credit_card,
        'ssn': mask_ssn
    }
    
    result_df = df
    
    for column, mask_type in masking_rules.items():
        if mask_type in masking_functions:
            result_df = masking_functions[mask_type](result_df, column)
        else:
            print(f"Warning: Unknown masking type '{mask_type}' for column '{column}'")
    
    return result_df

def redact_column(df: DataFrame, column_name: str) -> DataFrame:
    """
    Completely redact a column (replace all values with [REDACTED])
    
    Args:
        df: Input DataFrame
        column_name: Name of column to redact
        
    Returns:
        DataFrame with redacted column
    """
    return df.withColumn(column_name, lit("[REDACTED]"))

# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    # Create sample data with sensitive information
    data = [
        (1, "john.doe@example.com", "555-123-4567", "4111111111111111"),
        (2, "jane.smith@test.org", "555-987-6543", "5500000000000004"),
        (3, "bob.wilson@company.net", "555-456-7890", "340000000000009")
    ]
    columns = ["id", "email", "phone", "credit_card"]
    df = spark.createDataFrame(data, columns)
    
    print("Original Data:")
    df.show(truncate=False)
    
    # Apply masking rules
    masking_rules = {
        'email': 'email',
        'phone': 'phone',
        'credit_card': 'credit_card'
    }
    
    masked_df = mask_multiple_columns(df, masking_rules)
    
    print("\nMasked Data:")
    masked_df.show(truncate=False)
