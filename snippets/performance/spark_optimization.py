"""
Spark Performance Optimization Utilities

Description:
    Functions and configurations for optimizing Spark performance in Microsoft Fabric

Prerequisites:
    - Understanding of Spark architecture
    - Access to Spark configuration

Features:
    - Optimal configuration settings
    - Partition management
    - Caching strategies
    - Query optimization helpers

Example Usage:
    apply_performance_config(spark)
    df = optimize_partitions(df, target_partitions=200)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, spark_partition_id
from typing import Dict, Any, List

def apply_performance_config(spark: SparkSession, config: Dict[str, Any] = None):
    """
    Apply recommended performance configurations to Spark session
    
    Args:
        spark: Active SparkSession
        config: Optional custom configuration overrides
    """
    # Default recommended configurations
    default_config = {
        # Adaptive Query Execution
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        
        # Shuffle optimizations
        "spark.sql.shuffle.partitions": "auto",
        
        # Memory management
        "spark.sql.autoBroadcastJoinThreshold": "10485760",  # 10MB
        
        # Predicate pushdown
        "spark.sql.parquet.filterPushdown": "true",
        "spark.sql.orc.filterPushdown": "true"
    }
    
    # Merge with custom config
    if config:
        default_config.update(config)
    
    # Apply configurations
    for key, value in default_config.items():
        spark.conf.set(key, value)
        print(f"Set {key} = {value}")
    
    print("\nPerformance configuration applied successfully")

def get_partition_info(df: DataFrame) -> Dict[str, Any]:
    """
    Get partition information for a DataFrame
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Dictionary with partition statistics
    """
    partition_count = df.rdd.getNumPartitions()
    
    # Get row counts per partition
    partition_stats = df.withColumn("partition_id", spark_partition_id()) \
        .groupBy("partition_id") \
        .count() \
        .collect()
    
    row_counts = [row['count'] for row in partition_stats]
    
    return {
        'partition_count': partition_count,
        'total_rows': sum(row_counts),
        'avg_rows_per_partition': sum(row_counts) / partition_count if partition_count > 0 else 0,
        'min_rows': min(row_counts) if row_counts else 0,
        'max_rows': max(row_counts) if row_counts else 0,
        'skew_ratio': max(row_counts) / min(row_counts) if row_counts and min(row_counts) > 0 else 0
    }

def optimize_partitions(
    df: DataFrame,
    target_partitions: int = None,
    target_rows_per_partition: int = 1000000
) -> DataFrame:
    """
    Optimize DataFrame partitioning
    
    Args:
        df: DataFrame to optimize
        target_partitions: Target number of partitions (optional)
        target_rows_per_partition: Target rows per partition for auto-calculation
        
    Returns:
        Repartitioned DataFrame
    """
    current_partitions = df.rdd.getNumPartitions()
    
    if target_partitions is None:
        # Auto-calculate based on data size
        row_count = df.count()
        target_partitions = max(1, row_count // target_rows_per_partition)
    
    print(f"Current partitions: {current_partitions}")
    print(f"Target partitions: {target_partitions}")
    
    if target_partitions < current_partitions:
        # Use coalesce to reduce partitions (no shuffle)
        result_df = df.coalesce(target_partitions)
        print("Used coalesce to reduce partitions")
    else:
        # Use repartition to increase partitions
        result_df = df.repartition(target_partitions)
        print("Used repartition to increase partitions")
    
    return result_df

def cache_dataframe(df: DataFrame, storage_level: str = "MEMORY_AND_DISK") -> DataFrame:
    """
    Cache DataFrame with specified storage level
    
    Args:
        df: DataFrame to cache
        storage_level: Storage level (MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY, etc.)
        
    Returns:
        Cached DataFrame
    """
    from pyspark import StorageLevel
    
    storage_levels = {
        "MEMORY_ONLY": StorageLevel.MEMORY_ONLY,
        "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK,
        "DISK_ONLY": StorageLevel.DISK_ONLY,
        "MEMORY_ONLY_SER": StorageLevel.MEMORY_ONLY_SER,
        "MEMORY_AND_DISK_SER": StorageLevel.MEMORY_AND_DISK_SER
    }
    
    level = storage_levels.get(storage_level, StorageLevel.MEMORY_AND_DISK)
    df.persist(level)
    
    # Force materialization
    df.count()
    
    print(f"DataFrame cached with storage level: {storage_level}")
    return df

def analyze_query_plan(df: DataFrame, extended: bool = False):
    """
    Print and analyze query execution plan
    
    Args:
        df: DataFrame to analyze
        extended: If True, show extended plan with statistics
    """
    print("\n" + "="*60)
    print("QUERY EXECUTION PLAN")
    print("="*60)
    
    if extended:
        df.explain(mode="extended")
    else:
        df.explain(mode="simple")
    
    print("="*60 + "\n")

def get_broadcast_hint(df: DataFrame) -> DataFrame:
    """
    Add broadcast hint to DataFrame for join optimization
    
    Use this for small DataFrames that will be joined with larger ones.
    
    Args:
        df: Small DataFrame to broadcast
        
    Returns:
        DataFrame with broadcast hint
    """
    from pyspark.sql.functions import broadcast
    return broadcast(df)

def profile_dataframe(df: DataFrame, sample_size: float = 0.1) -> Dict[str, Any]:
    """
    Profile DataFrame for performance optimization insights
    
    Args:
        df: DataFrame to profile
        sample_size: Fraction of data to sample for profiling
        
    Returns:
        Profiling results dictionary
    """
    # Get basic info
    partition_info = get_partition_info(df)
    schema_info = [(field.name, str(field.dataType)) for field in df.schema.fields]
    
    # Sample for detailed analysis
    sample_df = df.sample(False, sample_size)
    
    profile = {
        'schema': schema_info,
        'column_count': len(df.columns),
        'partition_info': partition_info,
        'sample_size_used': sample_size
    }
    
    print("\n" + "="*60)
    print("DATAFRAME PROFILE")
    print("="*60)
    print(f"Columns: {profile['column_count']}")
    print(f"Total Rows: {partition_info['total_rows']}")
    print(f"Partitions: {partition_info['partition_count']}")
    print(f"Avg Rows/Partition: {partition_info['avg_rows_per_partition']:.0f}")
    print(f"Partition Skew Ratio: {partition_info['skew_ratio']:.2f}")
    print("="*60 + "\n")
    
    return profile

# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    # Apply performance config
    apply_performance_config(spark)
    
    # Create sample DataFrame
    data = [(i, f"name_{i}", i * 100) for i in range(10000)]
    df = spark.createDataFrame(data, ["id", "name", "value"])
    
    # Profile DataFrame
    profile = profile_dataframe(df)
    
    # Optimize partitions
    optimized_df = optimize_partitions(df, target_partitions=4)
    
    # Analyze query plan
    result_df = optimized_df.filter(col("value") > 5000)
    analyze_query_plan(result_df)
