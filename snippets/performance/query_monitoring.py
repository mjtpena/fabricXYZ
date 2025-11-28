"""
Query Performance Monitoring

Description:
    Utilities for monitoring and analyzing query performance in Microsoft Fabric

Prerequisites:
    - Active SparkSession
    - Understanding of Spark metrics

Features:
    - Query execution timing
    - Spark UI metrics collection
    - Performance comparison utilities
    - Bottleneck identification

Example Usage:
    with QueryTimer("My Query") as timer:
        df.count()
    print(timer.summary())
"""

from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from typing import Dict, Any, List, Optional
import time

class QueryTimer:
    """Context manager for timing query execution"""
    
    def __init__(self, query_name: str = "Query"):
        """
        Initialize timer
        
        Args:
            query_name: Descriptive name for the query
        """
        self.query_name = query_name
        self.start_time = None
        self.end_time = None
        self.duration = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        print(f"[{self.start_time.strftime('%H:%M:%S')}] Starting: {self.query_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = datetime.now()
        self.duration = (self.end_time - self.start_time).total_seconds()
        
        status = "completed" if exc_type is None else "failed"
        print(f"[{self.end_time.strftime('%H:%M:%S')}] {self.query_name} {status} in {self.duration:.2f}s")
        
        return False
    
    def summary(self) -> Dict[str, Any]:
        """Get timing summary"""
        return {
            'query_name': self.query_name,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.duration
        }

class PerformanceMonitor:
    """Monitor and compare query performance"""
    
    def __init__(self):
        self.timings: List[Dict[str, Any]] = []
    
    def time_query(self, query_name: str, func, *args, **kwargs) -> Any:
        """
        Time a query function
        
        Args:
            query_name: Name for the query
            func: Function to execute
            *args, **kwargs: Arguments for the function
            
        Returns:
            Function result
        """
        start = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start
        
        self.timings.append({
            'query_name': query_name,
            'duration': duration,
            'timestamp': datetime.now().isoformat()
        })
        
        print(f"Query '{query_name}' executed in {duration:.2f}s")
        return result
    
    def get_report(self) -> str:
        """Generate performance report"""
        if not self.timings:
            return "No queries recorded"
        
        report = ["\n" + "="*60, "PERFORMANCE REPORT", "="*60]
        
        total_time = sum(t['duration'] for t in self.timings)
        
        for timing in self.timings:
            pct = (timing['duration'] / total_time) * 100 if total_time > 0 else 0
            report.append(
                f"{timing['query_name']}: {timing['duration']:.2f}s ({pct:.1f}%)"
            )
        
        report.extend([
            "-"*60,
            f"Total time: {total_time:.2f}s",
            f"Average time: {total_time/len(self.timings):.2f}s",
            "="*60 + "\n"
        ])
        
        return "\n".join(report)
    
    def compare_queries(self, baseline: str, comparison: str) -> Dict[str, Any]:
        """
        Compare two query timings
        
        Args:
            baseline: Name of baseline query
            comparison: Name of comparison query
            
        Returns:
            Comparison results
        """
        baseline_time = next(
            (t['duration'] for t in self.timings if t['query_name'] == baseline),
            None
        )
        comparison_time = next(
            (t['duration'] for t in self.timings if t['query_name'] == comparison),
            None
        )
        
        if baseline_time is None or comparison_time is None:
            return {'error': 'Query not found'}
        
        diff = comparison_time - baseline_time
        pct_change = ((comparison_time - baseline_time) / baseline_time) * 100
        
        return {
            'baseline': baseline,
            'baseline_time': baseline_time,
            'comparison': comparison,
            'comparison_time': comparison_time,
            'difference': diff,
            'percent_change': pct_change,
            'faster': comparison_time < baseline_time
        }

def measure_io_performance(
    df: DataFrame,
    output_path: str,
    formats: List[str] = None
) -> Dict[str, float]:
    """
    Measure I/O performance for different file formats
    
    Args:
        df: DataFrame to write
        output_path: Base path for output
        formats: List of formats to test
        
    Returns:
        Dictionary of format: duration pairs
    """
    if formats is None:
        formats = ['parquet', 'delta']
    
    results = {}
    
    for fmt in formats:
        path = f"{output_path}_{fmt}"
        
        start = time.time()
        df.write.format(fmt).mode("overwrite").save(path)
        duration = time.time() - start
        
        results[fmt] = duration
        print(f"{fmt.upper()}: {duration:.2f}s")
    
    return results

def get_spark_metrics(spark: SparkSession) -> Dict[str, Any]:
    """
    Get current Spark session metrics
    
    Args:
        spark: Active SparkSession
        
    Returns:
        Dictionary of metrics
    """
    sc = spark.sparkContext
    
    metrics = {
        'app_name': sc.appName,
        'spark_version': sc.version,
        'default_parallelism': sc.defaultParallelism,
        'executor_memory': spark.conf.get("spark.executor.memory", "Not set"),
        'driver_memory': spark.conf.get("spark.driver.memory", "Not set"),
        'shuffle_partitions': spark.conf.get("spark.sql.shuffle.partitions", "200"),
        'adaptive_enabled': spark.conf.get("spark.sql.adaptive.enabled", "false")
    }
    
    print("\n" + "="*60)
    print("SPARK SESSION METRICS")
    print("="*60)
    for key, value in metrics.items():
        print(f"{key}: {value}")
    print("="*60 + "\n")
    
    return metrics

# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    # Example 1: Time a single query
    data = [(i, f"name_{i}") for i in range(100000)]
    df = spark.createDataFrame(data, ["id", "name"])
    
    with QueryTimer("Count Query") as timer:
        count = df.count()
    
    print(f"Count: {count}")
    print(timer.summary())
    
    # Example 2: Monitor multiple queries
    monitor = PerformanceMonitor()
    
    monitor.time_query("Simple Count", lambda: df.count())
    monitor.time_query("Filter Count", lambda: df.filter(df.id > 50000).count())
    monitor.time_query("GroupBy", lambda: df.groupBy("id").count().count())
    
    print(monitor.get_report())
    
    # Example 3: Get Spark metrics
    metrics = get_spark_metrics(spark)
