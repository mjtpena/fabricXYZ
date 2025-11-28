"""
Semantic Link Integration for Microsoft Fabric

Description:
    Utilities for working with Semantic Link to connect Power BI datasets
    with Spark in Microsoft Fabric

Prerequisites:
    - sempy library installed (pip install semantic-link)
    - Power BI workspace access
    - Appropriate permissions on datasets

Features:
    - Read data from Power BI datasets
    - Execute DAX queries
    - List datasets and tables

Example Usage:
    from semantic_link import read_power_bi_table
    df = read_power_bi_table("Sales", "FactSales", "my-workspace")
"""

from typing import List, Optional

def read_power_bi_table(
    dataset_name: str,
    table_name: str,
    workspace: Optional[str] = None
):
    """
    Read a table from a Power BI dataset into a Spark DataFrame
    
    Args:
        dataset_name: Name of the Power BI dataset
        table_name: Name of the table to read
        workspace: Workspace name (optional, uses default if not specified)
        
    Returns:
        Spark DataFrame with table data
    """
    try:
        import sempy.fabric as fabric
        
        df = fabric.read_table(
            dataset=dataset_name,
            table=table_name,
            workspace=workspace
        )
        
        print(f"Successfully read table: {table_name} from {dataset_name}")
        print(f"Row count: {len(df)}")
        
        return df
        
    except ImportError:
        print("Error: semantic-link library not installed")
        print("Install with: pip install semantic-link")
        raise
    except Exception as e:
        print(f"Error reading Power BI table: {str(e)}")
        raise

def execute_dax_query(
    dataset_name: str,
    dax_query: str,
    workspace: Optional[str] = None
):
    """
    Execute a DAX query against a Power BI dataset
    
    Args:
        dataset_name: Name of the Power BI dataset
        dax_query: DAX query string
        workspace: Workspace name (optional)
        
    Returns:
        DataFrame with query results
    """
    try:
        import sempy.fabric as fabric
        
        df = fabric.evaluate_dax(
            dataset=dataset_name,
            dax_string=dax_query,
            workspace=workspace
        )
        
        print(f"DAX query executed successfully")
        print(f"Result rows: {len(df)}")
        
        return df
        
    except ImportError:
        print("Error: semantic-link library not installed")
        raise
    except Exception as e:
        print(f"Error executing DAX query: {str(e)}")
        raise

def list_datasets(workspace: Optional[str] = None) -> List[str]:
    """
    List available Power BI datasets
    
    Args:
        workspace: Workspace name (optional)
        
    Returns:
        List of dataset names
    """
    try:
        import sempy.fabric as fabric
        
        datasets = fabric.list_datasets(workspace=workspace)
        
        print(f"Found {len(datasets)} datasets")
        return datasets['Dataset Name'].tolist()
        
    except ImportError:
        print("Error: semantic-link library not installed")
        raise
    except Exception as e:
        print(f"Error listing datasets: {str(e)}")
        raise

def list_tables(
    dataset_name: str,
    workspace: Optional[str] = None
) -> List[str]:
    """
    List tables in a Power BI dataset
    
    Args:
        dataset_name: Name of the dataset
        workspace: Workspace name (optional)
        
    Returns:
        List of table names
    """
    try:
        import sempy.fabric as fabric
        
        tables = fabric.list_tables(
            dataset=dataset_name,
            workspace=workspace
        )
        
        print(f"Found {len(tables)} tables in {dataset_name}")
        return tables['Name'].tolist()
        
    except ImportError:
        print("Error: semantic-link library not installed")
        raise
    except Exception as e:
        print(f"Error listing tables: {str(e)}")
        raise

# Example usage
if __name__ == "__main__":
    # Example 1: Read a table from Power BI
    # df = read_power_bi_table(
    #     dataset_name="Sales Dataset",
    #     table_name="FactSales"
    # )
    # print(df.head())
    
    # Example 2: Execute DAX query
    # dax = """
    # EVALUATE
    # SUMMARIZE(
    #     FactSales,
    #     DimProduct[Category],
    #     "Total Sales", SUM(FactSales[Amount])
    # )
    # """
    # results = execute_dax_query("Sales Dataset", dax)
    # print(results)
    
    # Example 3: List available datasets
    # datasets = list_datasets()
    # for ds in datasets:
    #     print(f"Dataset: {ds}")
    
    print("Configure Power BI access and uncomment the example code to use")
