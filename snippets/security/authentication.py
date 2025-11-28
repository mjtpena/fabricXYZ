"""
Azure AD Authentication Utilities

Description:
    Authentication utilities for Microsoft Fabric using Azure Active Directory

Prerequisites:
    - Azure AD application registration (for service principal auth)
    - Azure Identity library installed
    - Appropriate API permissions configured

Features:
    - Interactive authentication
    - Service principal authentication
    - Token retrieval for various resources

Example Usage:
    token = get_fabric_token_interactive()
    # or
    token = get_fabric_token_service_principal(
        tenant_id="your-tenant-id",
        client_id="your-client-id",
        client_secret="your-client-secret"
    )
"""

from typing import Optional

# Fabric API resource
FABRIC_RESOURCE = "https://api.fabric.microsoft.com"
# Power BI resource
POWERBI_RESOURCE = "https://analysis.windows.net/powerbi/api"
# Azure Storage resource
STORAGE_RESOURCE = "https://storage.azure.com"

def get_fabric_token_interactive() -> str:
    """
    Get Fabric API token using interactive authentication
    
    Returns:
        Access token string
    """
    try:
        from azure.identity import InteractiveBrowserCredential
        
        credential = InteractiveBrowserCredential()
        token = credential.get_token(f"{FABRIC_RESOURCE}/.default")
        
        print("Successfully authenticated interactively")
        return token.token
        
    except ImportError:
        print("Error: azure-identity library not installed")
        print("Install with: pip install azure-identity")
        raise
    except Exception as e:
        print(f"Authentication failed: {str(e)}")
        raise

def get_fabric_token_service_principal(
    tenant_id: str,
    client_id: str,
    client_secret: str
) -> str:
    """
    Get Fabric API token using service principal
    
    Args:
        tenant_id: Azure AD tenant ID
        client_id: Application (client) ID
        client_secret: Client secret
        
    Returns:
        Access token string
    """
    try:
        from azure.identity import ClientSecretCredential
        
        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )
        token = credential.get_token(f"{FABRIC_RESOURCE}/.default")
        
        print("Successfully authenticated with service principal")
        return token.token
        
    except ImportError:
        print("Error: azure-identity library not installed")
        raise
    except Exception as e:
        print(f"Service principal authentication failed: {str(e)}")
        raise

def get_fabric_token_from_notebook() -> str:
    """
    Get Fabric API token within a Fabric notebook
    
    This method uses notebookutils which is available in Fabric notebooks.
    
    Returns:
        Access token string
    """
    try:
        from notebookutils import mssparkutils
        
        token = mssparkutils.credentials.getToken(FABRIC_RESOURCE)
        print("Successfully retrieved token from notebook context")
        return token
        
    except ImportError:
        print("Error: notebookutils not available (only works in Fabric notebooks)")
        raise
    except Exception as e:
        print(f"Failed to get token from notebook: {str(e)}")
        raise

def get_storage_token_from_notebook() -> str:
    """
    Get Azure Storage token within a Fabric notebook
    
    Returns:
        Access token string for Azure Storage
    """
    try:
        from notebookutils import mssparkutils
        
        token = mssparkutils.credentials.getToken(STORAGE_RESOURCE)
        print("Successfully retrieved storage token from notebook context")
        return token
        
    except ImportError:
        print("Error: notebookutils not available")
        raise
    except Exception as e:
        print(f"Failed to get storage token: {str(e)}")
        raise

def validate_token(token: str) -> bool:
    """
    Validate if a token is properly formatted (basic check)
    
    Args:
        token: JWT token string
        
    Returns:
        True if token appears valid
    """
    if not token:
        return False
    
    # Basic JWT structure check (header.payload.signature)
    parts = token.split('.')
    if len(parts) != 3:
        return False
    
    return True

# Example usage
if __name__ == "__main__":
    # Example 1: Interactive authentication
    # token = get_fabric_token_interactive()
    # print(f"Token valid: {validate_token(token)}")
    
    # Example 2: Service principal authentication
    # token = get_fabric_token_service_principal(
    #     tenant_id="your-tenant-id",
    #     client_id="your-client-id",
    #     client_secret="your-client-secret"
    # )
    
    # Example 3: From Fabric notebook
    # token = get_fabric_token_from_notebook()
    
    print("Configure authentication method and uncomment the example code to use")
