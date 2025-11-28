"""
Microsoft Fabric REST API Client

Description:
    Utility functions for interacting with Microsoft Fabric REST APIs

Prerequisites:
    - Azure AD authentication configured
    - Appropriate API permissions granted
    - requests library installed

Features:
    - Workspace management
    - Lakehouse operations
    - Item listing and metadata retrieval

Example Usage:
    client = FabricAPIClient(access_token)
    workspaces = client.list_workspaces()
"""

import requests
from typing import Dict, Any, List, Optional

class FabricAPIClient:
    """Client for Microsoft Fabric REST APIs"""
    
    BASE_URL = "https://api.fabric.microsoft.com/v1"
    
    def __init__(self, access_token: str):
        """
        Initialize API client
        
        Args:
            access_token: Azure AD access token
        """
        self.access_token = access_token
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make an API request
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            data: Request body data
            
        Returns:
            API response as dictionary
        """
        url = f"{self.BASE_URL}/{endpoint}"
        
        try:
            if method.upper() == "GET":
                response = requests.get(url, headers=self.headers)
            elif method.upper() == "POST":
                response = requests.post(url, headers=self.headers, json=data)
            elif method.upper() == "PATCH":
                response = requests.patch(url, headers=self.headers, json=data)
            elif method.upper() == "DELETE":
                response = requests.delete(url, headers=self.headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            
            if response.content:
                return response.json()
            return {}
            
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {str(e)}")
            raise
    
    def list_workspaces(self) -> List[Dict[str, Any]]:
        """
        List all accessible workspaces
        
        Returns:
            List of workspace objects
        """
        response = self._make_request("GET", "workspaces")
        return response.get("value", [])
    
    def get_workspace(self, workspace_id: str) -> Dict[str, Any]:
        """
        Get workspace details
        
        Args:
            workspace_id: Workspace GUID
            
        Returns:
            Workspace details
        """
        return self._make_request("GET", f"workspaces/{workspace_id}")
    
    def list_workspace_items(
        self,
        workspace_id: str,
        item_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List items in a workspace
        
        Args:
            workspace_id: Workspace GUID
            item_type: Filter by item type (optional)
            
        Returns:
            List of items
        """
        endpoint = f"workspaces/{workspace_id}/items"
        if item_type:
            endpoint += f"?type={item_type}"
        
        response = self._make_request("GET", endpoint)
        return response.get("value", [])
    
    def get_item(
        self,
        workspace_id: str,
        item_id: str
    ) -> Dict[str, Any]:
        """
        Get item details
        
        Args:
            workspace_id: Workspace GUID
            item_id: Item GUID
            
        Returns:
            Item details
        """
        return self._make_request("GET", f"workspaces/{workspace_id}/items/{item_id}")
    
    def list_lakehouses(self, workspace_id: str) -> List[Dict[str, Any]]:
        """
        List lakehouses in a workspace
        
        Args:
            workspace_id: Workspace GUID
            
        Returns:
            List of lakehouses
        """
        return self.list_workspace_items(workspace_id, "Lakehouse")

def get_access_token_interactive() -> str:
    """
    Get access token using interactive authentication
    
    Note: This is a placeholder. In production, use Azure Identity library
    or notebookutils for authentication in Fabric.
    
    Returns:
        Access token string
    """
    # In Fabric notebooks, you can use:
    # from notebookutils import mssparkutils
    # token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com")
    
    print("Note: Implement authentication using Azure Identity or notebookutils")
    raise NotImplementedError("Implement authentication for your environment")

# Example usage
if __name__ == "__main__":
    # Example: List workspaces
    # access_token = get_access_token_interactive()
    # client = FabricAPIClient(access_token)
    
    # workspaces = client.list_workspaces()
    # for ws in workspaces:
    #     print(f"Workspace: {ws['displayName']} ({ws['id']})")
    
    print("Configure authentication and uncomment the example code to use")
