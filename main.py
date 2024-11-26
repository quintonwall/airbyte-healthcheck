import streamlit as st
import os
from dotenv import load_dotenv
import airbyte as ab
from airbyte import cloud
from airbyte_api import AirbyteAPI
from airbyte_api import api, models
import pandas as pd
import requests


import time


# Load environment variables
load_dotenv()

# Initialize Airbyte client
def init_client():
   host=os.getenv('AIRBYTE_HOST', 'https://api.airbyte.com')
   workspace_id=os.getenv('AIRBYTE_WORKSPACE_ID', '')
   api_key=os.getenv('AIRBYTE_REFRESH_TOKEN', '')
    
   # set up the pyairbyte workspace, although most of the time we will use the api client instead. 
   workspace = cloud.CloudWorkspace(workspace_id,api_key)
   return workspace

def init_api_client():
    try:
        client = AirbyteAPI(
            security=models.Security(
                bearer_auth=os.getenv('AIRBYTE_REFRESH_TOKEN', '')
            )
        )
        return client
    except Exception as e:
        st.error(f"API ClientError: {str(e)}")

def refresh_airbyte_token():
    try:
         # Check if we recently refreshed
        last_refresh = st.session_state.get('last_token_refresh', 0)
        if time.time() - last_refresh < 120:  # 120 seconds = 2 minutes
            return st.session_state.get('current_token')
        
        #if older, go get a new one
        st.info("Fetching an updated token.")
        refresh_token = os.getenv('AIRBYTE_REFRESH_TOKEN')
        if not refresh_token:
            raise ValueError("AIRBYTE_REFRESH_TOKEN not found in environment variables")

        base_url = os.getenv('AIRBYTE_HOST').rstrip('/')
        response = requests.post(
            f'{base_url}/v1/applications/token',
            data={
                'grant_type': 'refresh_token',
                'client_id': os.getenv('AIRBYTE_CLIENT_ID'),
                'client_secret': os.getenv('AIRBYTE_CLIENT_SECRET'),
                'refresh_token': refresh_token
            }
        )
        response.raise_for_status()
        
        new_token = response.json().get('access_token')
        if not new_token:
            raise ValueError("No access token in response")

        # Update session state with new clients
        st.session_state.workspace = cloud.CloudWorkspace(
            os.getenv('AIRBYTE_WORKSPACE_ID'),
            new_token
        )
        st.session_state.client = AirbyteAPI(
            security=models.Security(bearer_auth=new_token)
        )
        st.session_state.last_token_refresh = time.time()
        st.session_state.current_token = new_token
        
        return new_token

    except Exception as e:
        st.error(f"Failed to refresh token: {str(e)}")
        raise

# Page config
st.set_page_config(page_title="Airbyte Connection Checker", page_icon="🔌")

try:
    # Initialize API clients
    workspace = init_client()
    workspace.connect()
    client = init_api_client()
    refresh_airbyte_token()

    
  
    
    st.title("🔄 Airbyte Connection Validator")
   
     #  New way via pyairbyte way to get a worksace. 
    #connection = workspace.get_connection("ac5a2490-54fb-4632-85ca-5c67bfacc5fc")
    #st.write(connection.stream_names)

     # Then need to wait for a yairbyte patch for this. 
    #connections = workspace.list_connections()


    #old school way of retrieving via airbyte-api. fetch the workspace, then get a list of connections
    # Get workspace info
    #refresh_airbyte_token()
    workspace_response = st.session_state.client.workspaces.get_workspace(request=api.GetWorkspaceRequest(workspace_id=workspace.workspace_id)) 
    
    # Fetch connections for this workspace
    connections_request = api.ListConnectionsRequest(
        workspace_ids=[workspace.workspace_id]
    )
    connections_response = client.connections.list_connections(request=connections_request)
   

   # Create DataFrame with desired columns
    df = pd.DataFrame([{
        'Name': conn.name,
        'Connection ID': conn.connection_id,
        'Status': '🟢' if conn.status.lower() == 'active' else '🔴'
    } for conn in connections_response.connections_response.data])
    
    # Title and description
    st.header("Connection Status Table")
    st.caption("Overview of all connections and their current status. 🟢 Active, 🔴 Inactive")
    
    col1, col2, col3 = st.columns([3, 1, 2])
    col1.write("**Name**")
    #col2.write("Connection ID")
    col2.write("**Active**")
    col3.write("**Streams**")
    st.divider()

    # Add CSS for row height
    st.markdown("""
        <style>
        .row-content {
            height: 50px;
            display: flex;
            align-items: center;
            padding: 0 10px;
        }
        </style>
    """, unsafe_allow_html=True)

    # Display each row
    for index, row in df.iterrows():
        col1.markdown(f'<div class="row-content">{row["Name"]}</div>', unsafe_allow_html=True)
        col2.markdown(f'<div class="row-content">{row["Status"]}</div>', unsafe_allow_html=True)
       
        if col3.button("View Streams", key=f"streams_{row['Connection ID']}"):
            refresh_airbyte_token()
            connection = st.session_state.workspace.get_connection(row['Connection ID'])
            st.subheader(f"Streams for {row['Name']}")
            for stream in connection.stream_names:
                st.write(f"• {stream}")
            st.divider()
    # Get all connections
    
except Exception as e:
    st.error(f"Error: {str(e)}")
