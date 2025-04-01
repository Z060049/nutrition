import json
import pandas as pd
import os
import time
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import pickle
import numpy as np

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def load_config():
    """Load configuration from config.json"""
    time.sleep(0.1)  # Small delay to ensure file system sync
    with open('config.json', 'r') as f:
        config = json.load(f)
        print("Loaded config:", config)  # Debug print
        if 'output' not in config:
            raise KeyError("output URL not found in config.json")
        return config

def get_sheet_id_and_gid_from_url(url):
    """Extract sheet ID and gid from Google Sheets URL"""
    # Extract the main spreadsheet ID
    sheet_id = url.split('/d/')[1].split('/')[0]
    
    # Extract the gid if present, otherwise use default gid of 0
    try:
        gid = url.split('gid=')[1].split('#')[0]
    except IndexError:
        gid = '0'  # Default to first sheet if no gid specified
    
    return sheet_id, gid

def get_google_sheets_service():
    """Get or refresh Google Sheets API credentials"""
    creds = None
    # The file token.pickle stores the user's access and refresh tokens
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    # If there are no (valid) credentials available, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            # Use a simpler authentication flow
            creds = flow.run_local_server(
                port=8080,
                success_message='The authentication flow has completed. You may close this window.',
                authorization_prompt_message='Please visit this URL to authorize this application:',
                open_browser=True
            )
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    return build('sheets', 'v4', credentials=creds)

def clean_data_for_sheets(df):
    """Clean DataFrame values for Google Sheets API"""
    # Replace NaN with empty string
    df = df.replace({np.nan: ''})
    
    # Convert all values to strings
    for col in df.columns:
        df[col] = df[col].astype(str)
    
    return df

def write_to_google_sheet(url, df):
    """Write DataFrame to Google Sheet using Google Sheets API"""
    sheet_id, gid = get_sheet_id_and_gid_from_url(url)
    
    # Get the service
    service = get_google_sheets_service()
    
    # Clean the data
    df = clean_data_for_sheets(df)
    
    # Convert DataFrame to values
    values = [df.columns.tolist()] + df.values.tolist()
    
    # Calculate the range based on data size
    num_rows = len(values)
    num_cols = len(values[0])
    end_col = chr(ord('A') + num_cols - 1)  # Convert column number to letter
    range_name = f'mapped_final!A1:{end_col}{num_rows}'
    
    # Clear the sheet first
    try:
        service.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
    except Exception as e:
        print(f"Warning: Could not clear sheet: {str(e)}")
    
    # Write the new data
    body = {
        'values': values
    }
    
    try:
        result = service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        print("Successfully wrote data to Google Sheet")
        return True
    except Exception as e:
        print(f"Error writing to sheet: {str(e)}")
        return False

def main():
    """Main function to write combinations data to Google Sheet"""
    # Load configuration
    config = load_config()
    
    # Read the combinations data
    combinations_df = pd.read_csv('all_combinations.csv')
    
    print("\nWriting combinations data to Google Sheet...")
    print(f"Data shape: {combinations_df.shape}")
    print("\nFirst few rows of data to be written:")
    print(combinations_df.head())
    
    # Write to Google Sheet
    success = write_to_google_sheet(config['output'], combinations_df)
    
    if success:
        print("\nSuccessfully updated Google Sheet with combinations data")
    else:
        print("\nFailed to update Google Sheet")

if __name__ == "__main__":
    main() 