import json
import pandas as pd
import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import pickle

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def load_config():
    with open('config.json', 'r') as f:
        return json.load(f)

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
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    return build('sheets', 'v4', credentials=creds)

def read_from_google_sheet(url):
    """Read data from a Google Sheet"""
    sheet_id, gid = get_sheet_id_and_gid_from_url(url)
    
    # Get the service
    service = get_google_sheets_service()
    
    try:
        # First, try to get the spreadsheet to check if we have access
        spreadsheet = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        print(f"Successfully accessed spreadsheet: {spreadsheet['properties']['title']}")
        
        # Get all sheets in the spreadsheet
        sheets = spreadsheet.get('sheets', [])
        target_sheet = None
        
        # Find the sheet with matching gid
        for sheet in sheets:
            if sheet.get('properties', {}).get('sheetId') == int(gid):
                target_sheet = sheet.get('properties', {}).get('title')
                break
        
        if not target_sheet:
            print(f"Could not find sheet with gid {gid}, using default sheet")
            target_sheet = spreadsheet['sheets'][0]['properties']['title']
        
        print(f"Reading from sheet: {target_sheet}")
        
        # Read the data
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"'{target_sheet}'"
        ).execute()
        
        values = result.get('values', [])
        if not values:
            print('No data found.')
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(values[1:], columns=values[0])
        return df
    except Exception as e:
        print(f"Error accessing or reading sheet: {str(e)}")
        return pd.DataFrame()

def update_google_sheet(url, df):
    """Update a Google Sheet with the given DataFrame."""
    try:
        print(f"\nAttempting to update sheet with URL: {url}")
        sheet_id, gid = get_sheet_id_and_gid_from_url(url)
        print(f"Extracted sheet_id: {sheet_id}")
        print(f"Extracted gid: {gid}")
        
        service = get_google_sheets_service()
        
        # Get the spreadsheet to verify access and sheet name
        spreadsheet = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        print(f"Accessing spreadsheet: {spreadsheet['properties']['title']}")
        
        # Find the sheet with matching gid
        sheets = spreadsheet.get('sheets', [])
        target_sheet = None
        for sheet in sheets:
            if sheet.get('properties', {}).get('sheetId') == int(gid):
                target_sheet = sheet.get('properties', {}).get('title')
                break
        
        if not target_sheet:
            print(f"Could not find sheet with gid {gid}, using default sheet")
            target_sheet = spreadsheet['sheets'][0]['properties']['title']
        
        print(f"Writing to sheet: {target_sheet}")
        
        # First, clear the existing content
        clear_request = service.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range=f"'{target_sheet}'!A:Z"
        )
        clear_request.execute()
        print("Cleared existing content")
        
        # Convert DataFrame to list of lists, replacing NaN with empty strings
        values = df.fillna('').values.tolist()
        
        # Add headers as the first row
        headers = df.columns.tolist()
        values.insert(0, headers)
        
        # Update the sheet
        range_name = f"'{target_sheet}'!A1"
        body = {
            'values': values
        }
        result = service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        
        print(f"Successfully updated {result.get('updatedCells')} cells")
        return True
    except Exception as e:
        print(f"Error accessing or updating sheet: {e}")
        return False

def main():
    # Load configuration
    config = load_config()
    
    print("\nReading from product_to_map sheet...")
    product_df = read_from_google_sheet(config['product_to_map'])
    
    if product_df.empty:
        print("Failed to read product data")
        return
    
    print("\nReading nutrition data...")
    nutrition_df = read_from_google_sheet(config['aaron_nutrition_raw'])
    
    if nutrition_df.empty:
        print("Failed to read nutrition data")
        return
    
    # Select the first 6 columns from product data
    product_columns = product_df.columns[:6].tolist()
    product_data = product_df[product_columns].copy()
    
    # Create nutrition mapping dictionary
    nutrition_map = {}
    for _, row in nutrition_df.iterrows():
        key = row['Beverage'].strip() if 'Beverage' in row else None
        if key:
            nutrition_map[key] = {
                'calories': row.get('Calories', ''),
                'caffeine': row.get('Caffeine (mg)', ''),
                'protein': row.get('Protein (g)', ''),
                'sodium': row.get('Sodium (mg)', '')
            }
    
    # Add nutrition columns
    nutrition_columns = ['calories', 'caffeine', 'protein', 'sodium']
    for col in nutrition_columns:
        product_data[col] = ''
    
    # Fill in nutrition data
    beverage_col = product_columns[0]  # Assuming first column is beverage name
    for idx, row in product_data.iterrows():
        beverage = str(row[beverage_col]).strip()
        if beverage in nutrition_map:
            for col in nutrition_columns:
                product_data.at[idx, col] = nutrition_map[beverage][col]
    
    print("\nUpdating output sheet...")
    success = update_google_sheet(config['output'], product_data)
    
    if success:
        print("\nSuccessfully updated the Google Sheet with combined data")
    else:
        print("\nFailed to update the Google Sheet")

if __name__ == "__main__":
    main()
