import pandas as pd
import requests
from io import BytesIO
import json
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pickle
import os
from fuzzywuzzy import fuzz


def load_config():
    """Load configuration from config.json"""
    with open('config.json', 'r') as f:
        return json.load(f)


def get_sheet_id_and_gid_from_url(url):
    """Extract sheet ID and gid from Google Sheets URL"""
    # Extract the main spreadsheet ID
    sheet_id = url.split('/d/')[1].split('/')[0]
    # Extract the gid
    gid = url.split('gid=')[1].split('#')[0]
    return sheet_id, gid


def load_google_sheet(url):
    """Load data from Google Sheet URL"""
    sheet_id, gid = get_sheet_id_and_gid_from_url(url)
    # Convert the URL to a direct download URL, specifying the gid
    download_url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}'
    
    try:
        response = requests.get(download_url)
        response.raise_for_status()
        return pd.read_csv(BytesIO(response.content))
    except Exception as e:
        print(f"Error loading sheet from {url}: {str(e)}")
        return None


def get_credentials():
    """Get OAuth 2.0 credentials for Google Sheets API"""
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
                'credentials.json', 
                ['https://www.googleapis.com/auth/spreadsheets']
            )
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    return creds


def clear_and_write_to_sheet(df, url):
    """Clear the sheet and write new data"""
    try:
        # Get credentials
        creds = get_credentials()
        
        # Build the Sheets API service
        service = build('sheets', 'v4', credentials=creds)
        
        # Get sheet ID
        sheet_id, _ = get_sheet_id_and_gid_from_url(url)
        
        # Use the specific sheet name
        sheet_name = "product_to_map"
        
        # Clear the sheet
        range_name = f"'{sheet_name}'"
        service.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        
        # Prepare the data for writing
        values = [df.columns.tolist()] + df.values.tolist()
        body = {
            'values': values
        }
        
        # Write the new data
        result = service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        
        print(f"Successfully wrote {result.get('updatedCells')} cells to the sheet")
        
    except HttpError as error:
        print(f"An error occurred: {error}")


def map_product_to_nutrition(product_to_map_df, product_nutrition_map_df, aaron_nutrition_raw_df):
    """Map product_to_map to product_nutrition_map and add new columns"""
    # Create a dictionary for quick lookup from product_nutrition_map
    nutrition_map = {}
    for _, row in product_nutrition_map_df.iterrows():
        key = (row['Product Name'], row['Temperature L1'])
        nutrition_map[key] = row['nutrition_sheet_name']
    
    # Add new columns to product_to_map
    def map_row(row):
        # Generate identifier
        identifier = f"{row['Ounce']} {row['Temperature L1']} {row['product_name_in_nutrition']}"
        
        # Map product name in nutrition
        nutrition_key = (row['Product Name'], row['Temperature L1'])
        row['product_name_in_nutrition'] = nutrition_map.get(nutrition_key, 'unmapped')
        
        # Fuzzy match with aaron_nutrition_raw
        best_match = None
        best_score = 0
        for _, aaron_row in aaron_nutrition_raw_df.iterrows():
            score = fuzz.ratio(identifier, aaron_row['Identifier'])
            if score > best_score:
                best_score = score
                best_match = aaron_row['Identifier']
        
        # Add aaron sheet name
        row['aaron_sheet_name'] = best_match if best_score >= 75 else 'unmapped'
        return row
    
    product_to_map_df = product_to_map_df.apply(map_row, axis=1)
    
    return product_to_map_df


def main():
    # Load configuration
    config = load_config()
    
    # Load product_to_map sheet
    product_to_map_df = load_google_sheet(config['product_to_map'])
    if product_to_map_df is None:
        print("Failed to load product_to_map sheet")
        return
    
    # Load product_nutrition_map sheet
    product_nutrition_map_df = load_google_sheet(config['product_nutrition_map'])
    if product_nutrition_map_df is None:
        print("Failed to load product_nutrition_map sheet")
        return
    
    # Load aaron_nutrition_raw sheet
    aaron_nutrition_raw_df = load_google_sheet(config['aaron_nutrition_raw'])
    if aaron_nutrition_raw_df is None:
        print("Failed to load aaron_nutrition_raw sheet")
        return
    
    # Map products to nutrition
    mapped_df = map_product_to_nutrition(product_to_map_df, product_nutrition_map_df, aaron_nutrition_raw_df)
    
    # Write to Google Sheet
    print("\nWriting to product_to_map sheet...")
    clear_and_write_to_sheet(mapped_df, config['product_to_map'])
    
    # Save the result locally
    mapped_df.to_csv('processed_data/product_to_map_with_nutrition.csv', index=False)
    print("Mapping completed and saved to processed_data/product_to_map_with_nutrition.csv")


if __name__ == "__main__":
    main()
