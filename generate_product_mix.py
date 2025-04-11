import json
import pandas as pd
import requests
from io import BytesIO
import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pickle

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

def save_dataframe(df, filename, folder='options'):
    """Save DataFrame to CSV in the specified folder"""
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, f'{filename}.csv')
    df.to_csv(filepath, index=False)
    print(f"Saved {filename} to {filepath}")

def process_nutrition_data(nutrition_df):
    """Process nutrition data"""
    # Select and rename columns
    selected_columns = ['Identifier', 'Calories', 'Caffeine (mg)', 'Sodium (mg)', 'Protein (g)']
    processed_df = nutrition_df[selected_columns]
    
    # Rename Identifier to Beverage Type for consistency
    processed_df = processed_df.rename(columns={'Identifier': 'Beverage Type'})
    
    # Save the original nutrition data
    os.makedirs('nutrition_data', exist_ok=True)
    nutrition_df.to_csv('nutrition_data/nutrition.csv', index=False)
    print("Saved nutrition data to nutrition_data/nutrition.csv")
    
    return nutrition_df, processed_df

def create_product_mapping(product_df, temperature_df, size_df):
    """Create product mapping with product, temperature, size, ounce and category"""
    # Create all possible combinations
    mapping_data = []
    
    for _, product_row in product_df.iterrows():
        for _, temp_row in temperature_df.iterrows():
            for _, size_row in size_df.iterrows():
                mapping_data.append({
                    'Product Name': product_row['Product name'],
                    'Temperature L1': temp_row['Temperature L1'],
                    'Temperature L2': temp_row['Temperature L2'],
                    'Size': size_row['Size Name'],
                    'Ounce': size_row['Ounce'],
                    'Category': 'Tea Latte' if 'Latte' in product_row['Product name'] else 'Tea'
                })
    
    # Create DataFrame from the combinations
    mapping_df = pd.DataFrame(mapping_data)
    
    # Save to processed_data folder
    os.makedirs('processed_data', exist_ok=True)
    mapping_df.to_csv('processed_data/product_to_map.csv', index=False)
    print("\nCreated product mapping with shape:", mapping_df.shape)
    print("First few rows of product mapping:")
    print(mapping_df.head())
    return mapping_df

def main():
    # Load configuration
    config = load_config()
    
    # Dictionary to store all dataframes
    dfs = {}
    
    # First, process nutrition data separately
    if 'nutritionurl' in config:
        print("\nProcessing nutrition data...")
        nutrition_df, processed_nutrition_df = process_nutrition_data(load_google_sheet(config['nutritionurl']))
        dfs['nutrition'] = nutrition_df
        dfs['processed_nutrition'] = processed_nutrition_df
    
    # Then process other sheets
    for sheet_name, url in config.items():
        if sheet_name != 'nutritionurl':  # Skip nutrition as it's already processed
            print(f"\nLoading {sheet_name}...")
            df = load_google_sheet(url)
            if df is not None:
                clean_name = sheet_name.replace('url', '')
                dfs[sheet_name] = df
                save_dataframe(df, clean_name)
                print(f"Successfully loaded {sheet_name} with shape {df.shape}")
                if sheet_name == 'temperatureurl':
                    print("\nTemperature columns:")
                    print(df.columns.tolist())
    
    # Create product mapping
    print("\nCreating product mapping...")
    mapping_df = create_product_mapping(dfs['producturl'], dfs['temperatureurl'], dfs['sizeurl'])
    
    # Write to Google Sheet
    print("\nWriting to product_to_map sheet...")
    clear_and_write_to_sheet(mapping_df, config['product_to_map'])
    
    return dfs

if __name__ == "__main__":
    dfs = main()