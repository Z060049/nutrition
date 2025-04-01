import json
import pandas as pd
import requests
from io import BytesIO
import os

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

def save_dataframe(df, filename, folder='options'):
    """Save DataFrame to CSV in the specified folder"""
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, f'{filename}.csv')
    df.to_csv(filepath, index=False)
    print(f"Saved {filename} to {filepath}")

def process_nutrition_data(nutrition_df):
    """Process nutrition data and save it separately"""
    if nutrition_df is not None:
        # Save nutrition data to a separate folder
        os.makedirs('nutrition_data', exist_ok=True)
        nutrition_df.to_csv('nutrition_data/nutrition.csv', index=False)
        print("Saved nutrition data to nutrition_data/nutrition.csv")
        print(f"Nutrition data shape: {nutrition_df.shape}")
        print("\nNutrition data columns:")
        print(nutrition_df.columns.tolist())
        print("\nFirst few rows of nutrition data:")
        print(nutrition_df.head())
        
        # Process nutrition data to extract specific columns
        selected_columns = ['Beverage Type', 'Calories', 'Caffeine (mg)', 'Sodium (mg)', 'Protein (g)']
        processed_df = nutrition_df[selected_columns]
        
        # Save processed nutrition data
        os.makedirs('processed_data', exist_ok=True)
        processed_df.to_csv('processed_data/nutrition_raw.csv', index=False)
        print("\nSaved processed nutrition data to processed_data/nutrition_raw.csv")
        print("Processed nutrition data shape:", processed_df.shape)
        print("\nFirst few rows of processed nutrition data:")
        print(processed_df.head())
        
        return nutrition_df, processed_df
    return None, None

def create_nutrition_mapping(product_df, temperature_df, size_df):
    """Create nutrition mapping with product, temperature, size, ounce and category"""
    # Create all possible combinations
    mapping_data = []
    
    for _, product_row in product_df.iterrows():
        for _, temp_row in temperature_df.iterrows():
            for _, size_row in size_df.iterrows():
                mapping_data.append({
                    'Product Name': product_row['Product Name'],
                    'Temperature L1': temp_row['Temperature L1'],
                    'Temperature L2': temp_row['Temperature L2'],
                    'Size': size_row['Size Name'],
                    'Ounce': size_row['Ounce'],
                    'Category': product_row['Category']
                })
    
    # Create DataFrame from the combinations
    mapping_df = pd.DataFrame(mapping_data)
    
    # Save to processed_data folder
    os.makedirs('processed_data', exist_ok=True)
    mapping_df.to_csv('processed_data/nutrition_to_map.csv', index=False)
    print("\nCreated nutrition mapping with shape:", mapping_df.shape)
    print("First few rows of nutrition mapping:")
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
    
    # Create nutrition mapping
    print("\nCreating nutrition mapping...")
    create_nutrition_mapping(dfs['producturl'], dfs['temperatureurl'], dfs['sizeurl'])
    
    return dfs

if __name__ == "__main__":
    dfs = main()