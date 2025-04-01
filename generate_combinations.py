import pandas as pd
import itertools
import os
import json

def load_config():
    """Load configuration from config.json"""
    with open('config.json', 'r') as f:
        return json.load(f)

def clear_existing_file():
    """Clear existing combinations file if it exists"""
    if os.path.exists('all_combinations.csv'):
        os.remove('all_combinations.csv')
        print("Cleared existing combinations file")

def load_input_files():
    """Load all input CSV files"""
    products_df = pd.read_csv('options/product.csv')
    sizes_df = pd.read_csv('options/size.csv')
    temperatures_df = pd.read_csv('options/temperature.csv')
    sugars_df = pd.read_csv('options/sugar.csv')
    milks_df = pd.read_csv('options/milk.csv')
    
    # Load nutrition mapping from Google Sheet URL
    config = load_config()
    url = config['mapped_nutrition_url']
    # Convert Google Sheet URL to export URL
    export_url = url.replace('/edit?gid=', '/export?format=csv&gid=')
    nutrition_mapped_df = pd.read_csv(export_url)
    
    # Load nutrition raw data
    nutrition_raw_df = pd.read_csv('processed_data/nutrition_raw.csv')
    nutrition_raw_df.columns = [col.strip() for col in nutrition_raw_df.columns]  # Strip whitespace from column names
    
    return products_df, sizes_df, temperatures_df, sugars_df, milks_df, nutrition_mapped_df, nutrition_raw_df

def generate_combinations(products_df, sizes_df, temperatures_df, sugars_df, milks_df):
    """Generate all possible combinations"""
    # Get lists of unique values
    products = products_df['Product Name'].unique()
    # Store both size name and ounce, but only display size name
    sizes = sizes_df.apply(lambda x: (x['Size Name'], f"{x['Size Name']} ({x['Ounce']})"), axis=1).unique()
    temperatures = temperatures_df.apply(lambda x: f"{x['Temperature L1']} ({x['Temperature L2']})", axis=1).unique()
    sugars = sugars_df['Sugar Level'].unique()
    milks = milks_df['Alternative Milk'].unique()
    
    # Generate all possible combinations
    combinations = list(itertools.product(products, sizes, temperatures, sugars, milks))
    
    # Create DataFrame with separate columns for display size and full size
    combinations_df = pd.DataFrame([
        (p, s[0], s[1], t, sg, m) for p, s, t, sg, m in combinations
    ], columns=['Product', 'Size', 'Size_Full', 'Temperature', 'Sugar', 'Milk'])
    
    # Split Size into Size Name and Ounce using the full size string
    combinations_df[['Size Name', 'Ounce']] = combinations_df['Size_Full'].str.extract(r'(.+) \((.+)\)')
    
    # Split Temperature into L1 and L2
    combinations_df[['Temperature L1', 'Temperature L2']] = combinations_df['Temperature'].str.extract(r'(.+) \((.+)\)')
    
    # Add category from products_df
    combinations_df = combinations_df.merge(
        products_df[['Product Name', 'Category']],
        left_on='Product',
        right_on='Product Name'
    ).drop(['Product Name', 'Size_Full'], axis=1)
    
    return combinations_df

def find_matching_identifier(row, nutrition_mapped_df):
    """Find matching identifier in nutrition_mapped based on product, ounce, size, category and temperature_l1"""
    # Extract base product name (remove size and modifiers)
    product_name = row['Product'].split('(')[0].strip()
    
    # Create a mask for matching conditions
    mask = (
        (nutrition_mapped_df['product_name'].str.contains(product_name, case=False, na=False)) &
        (nutrition_mapped_df['ounce'] == row['Ounce']) &
        (nutrition_mapped_df['size'] == row['Size Name']) &
        (nutrition_mapped_df['temperature_l1'] == row['Temperature L1'])
    )
    
    # Get matching rows
    matches = nutrition_mapped_df[mask]
    
    if len(matches) > 0:
        return matches.iloc[0]['identifier']
    return None

def get_nutrition_info(identifier, nutrition_raw_df):
    """Get nutrition information from nutrition_raw.csv based on identifier"""
    if pd.isna(identifier):
        return pd.Series({
            'calories': None,
            'caffeine': None,
            'sodium': None,
            'protein': None
        })
    
    # Find the matching row in nutrition_raw_df
    mask = nutrition_raw_df['Beverage Type'] == identifier
    if not mask.any():
        return pd.Series({
            'calories': None,
            'caffeine': None,
            'sodium': None,
            'protein': None
        })
    
    row = nutrition_raw_df[mask].iloc[0]
    return pd.Series({
        'calories': row['Calories'],
        'caffeine': row['Caffeine (mg)'],
        'sodium': row['Sodium (mg)'],
        'protein': row['Protein (g)']
    })

def main():
    """Main function to generate combinations and match with nutrition data"""
    # Clear existing file
    clear_existing_file()
    
    # Load configuration and input files
    config = load_config()
    products_df, sizes_df, temperatures_df, sugars_df, milks_df, nutrition_mapped_df, nutrition_raw_df = load_input_files()
    
    # Generate combinations
    combinations_df = generate_combinations(products_df, sizes_df, temperatures_df, sugars_df, milks_df)
    
    # Find matching identifiers
    combinations_df['identifier'] = combinations_df.apply(
        lambda row: find_matching_identifier(row, nutrition_mapped_df), 
        axis=1
    )
    
    # Get nutrition information
    nutrition_info = combinations_df['identifier'].apply(
        lambda x: get_nutrition_info(x, nutrition_raw_df)
    )
    
    # Add nutrition columns
    combinations_df['calories'] = nutrition_info['calories']
    combinations_df['caffeine'] = nutrition_info['caffeine']
    combinations_df['sodium'] = nutrition_info['sodium']
    combinations_df['protein'] = nutrition_info['protein']
    
    # Select and reorder columns
    output_columns = [
        'identifier', 'Category', 'Temperature L1', 'Product', 'Size', 'Temperature L2',
        'Sugar', 'Milk', 'calories', 'caffeine', 'protein', 'sodium'
    ]
    
    # Save to local CSV first
    combinations_df[output_columns].to_csv('all_combinations.csv', index=False)
    
    # Print statistics
    total_rows = len(combinations_df)
    matched_rows = combinations_df['identifier'].notna().sum()
    print(f"\nGenerated {total_rows} combinations")
    print(f"Successfully matched: {matched_rows}")
    print(f"Match rate: {(matched_rows/total_rows)*100:.2f}%")
    print("\nFirst few combinations:")
    print(combinations_df[output_columns].head())
    
    print("\nNow writing to Google Sheet...")
    os.system('python write_mapping_to_gsheet.py')

if __name__ == "__main__":
    main() 