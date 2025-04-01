import pandas as pd
import os
from fuzzywuzzy import fuzz

def extract_size_and_name(beverage_type):
    """Extract size and name from beverage type"""
    parts = beverage_type.split(' ', 2)  # Split into 3 parts: size, unit, name
    if len(parts) >= 3:
        size = f"{parts[0]} {parts[1]}"
        name = parts[2]
        return size, name
    return None, beverage_type

def clean_name(name):
    """Clean product name for better matching"""
    name = name.lower()
    name = name.replace('(', '').replace(')', '')
    name = name.replace('extracted', '').replace('brewed', '')
    name = name.replace('hot tea', '').replace('iced tea', '')
    name = name.replace('hot', '').replace('ice', '')
    name = name.replace('tea latte', 'tea').replace('latte', '')
    name = name.replace('pure tea', '').replace('tea', '')
    name = name.replace('bo ya', '')
    name = name.strip()
    return name

def map_size(size):
    """Map size to nutrition data format"""
    size_mapping = {
        'Small': '12 oz',
        'Regular': '16 oz',
        'Large': '22 oz'
    }
    return size_mapping.get(size, size)

def map_temperature(temp):
    """Map temperature to nutrition data format"""
    temp_mapping = {
        'Hot': 'Hot',
        'Ice': 'Ice',
        'Regular': 'Regular',
        'Less': 'Less'
    }
    return temp_mapping.get(temp, temp)

def create_identifier(row):
    """Create identifier from nutrition_to_map row"""
    return f"{row['Ounce']} {clean_name(row['Product Name'])} {row['Temperature L1']} {row['Category']}"

def find_best_match(beverage_type, mapping_df, min_score=75):
    """Find best matching product from mapping data"""
    best_match = None
    best_score = 0
    
    # Extract size and clean name from beverage type
    size, name = extract_size_and_name(beverage_type)
    clean_beverage = clean_name(name)
    
    # Determine if it's a latte
    is_latte = 'latte' in beverage_type.lower()
    
    for _, row in mapping_df.iterrows():
        # Only consider rows with matching size
        if size and size != row['Ounce']:
            continue
            
        # Only match lattes with lattes and non-lattes with non-lattes
        if is_latte != ('latte' in row['Product Name'].lower()):
            continue
            
        # Clean the product name
        clean_product = clean_name(row['Product Name'])
        
        # Calculate match score
        score = fuzz.ratio(clean_beverage, clean_product)
        
        # Boost score for exact tea type matches
        if any(tea in clean_beverage and tea in clean_product 
               for tea in ['jasmine', 'peach', 'oolong', 'ceylon', 'black']):
            score += 10
        
        if score > best_score:
            best_score = score
            best_match = row
    
    if best_score >= min_score:
        return best_match
    return None

def create_nutrition_mapping():
    """Create mapping between nutrition data and combinations"""
    # Read the data
    nutrition_df = pd.read_csv('nutrition_data/nutrition.csv')
    mapping_df = pd.read_csv('processed_data/nutrition_to_map.csv')
    
    # Create mapping results
    mapped_data = []
    
    for _, row in nutrition_df.iterrows():
        # Find best match for identifier
        match = find_best_match(row['Identifier'], mapping_df)
        
        if match is not None:
            mapped_data.append({
                'identifier': row['Identifier'],
                'product_name': match['Product Name'],
                'ounce': match['Ounce'],
                'size': match['Size'],
                'category': match['Category'],
                'temperature_l1': match['Temperature L1']
            })
        else:
            print(f"No good match found for: {row['Identifier']}")
            mapped_data.append({
                'identifier': row['Identifier'],
                'product_name': 'unmapped',
                'ounce': 'unmapped',
                'size': 'unmapped',
                'category': 'unmapped',
                'temperature_l1': 'unmapped'
            })
    
    # Create output DataFrame
    output_df = pd.DataFrame(mapped_data)
    
    # Save to CSV
    output_df.to_csv('processed_data/nutrition_mapped.csv', index=False)
    
    print(f"\nCreated nutrition mapping with shape: {output_df.shape}")
    print("First few rows of mapped data:")
    print(output_df.head())
    
    # Print mapping summary
    total = len(output_df)
    mapped = len(output_df[output_df['product_name'] != 'unmapped'])
    print(f"\nMapping Summary:")
    print(f"Total beverages: {total}")
    print(f"Successfully mapped: {mapped}")
    print(f"Unmapped beverages: {total - mapped}")

if __name__ == "__main__":
    create_nutrition_mapping() 