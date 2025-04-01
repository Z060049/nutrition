import pandas as pd
import itertools
import os

# Clear existing combinations file if it exists
if os.path.exists('all_combinations.csv'):
    os.remove('all_combinations.csv')
    print("Cleared existing combinations file")

# Read all the CSV files
products_df = pd.read_csv('options/product.csv')
sizes_df = pd.read_csv('options/size.csv')
temperatures_df = pd.read_csv('options/temperature.csv')
sugars_df = pd.read_csv('options/sugar.csv')
milks_df = pd.read_csv('options/milk.csv')

# Get lists of unique values
products = products_df['Product Name'].unique()
sizes = sizes_df['Size Name'].unique()
temperatures = temperatures_df['Ice Level'].unique()
sugars = sugars_df['Sugar Level'].unique()
milks = milks_df['Alternative Milk'].unique()

# Generate all possible combinations
combinations = list(itertools.product(products, sizes, temperatures, sugars, milks))

# Create a DataFrame from the combinations
combinations_df = pd.DataFrame(combinations, columns=['Product', 'Size', 'Temperature', 'Sugar', 'Milk'])

# Save to CSV
combinations_df.to_csv('all_combinations.csv', index=False)
print(f"Generated {len(combinations)} combinations")
print("First few combinations:")
print(combinations_df.head()) 