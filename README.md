#how to use this script

1. first run clean up file cleanup.py
2. then run generate_product_mix.py, this will write to the spread sheet product_to_map
3. then run np_map.py, this will generate a mapping saved in product_to_map with column G and H
4. 

# Nutrition Data Processing

This project processes and maps nutrition data for various beverages. It includes scripts for cleaning, processing, and mapping nutrition information from raw data to a standardized format.

## Project Structure

- `nutrition.py`: Main script for processing nutrition data
- `np_map.py`: Script for mapping nutrition data to standardized format
- `cleanup.py`: Utility script for cleaning up temporary files
- `processed_data/`: Directory containing processed data files
- `options/`: Directory containing option files

## Data Files

- `nutrition_raw.csv`: Raw nutrition data
- `nutrition_processed.csv`: Processed nutrition data
- `nutrition_mapped.csv`: Mapped nutrition data with standardized format
- `nutrition_to_map.csv`: Mapping configuration file

## Usage

1. Ensure all required Python packages are installed:
   ```bash
   pip install -r requirements.txt
   ```

2. Run the nutrition processing script:
   ```bash
   python nutrition.py
   ```

3. Run the mapping script:
   ```bash
   python np_map.py
   ```

## Output

The script generates a mapped nutrition data file (`nutrition_mapped.csv`) with the following columns:
- beverage_type
- product_name
- ounce
- size
- category

## Requirements

- Python 3.x
- pandas
- fuzzywuzzy 