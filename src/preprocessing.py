import pandas as pd
import os

# Configuration
RAW_DATA_PATH = "data/raw/"
PROCESSED_DATA_PATH = "data/processed/"

# Columns to remove from the raw dataset
COLS_TO_DROP = [
    'id', 'case_number', 'block', 'iucr', 'fbi_code', 
    'x_coordinate', 'y_coordinate', 'location', 'updated_on'
]

# ---------------------------------------------------------------------
# Processing Functions
# ---------------------------------------------------------------------

def preprocess_crime_data(input_filename: str, output_filename: str):
    """
    Load raw CSV, drop redundant columns, and perform basic cleaning.
    """
    input_path = os.path.join(RAW_DATA_PATH, input_filename)
    output_path = os.path.join(PROCESSED_DATA_PATH, output_filename)

    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found!")
        return

    print(f"Loading data from {input_filename}...")
    df = pd.read_csv(input_path)
    
    # 1. Drop redundant columns
    # errors='ignore' ensures the code does not fail if some columns are already missing
    df.drop(columns=COLS_TO_DROP, inplace=True, errors='ignore')
    print(f"Dropped redundant columns: {COLS_TO_DROP}")

    # 2. Handle date format (recommended to do this in preprocessing)
    # Convert string to real datetime objects for later feature extraction (weekday, hour, etc.)
    if 'date' in df.columns:
        print("Converting 'date' column to datetime objects...")
        df['date'] = pd.to_datetime(df['date'])

    # 3. Handle missing values (optional but recommended)
    # For example, rows with missing latitude/longitude usually cannot be used for spatial analysis
    initial_len = len(df)
    df.dropna(subset=['latitude', 'longitude', 'district'], inplace=True)
    print(f"Removed {initial_len - len(df)} rows with missing critical values.")

    # 4. Save cleaned data
    os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Successfully saved cleaned data to: {output_path}")
    print("-" * 30)

if __name__ == "__main__":
    print("Starting data preprocessing...")

    preprocess_crime_data(
        "chicago_crimes_2015_2024_raw.csv", 
        "chicago_crimes_2015_2024_cleaned.csv"
    )

    preprocess_crime_data(
        "chicago_crimes_2025_raw.csv", 
        "chicago_crimes_2025_cleaned.csv"
    )

    print("Preprocessing completed!")