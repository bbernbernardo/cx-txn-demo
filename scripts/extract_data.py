import pandas as pd
import os

# Define the path to the source file
file_path = os.path.join("..", "data", "customer_transactions.csv")

# Read the CSV file, forcing all columns to be read as strings
def extract_csv():
    print(f"Extracting csv {file_path} ...")
    df = pd.read_csv(file_path, dtype=str)
    return df

# print(extract_csv())
extract_csv()