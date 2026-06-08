import csv
import json
import os
import psycopg2
from db_config import LEADER_DB, FOLLOWER_DB

# Results directory
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

def ensure_results_dir():
    """
    Creates the directory where experiment results will be saved.
    """
    if not os.path.exists(RESULTS_DIR):
        os.makedirs(RESULTS_DIR)

def save_to_csv(filename, headers, rows):
    """
    Saves experiment data in CSV format.
    """
    ensure_results_dir()
    filepath = os.path.join(RESULTS_DIR, filename)
    try:
        with open(filepath, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
        print(f"Data successfully saved to: {filepath}")
        return filepath
    except Exception as e:
        print(f"ERROR: CSV saving error: {e}")
        return None

def save_to_json(filename, data):
    """
    Saves experiment data in JSON format.
    """
    ensure_results_dir()
    filepath = os.path.join(RESULTS_DIR, filename)
    try:
        with open(filepath, mode="w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, default=str)
        print(f"Data successfully saved to: {filepath}")
        return filepath
    except Exception as e:
        print(f"ERROR: JSON saving error: {e}")
        return None

def print_table(headers, rows):
    """
    Prints data in a neatly aligned table format in the terminal.
    """
    if not rows:
        print("Table is empty.")
        return
        
    # Find the maximum width of each column
    col_widths = [len(str(h)) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val)))
            
    # Create the format string
    row_format = " | ".join([f"{{:<{w}}}" for w in col_widths])
    border = "-+-".join(["-" * w for w in col_widths])
    
    print("\n" + border)
    print(row_format.format(*headers))
    print(border)
    for row in rows:
        print(row_format.format(*[str(val) for val in row]))
    print(border + "\n")
