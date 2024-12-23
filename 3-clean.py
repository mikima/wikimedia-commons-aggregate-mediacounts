#!/usr/bin/env python3
import csv
import os
from urllib.parse import unquote

def clean_and_sort_aggregated_data(
        input_file='results/aggregated_data.csv',
        output_file='results/3_aggregated_clean.csv'
    ):
    rows = []
    
    # Read the aggregated data
    with open(input_file, 'r', encoding='utf-8', newline='') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        
        for row in reader:
            name = row["name"]
            
            # Split on "/" to isolate the final file part
            filename = name.split("/")[-1]
            
            # Decode URL-encoded characters (e.g., %2C -> ,)
            decoded_filename = unquote(filename)
            
            # Replace underscores with spaces
            spaced_filename = decoded_filename.replace("_", " ")
            
            # Store this cleaned filename in a new column 'title'
            row["title"] = spaced_filename
            
            rows.append(row)
    
    # Sort rows by 'total' in descending order
    rows.sort(key=lambda r: int(r["total"]), reverse=True)
    
    # Write to the output CSV
    # Make sure 'title' is included among fieldnames
    if "title" not in fieldnames:
        fieldnames.append("title")
        
    with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

if __name__ == '__main__':
    clean_and_sort_aggregated_data()