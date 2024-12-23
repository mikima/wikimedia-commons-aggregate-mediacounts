import os
import csv
from collections import defaultdict

def aggregate_csv_data(input_folder='out-10', output_file='results/aggregated_data.csv'):
    # Dictionary to store aggregated sums by 'name'
    aggregated_data = defaultdict(lambda: {
        'total': 0,
        'original': 0,
        'transcoded_sm': 0,
        'transcoded_lg': 0,
        'internal': 0,
        'external': 0
    })
    
    # Loop through all files in the specified folder
    for filename in os.listdir(input_folder):
        if not filename.endswith('.csv'):
            # Skip non-CSV files
            continue
        
        # Print which file is being processed
        print(f"Processing file: {filename}")
        
        # Build the full path to the CSV file
        filepath = os.path.join(input_folder, filename)
        
        # Read the CSV file
        with open(filepath, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                name = row['name']
                
                # Accumulate the sums
                aggregated_data[name]['total'] += int(row['total'])
                aggregated_data[name]['original'] += int(row['original'])
                aggregated_data[name]['transcoded_sm'] += int(row['transcoded_sm'])
                aggregated_data[name]['transcoded_lg'] += int(row['transcoded_lg'])
                aggregated_data[name]['internal'] += int(row['internal'])
                aggregated_data[name]['external'] += int(row['external'])
    
    # Write the aggregated results to the output CSV
    fieldnames = ['name', 'total', 'original', 'transcoded_sm', 'transcoded_lg', 'internal', 'external']
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for name, sums in aggregated_data.items():
            writer.writerow({
                'name': name,
                'total': sums['total'],
                'original': sums['original'],
                'transcoded_sm': sums['transcoded_sm'],
                'transcoded_lg': sums['transcoded_lg'],
                'internal': sums['internal'],
                'external': sums['external']
            })

if __name__ == '__main__':
    aggregate_csv_data()