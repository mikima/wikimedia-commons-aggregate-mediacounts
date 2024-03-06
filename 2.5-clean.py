import requests
import csv

with open('results/aggregated_output.csv', 'r') as file, open('results/clean_output.csv', 'a', newline='') as outfile:
    reader = csv.DictReader(file)
    
    # Create a DictWriter object with fieldnames matching the reader's fieldnames
    writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
    
    cleaned = [row for row in reader]
    for row in cleaned:
        row['name'] = requests.utils.unquote(row['name'].split('/')[-1])

    # Write headers only if the file is new or empty
    if outfile.tell() == 0:
        writer.writeheader()
    
    # Write the cleaned data to the new file
    writer.writerows(cleaned)
