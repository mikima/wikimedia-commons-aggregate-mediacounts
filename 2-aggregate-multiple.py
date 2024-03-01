import pandas as pd
import os

def aggregate_csv(folder_path):
    # Initialize an empty DataFrame to store aggregated data
    aggregated_df = pd.DataFrame()

    # Iterate over each file in the specified folder
    for file in os.listdir(folder_path):
        # print that you are analyzing the file
        print(f'Analyzing {file}')
        # Check if the file is a CSV file
        if file.endswith('.csv'):

            print(f' Opening {file}')
            # Construct the full file path
            file_path = os.path.join(folder_path, file)

            print(f' Reading {file}')
            # Read the CSV file into a DataFrame
            temp_df = pd.read_csv(file_path)
            # Check if the aggregated DataFrame is empty; if so, initialize it with the first file
            if aggregated_df.empty:
                aggregated_df = temp_df
            else:
                print(f' Aggregating {file}')
                # Aggregate the data by summing 'total' and 'internal' for each 'name'
                aggregated_df = pd.concat([aggregated_df, temp_df]).groupby('name', as_index=False).sum()

    # Output file path
    output_file = os.path.join(folder_path, 'aggregated_output.csv')
    
    # Write the aggregated DataFrame to a new CSV file
    aggregated_df.to_csv(output_file, index=False)

    print(f'Aggregated data written to {output_file}')

# Example usage
folder_path = 'out-10'  # Make sure to replace this with the correct path to your folder
aggregate_csv(folder_path)