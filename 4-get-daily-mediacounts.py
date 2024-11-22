import csv
import requests
import urllib.parse
import time
import hashlib
from datetime import datetime

def construct_api_url(file_title, start_date, end_date):
    # Replace spaces with underscores and strip whitespace
    file_title = file_title.replace(' ', '_').strip()
    # Remove 'File:' prefix if present
    if file_title.startswith('File:'):
        file_title = file_title[len('File:'):]

    # Encode the filename to handle special characters
    file_title_encoded = urllib.parse.quote(file_title, safe='')

    # Compute the MD5 hash of the original filename (before encoding)
    md5_hash = hashlib.md5(file_title.encode('utf-8')).hexdigest()
    first_char = md5_hash[0]
    first_two_chars = md5_hash[:2]

    # Construct the file path using the encoded filename
    file_path = f"/wikipedia/commons/{first_char}/{first_two_chars}/{file_title_encoded}"

    # URL-encode the entire file path, including slashes
    encoded_file_path = urllib.parse.quote(file_path, safe='')

    # Construct the API URL
    api_url = (
        f"https://wikimedia.org/api/rest_v1/metrics/mediarequests/per-file/"
        f"all-referers/all-agents/{encoded_file_path}/daily/{start_date}/{end_date}"
    )

    return api_url

def fetch_media_requests(api_url):
    headers = {'User-Agent': 'MediaRequestDataCollector/1.0 (your_email@example.com)'}
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 404:
        # No data available for this file
        return None
    else:
        print(f"API request failed with status code {response.status_code} for URL: {api_url}")
        return None

def format_timestamp(timestamp):
    # Original timestamp format is 'YYYYMMDD00'
    # We need to convert it to 'YYYY-MM-DD'
    date_str = timestamp[:-2]  # Remove the '00' at the end
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    formatted_date = date_obj.strftime('%Y-%m-%d')
    return formatted_date

def main():
    input_csv = 'results/clean_output.csv'  # Your TSV file
    output_csv = 'results/media_requests_output.csv'
    start_date = '20230101'  # January 1, 2023
    end_date = '20231231'    # December 31, 2023

    # Open the input TSV file
    with open(input_csv, mode='r', newline='', encoding='utf-8') as csvfile_in:
        reader = csv.DictReader(csvfile_in, delimiter='\t')
        fieldnames = reader.fieldnames + ['timestamp', 'requests']

        # Open the output TSV file
        with open(output_csv, mode='w', newline='', encoding='utf-8') as csvfile_out:
            writer = csv.DictWriter(csvfile_out, fieldnames=fieldnames, delimiter='\t')
            writer.writeheader()

            for row in reader:
                file_title = row['title']
                print(f"Processing file: {file_title}")
                api_url = construct_api_url(file_title, start_date, end_date)
                print(f"Constructed API URL: {api_url}")

                if not api_url:
                    continue  # Skip to the next row if URL construction failed

                # Fetch the media requests data
                data = fetch_media_requests(api_url)
                if data and 'items' in data:
                    print(f"Adding {file_title}")
                    for item in data['items']:
                        # Add the formatted timestamp and requests to the row
                        output_row = row.copy()
                        formatted_date = format_timestamp(item['timestamp'])
                        output_row['timestamp'] = formatted_date
                        output_row['requests'] = item['requests']
                        writer.writerow(output_row)
                else:
                    print(f"No data found for {file_title}")

                # Respectful delay to avoid hitting API rate limits
                #time.sleep(0.1)

if __name__ == "__main__":
    main()