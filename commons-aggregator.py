import urllib.request
import time
import csv
import os
import sys
from datetime import date, timedelta
import bz2
import gzip
import heapq

print('Updated version with error handling for incomplete downloads')

# Check if the correct number of arguments is passed
if len(sys.argv) != 3:
    print("Usage: script.py <start_date> <end_date>")
    print("Dates must be in YYYY-MM-DD format.")
    sys.exit(1)

# Try to parse the start and end dates from the command-line arguments
try:
    start_date = date.fromisoformat(sys.argv[1])
    end_date = date.fromisoformat(sys.argv[2])
except ValueError as e:
    print(f"Error parsing dates: {e}")
    print("Ensure the dates are in YYYY-MM-DD format.")
    sys.exit(1)

def show_progress(block_num, block_size, total_size):
    downloaded = block_num * block_size
    percent = (downloaded / total_size) * 100
    print(f"Downloading: {percent:.2f}% - {downloaded // (1024 * 1024)}MB/{total_size // (1024 * 1024)}MB", end='\r')

def loadDecompress(url, min_requests, limit):
    t0 = time.time()

    def aprint(text, startTime):
        print(f"{text} - Elapsed Time: {round(time.time() - startTime, 2)}s")

    print(f'Loading {url}')

    raw_zipped = url.split("/")[-1]
    out_zipped = f"out/{raw_zipped.replace('.bz2', '.csv.gz')}"

    if os.path.exists(out_zipped):
        print(f'{out_zipped} already processed.')
        return

    # Download the file if it doesn't exist
    if not os.path.exists(raw_zipped):
        print(f'Downloading {url}')
        try:
            urllib.request.urlretrieve(url, raw_zipped, show_progress)
            print()  # For a clean line after progress
            aprint('Downloaded', t0)
        except Exception as e:
            print(f"Error downloading {url}: {e}")
            return

    try:
        # Use a heap to keep track of top entries
        top_entries = []

        with bz2.open(raw_zipped, mode='rt', encoding='utf-8') as file:
            tsv_reader = csv.reader(file, delimiter='\t')
            # row 2 is total usage
            # row 3 original
            # row 7 transcoded
            # rows 8-13 are disaggregated by transcoded
            # row 22 internal refereers
            # row 23 external refereers
            # row 24 unknown refereers
            for row in tsv_reader:
                if len(row) > 22 and "commons" in row[0]:
                    try:
                        internal_requests = int(row[22])
                        external_requests = int(row[23])
                        transcoded_sm = int(row[8]) + int(row[9]) + int(row[10])
                        transcoded_lg = int(row[11]) + int(row[12]) + int(row[13])
                        original = int(row[3])
                        if internal_requests > min_requests:
                            # Use negative value because heapq is a min-heap
                            heapq.heappush(top_entries, (-internal_requests, row[0], row[2], original, transcoded_sm, transcoded_lg, internal_requests, external_requests))
                            # Limit the heap size to 'limit'
                            if len(top_entries) > limit:
                                heapq.heappop(top_entries)
                    except ValueError:
                        continue

        aprint('Processed', t0)

        # Convert heap to a sorted list
        top_entries.sort(reverse=True)
        aprint('Sorted', t0)

        # Write the output directly to a gzip file
        os.makedirs('out', exist_ok=True)
        with gzip.open(out_zipped, 'wt', encoding='utf-8') as f:
            writer = csv.writer(f)
            headers = ['name', 'total', 'original', 'transcoded_sm', 'transcoded_lg', 'internal', 'external']
            writer.writerow(headers)
            for entry in top_entries:
                writer.writerow([entry[1], entry[2], entry[3], entry[4], entry[5], entry[6], entry[7]])

        aprint('Saved', t0)

    except (EOFError, OSError) as e:
        print(f"Error processing {raw_zipped}: {e}. Skipping file.")
        os.remove(raw_zipped)  # Remove the corrupted file to retry later
        return
    finally:
        # Ensure the file is cleaned up
        if os.path.exists(raw_zipped):
            os.remove(raw_zipped)
        aprint('Cleaned up', t0)

delta = end_date - start_date

for i in range(delta.days + 1):
    day = start_date + timedelta(days=i)
    url = f"https://dumps.wikimedia.org/other/mediacounts/daily/{day.year}/mediacounts.{day}.v00.tsv.bz2"
    loadDecompress(url, 1000, 2000000)