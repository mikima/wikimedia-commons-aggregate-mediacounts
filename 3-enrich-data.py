import requests
import csv
from pprint import pprint

def create_chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def enrich(titles):
    """Enrich titles with data from Wikimedia Commons API."""
    baseurl = 'https://commons.wikimedia.org/w/api.php'
    params = {
        'action': 'query',
        'formatversion': '2',
        'format': 'json',
        'titles': "|".join(titles),
        'prop': 'imageinfo',
        'iiprop': 'url|size|mediatype'
    }
    response = requests.get(baseurl, params=params)
    data = response.json()
    return data

def process_and_write_csv(chunk, writer, processed_titles):
    """Process a chunk of data and write to CSV."""
    titles = ['File:' + row['name'] for row in chunk if 'File:' + row['name'] not in processed_titles]
    chunkdic = {'File:'+i['name']:i for i in chunk if 'File:' + row['name'] not in processed_titles}
    if not titles:  # Skip processing if all titles in the chunk have been processed
        return
    enriched_data = enrich(titles)['query']['pages']
    for page in enriched_data:
        try:
            imgtitle = page['title']
            imginfo = page['imageinfo'][0]
            writer.writerow([
                imgtitle,
                imginfo['url'],
                imginfo['width'],
                imginfo['height'],
                imginfo['width'] * imginfo['height'],
                imginfo['mediatype'],
                chunkdic[imgtitle.replace(" ", "_")]['total'],
                chunkdic[imgtitle.replace(" ", "_")]['internal']
            ])
            processed_titles.add(imgtitle)  # Mark title as processed
            print(f" Processed {imgtitle}")
        except KeyError:
            print(f"ERROR: Missing data for {page['title']}")

# Initialize set to keep track of processed titles
processed_titles = set()

#read 'enriched.csv', extract column "title" and add to processed_titles
with open('enriched.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        processed_titles.add(row['title'])

# Process input CSV and enrich data
with open('aggregated_output.csv', 'r') as file, open('enriched.csv', 'w', newline='') as outfile:
    reader = csv.DictReader(file)
    writer = csv.writer(outfile)
    writer.writerow(['title', 'url', 'width', 'height', 'area', 'mediatype', 'totalrequests', 'internalrequests'])
    
    refined = [row for row in reader]
    for row in refined:
        row['name'] = requests.utils.unquote(row['name'].split('/')[-1])
    
    chunks = list(create_chunks(refined, 40))
    for chunk in chunks:
        process_and_write_csv(chunk, writer, processed_titles)
