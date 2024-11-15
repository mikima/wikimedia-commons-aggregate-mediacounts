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
        'prop': 'imageinfo|categories',
        'iiprop': 'url|size|mediatype',
        'cllimit': 'max'  # Get all categories
    }
    response = requests.get(baseurl, params=params)
    #print(response.url)
    #try yo get data
    try:
        data = response.json()
    except:
        print(response.text)
        data = {}
    return data

def process_and_write_csv(chunk, writer, processed_titles):
    """Process a chunk of data and write to CSV."""
    titles = ['File:' + row['name'] for row in chunk]
    chunkdic = {'File:'+i['name']:i for i in chunk}
    if not titles:  # Skip processing if all titles in the chunk have been processed
        return
    enriched_data = enrich(titles)['query']['pages']
    for page in enriched_data:
        try:
            imgtitle = page['title']
            imginfo = page['imageinfo'][0]
            imgcats = [cat['title'] for cat in page['categories']]  # Extract categories
            #print(imgcats)

            writer.writerow([
                chunkdic[imgtitle.replace(" ", "_")]['name'],
                imginfo['url'],
                imginfo['width'],
                imginfo['height'],
                imginfo['width'] * imginfo['height'],
                imginfo['mediatype'],
                chunkdic[imgtitle.replace(" ", "_")]['total'],
                chunkdic[imgtitle.replace(" ", "_")]['internal'],
                "|".join(imgcats)  # Join categories with a separator
            ])
            processed_titles.add(imgtitle)  # Mark title as processed

            #print success overriding previous line
            print(f" SUCCESS: {imgtitle} enriched")
        except KeyError:
            print(f" ERROR: Missing data for {page['title']}")

# Initialize processed_titles as set to keep track of processed titles

processed_titles = set()

#check if results/enriched.csv exists
# if not, create it
try:
    with open('results/enriched.csv', 'r') as file:
        pass
except FileNotFoundError:
    with open('results/enriched.csv', 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['title', 'url', 'width', 'height', 'area', 'mediatype', 'totalrequests', 'internalrequests','categories'])


#read 'enriched.csv', extract column "title" and add to processed_titles
with open('results/enriched.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        #print(row)
        processed_titles.add(row['title'])


# Process input CSV.
# check the name column if present in processed_titles. if present, skip it. otherwise, add it to the list of titles to be enriched
# append the enriched data to results/enriched.csv
        
with open('results/clean_output.csv', 'r') as file, open('results/enriched.csv', 'a', newline='') as outfile:
    reader = csv.DictReader(file)
    
    #create csv writer ahta append new lines
    writer = csv.writer(outfile)

    cleaned = [row for row in reader]

    #print length of processed_titles
    print(f"Processed {len(processed_titles)} rows")

    filtered = [row for row in cleaned if row['name'] in processed_titles]
    refined = [row for row in cleaned if row['name'] not in processed_titles]
            
    #sort refined by total
    refined.sort(key=lambda x: int(x['total']), reverse=True)
    #take the first 10000
    refined = refined[:10000]

    print(f"Filtered {len(filtered)} rows on {len(cleaned)} rows")
    print(f"Processing {len(refined)} rows on {len(cleaned)} rows")
    
    chunks = list(create_chunks(refined, 40))


    for chunk in chunks:
        print (f"Processing chunk {chunks.index(chunk)+1} of {len(chunks)}, {(chunks.index(chunk)+1)/len(chunks)*100}%")
        process_and_write_csv(chunk, writer, processed_titles)

