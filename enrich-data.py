import requests
import csv
from pprint import pprint


#function to create chuncks
def create_chunks(lst, n):
    # Yield successive n-sized chunks from lst.
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def enrich(titles):
    # get sizes using API
    # example: https://commons.wikimedia.org/w/api.php?action=query&format=json&iiprop=url|size|mediatype|commonmetadata&prop=imageinfo&titles=File:Icons-mini-file_acrobat.gif|File:Background-bubbles-blue.svg
    # titles = ['File:Icons-mini-file_acrobat.gif','File:Background-bubbles-blue.svg']
    
    baseurl = 'https://commons.wikimedia.org/w/api.php'
    params = {}
    params['action'] = 'query'
    params['formatversion'] = '2'
    params['format'] = 'json'
    params['titles'] = "|".join(titles)
    params['prop'] = 'imageinfo'
    params['iiprop'] = 'url|size|mediatype'

    r = requests.get(baseurl, params = params)
    data = r.json()
    return data


#enrich()
output = []

with open('tests/top1000year.csv', 'r') as file:
    # create the reader
    reader = csv.DictReader(file.readlines())#[0:2000])


    with open('tests/enriched.csv', 'w') as outfile:
        # create the csv writer
        writer = csv.writer(outfile)
        # write the headers
        writer.writerow(['title','url','width','height','area', 'mediatype', 'totalrequests','internalrequests'])
        
        # divide it in chuks
        chunks = list(create_chunks(list(reader), 10))
        print('chuncks created')

        #for each chunk, call apis and write out the results
        for chunk in chunks:
            titles = ['File:'+i['name'] for i in chunk]
            chunkdic = {'File:'+i['name']:i for i in chunk}
            #print(titles)
            enriched = enrich(titles)['query']['pages']

            pprint(enriched)
            for page in enriched:
                #pprint(page['title'])
                #pprint(page)
                try:
                    imgtitle = page['title']
                    imgurl = page['imageinfo'][0]['url']
                    imgwidth = page['imageinfo'][0]['width']
                    imgheight = page['imageinfo'][0]['height']
                    imgmediatype = page['imageinfo'][0]['height']
                    reqs = chunkdic[imgtitle.replace(" ","_")]
                    #print(chunkdic[imgtitle])
                    pprint([imgtitle,imgurl,imgwidth,imgheight,imgheight*imgwidth, imgmediatype, reqs['total'],reqs['internal']])
                    writer.writerow([imgtitle,imgurl,imgwidth,imgheight,imgheight*imgwidth, imgmediatype, reqs['total'],reqs['internal']])
                except:
                    print("ERROR:", page['title'])
                    #writer.writerow([imgtitle])