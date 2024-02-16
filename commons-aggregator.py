import urllib.request
import bz2
import time
import csv
import os
from datetime import date, timedelta

#print
print('updated version')
#call it
# 
def loadDecompress(url, limit):

    # get initial time
    t0 = time.time()

    # advancement printer function
    def aprint(text, startTime):
        print("\t",text, round(time.time()-startTime,2))

    # prepare progressbar
    def show_progress(block_num, block_size, total_size):
        print('downloading', end=' ')
        print(str(round(block_num * block_size / total_size *100,2))+"% - " + str(round(time.time()-t0,0)) + ' seconds', end="\r")

    print(t0)

    #####################
    # start the process #
    #####################

    print('loading',url)

    # download the file
    name = url.split("/")[-1]
    urllib.request.urlretrieve(url, name, show_progress)
    print('')
    aprint('open', t0)

    # open the file
    zipfile = bz2.BZ2File(name) 
    aprint('read', t0)

    # get the decompressed data
    binaries = zipfile.read() 
    aprint('decompress', t0)

    #decode it
    data = binaries.decode().splitlines()
    aprint('decoded', t0)

    # create list, extract name and number of requests
    # column 0 is the name, column 2 contains the total requests, and column 22 are the requests from mediawiki
    # for more info see https://dumps.wikimedia.org/other/mediacounts/README.txt
    splitted = [[i.split('\t')[0], int(i.split('\t')[2]),int(i.split('\t')[22])] for i in data]
    aprint('splitted', t0)

    # filter the list
    filtered = list(filter(lambda row: row[2] > limit and 'commons' in row[0], splitted))   
    aprint('filtered', t0)

    ordered = sorted(filtered, key=lambda x: x[2], reverse=True)

    aprint('ordered', t0)

    # save it
    outCsv = 'out/'+name.split(".")[0]+'.'+name.split(".")[1]+".csv"
    # open the file in the write mode
    with open(outCsv, 'w') as f:
        # create the csv writer
        writer = csv.writer(f)
        headers = ['name','total','internal']
        writer.writerow(headers)

        # write all the lines
        for row in ordered:
            # write a row to the csv file
            writer.writerow([row[0],row[1],row[2]])
    
    aprint('saved', t0)

    #zip the file
    os.system('gzip '+outCsv)
    aprint('zipped', t0)

    #remove the downloaded file
    os.remove(name)
    aprint('removed dowloaded file', t0)


#loadDecompress("https://projects.densitydesign.org/sample.tsv.bz2", 0)
#loadDecompress("https://dumps.wikimedia.org/other/mediacounts/daily/2022/mediacounts.2022-01-07.v00.tsv.bz2", 100)

#

start_date = date(2023, 1, 1)
end_date = date(2023, 12, 31)

delta = end_date - start_date   # returns timedelta

for i in range(delta.days + 1):
    day = start_date + timedelta(days=i)
    url = "https://dumps.wikimedia.org/other/mediacounts/daily/2023/mediacounts."+str(day)+".v00.tsv.bz2"
    print(url)
    loadDecompress(url, 100)