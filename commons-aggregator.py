import urllib.request
import time
import csv
import os
from datetime import date, timedelta

#print
print('updated version 3 for linux')
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
    aprint('downloaded', t0)


    # open the file using os bzip2 function
    os.system('bzip2 -d '+name)
    aprint('decompressed', t0)

    # open the tsv file, load only column 0, 2 and 22
    # column 0 is the name, column 2 contains the total requests, and column 22 are the requests from mediawiki
    # Variable to store the filtered rows
    filtered_rows = []

    # Open the TSV file
    with open(name.replace('.bz2',''), mode='r', newline='', encoding='utf-8') as file:
        # Create a reader to read the file with tab as a delimiter
        tsv_reader = csv.reader(file, delimiter='\t')
        
        # Loop over each row in the TSV file
        for row in tsv_reader:
            # Check if the row has at least 23 columns and meets the specific conditions
            if len(row) > 22 and "commons" in row[0]:
                try:
                    # Check if the value in column 22 is greater than 100
                    # Column indexes are 0-based, so index 22 is the 23rd column
                    if float(row[22]) > 100:
                        # If conditions are met, save the row
                        filtered_rows.append([row[0], row[2], row[22]])
                except ValueError:
                    # In case the value in column 22 is not a number, skip this row
                    continue

    aprint('read', t0)

    ordered = sorted(filtered_rows, key=lambda x: x[2], reverse=True)

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

        #delete the original file
        #if the original file is still there, delete it
        
        try:
            os.system('rm '+name.replace('.bz2',''))
            aprint('deleted uncompressed file', t0)
        except Exception as e:  # Add except clause here
            print(e)
            pass


        #delete the compressed file
        try:
            os.system('rm '+name)
            aprint('deleted compressed file', t0)
        except Exception as e:  # Add except clause here
            print(e)
            pass

        #zip the file
        os.system('gzip '+outCsv)
        aprint('zipped', t0)

        #delete the unzipped file, if present
        #delete the compressed file
        os.system('rm '+outCsv)


    start_date = date(2023, 1, 10)
end_date = date(2023, 12, 31)

delta = end_date - start_date   # returns timedelta

for i in range(delta.days + 1):
    day = start_date + timedelta(days=i)
    url = "https://dumps.wikimedia.org/other/mediacounts/daily/2023/mediacounts."+str(day)+".v00.tsv.bz2"
    print(url)
    loadDecompress(url, 100)