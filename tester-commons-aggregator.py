from time import process_time
import csv

def extractListFromFile(file, limit):

    with open(file) as data:

        # create list, extract name and number of requests
        splitted = [[i.split('\t')[0], int(i.split('\t')[2]),int(i.split('\t')[22])] for i in data]

        print('splitted')

        # filter the list
        filtered = list(filter(lambda row: row[2] > limit and 'commons' in row[0], splitted))
        
        print('filtered')

        ordered = sorted(filtered, key=lambda x: x[2], reverse=True)

        print('ordered')

        #pprint(ordered)

        # save it
        # open the file in the write mode
        with open(file.split(".")[0]+'.'+file.split(".")[1]+"_out.csv", 'w') as f:
            # create the csv writer
            writer = csv.writer(f)
            for row in ordered:
                name = row[0].split("/")[-1]
                # write a row to the csv file
                writer.writerow([name,row[1],row[2]])


extractListFromFile('wiki-dump-test/mediacounts.2022-01-01.v00.tsv',100)