#for each file in the folder "out", extract the first 100000 lines and save them in a new folder "out-10"

import os
import sys
import glob

path = "out"

if not os.path.exists(path):
    print("Error: folder 'out' does not exist")
    sys.exit()

path2 = "out-10"
if not os.path.exists(path2):
    os.makedirs(path2)
    
#list all files in the folder
list = os.path.join(path, '*.csv.gz')


for filename in sorted(glob.glob(list)):

    #new filename
    newfilename = os.path.join(path2, "trim"+ os.path.basename(filename).replace(".gz",""))

    #check if it is already processed
    if os.path.exists(newfilename):
        print("File already processed: " + filename)
        continue


    #try to open the file
    try:

        print("Processing file: " + filename)

        #unzip the file
        os.system("gunzip " + filename)
        
        with open(filename.replace(".gz",""), 'r') as f:
            lines = f.readlines()
            lines = lines[:100000]
           
            with open(newfilename, 'w') as f2:
                for line in lines:
                    f2.write(line)
        
        #delete uncompressed file
        
        os.system("rm " + filename.replace(".gz",""))
    
    except:
        print("! Error processing file: " + filename)
        continue