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

for filename in glob.glob(os.path.join(path, '*.csv')):

    print("Processing file: " + filename)
    
    with open(filename, 'r') as f:
        lines = f.readlines()
        lines = lines[:100000]
        newfilename = os.path.join(path2, "trim"+ os.path.basename(filename))
        with open(newfilename, 'w') as f2:
            for line in lines:
                f2.write(line)