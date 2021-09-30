import glob
import os
import sys

filenames = glob.glob("*.csv")

header_saved = False
for filename in filenames:
    testName = os.path.splitext(filename)[0]
    tes = testName.split("_")[1]
    src = testName.split("Src")[1]
    print(tes)
    with open(filename) as fin:
        filePath = "latency" + tes + ".log"
        fileExists = os.path.isfile(filePath)
        with open(filePath, 'a') as fout:
            if (fileExists == False):
                fout.write("SourceCnt,ts,latency\n")
            for line in fin:
                line = src + "," + line
                fout.write(line)