import glob
import os
import sys

if os.path.exists("latency.out"):
    os.remove("latency.out")
else:
    print("The file does not exist")

interesting_files = glob.glob("*.csv")

header_saved = False
with open('latency.out', 'wb') as fout:
    for filename in interesting_files:
        header = os.path.splitext(filename)
        header = str(header[0]).split("Latency_",1)[1]
        with open(filename) as fin:
            header = next(fin)
            if not header_saved:
                fout.write(header)
                header_saved = True
            for line in fin:
                line = header + "," + line
                fout.write(line)
