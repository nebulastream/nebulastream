import glob
import os
import sys


if os.path.exists("output.csv"):
  os.remove("output.csv")
else:
  print("The file does not exist")

print 'Number of arguments:', len(sys.argv), 'arguments.'
if len(sys.argv) != 2:
	print 'error change reason has to be provided'
	exit()
print 'Argument List:', str(sys.argv)



interesting_files = glob.glob("*.csv") 

header_saved = False
with open('output.csv','wb') as fout:
    for filename in interesting_files:
        with open(filename) as fin:
            header = next(fin).rstrip() + ",change" + '\n'
            if not header_saved:
                fout.write(header)
                header_saved = True
            for line in fin:
            	line = line.rstrip() + ',' + str(sys.argv[1])+ '\n'
                fout.write(line) 