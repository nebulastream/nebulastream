import glob
import os
import sys


print 'Number of arguments:', len(sys.argv), 'arguments.'
if len(sys.argv) != 2:
    print 'error change reason has to be provided'
    exit()
print 'Argument List:', str(sys.argv)


filenames = glob.glob("*.csv") 

header_saved = False
for filename in filenames:
    print(os.path.splitext(filename)[0])
    testName = os.path.splitext(filename)[0]
    with open('gen_' + testName + '.csv' ,'wb') as fout:
        header_saved = False
        with open(filename) as fin:
            header = next(fin).rstrip() + ",change" + '\n'
            if not header_saved:
                fout.write(header)
                header_saved = True
            for line in fin:
                line = line.replace("E2ERunner.csv", testName)
                line = line.rstrip() + ',' + str(sys.argv[1])+ '\n'
                fout.write(line)