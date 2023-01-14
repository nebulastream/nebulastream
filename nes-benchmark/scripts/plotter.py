import matplotlib.pyplot as plt
import re
import numpy as np

""""
def parseAllCSV():                  
    for i in range(numOfQueries):
        queryName = "q" + str(i+1)
        file = open()"""

def getIndexOfCol(cols : list, value : str):
    n = len(cols)
    ret = 0
    for z in range(n):
        ret = z
        if cols[z] == value:
            break
    print(ret)
    return ret


def plotInfo(info: str):
    plotList = []

    value = 0.0
    values = []
    index = getIndexOfCol(cols, info)
    for i in range(n):
        line = listOfLines[i]
        values = line.split(",")
        
        value = float(values[index])
        
        plotList.append(value)

    plt.figure()
    plt.plot(range(n),plotList,'ro')

    print(plotList)
    plt.ylabel(info)
    plt.xlabel('#Execution')
    plt.savefig(info + '.png')

    return

numOfQueries = 22
queryName = "q1"
n = 3
fileString = "../../build/nes-benchmark/"
file = open( fileString + queryName + ".csv","r")


file.readline() # skip 1st line
colString = file.readline()
cols = colString.split(",")

listOfLines = []
for j in range(n):
    listOfLines.append(file.readline())


nCols = len(cols)
for i in range(2,10):
    info = cols[i]
    print(info)
    plotInfo(info)