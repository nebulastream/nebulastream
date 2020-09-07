from argparse import ArgumentParser
from argparse import ArgumentTypeError
from datetime import datetime


import os
import subprocess
import json
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import math

BENCHMARK_PARAMS = ["Updates", "Keys", "Type of Distribution", "Threads"]
PRE_SI_UNIT = ["", "k", "M", "B"]
CHUNK_SIZE = 8


def parseArguments():
	parser = ArgumentParser()
	parser.add_argument("-b", "--benchmark", action="store", dest="benchmark", help="Location of statemanager benchmark", required=True)
	parser.add_argument("-p", "--plotfolder", action="store", dest="plotFolder", help="Name of folder in which all plots will be stored", required=True)

	args = parser.parse_args()
	return (args)


def fileFolderExists(file):
	return os.path.exists(file)


def createFolder(folder):
	if fileFolderExists(folder):
		print(f"{folder} already exists")
	else:
		os.mkdir(folder)
		print(f"{folder} already exists")


def runBenchmark(benchmark, logFile):
	cmdString = f"./{benchmark} --benchmark_format=json --benchmark_out={logFile}"
	print(cmdString)
	# with open("/dev/null") as f:
	subprocess.check_call(cmdString.split())#, stdout=f)



#Loading everything into a dataframe
def loadDataIntoDataFrame(logFile):
	with open(logFile) as file:
		jsonFile = json.load(file)
	jsonFile = jsonFile["benchmarks"]
	return pd.DataFrame.from_dict(jsonFile)

def millify(n):
    n = float(n)
    idx = max(0,min(len(PRE_SI_UNIT)-1,
                        int(math.floor(0 if n == 0 else math.log10(abs(n))/3))))

    return '{:.1f}{}'.format(n / 10**(3 * idx), PRE_SI_UNIT[idx])


def createPlotableGroups(df):
	# Deleting column run_name as this column is not necessary
	df = df.drop(columns=["run_name"])

	# Dropping all records with aggregate_name median as mean is used
	df = df.drop(df[df.aggregate_name == "median"].index)

	# Splitting name column into new column benchmark_params and name
	# Also writing params in scientific notation
	benchmarkParams = []
	benchmarkNames = []
	regexBenchmarkRunName = r"([^\/]*)\/.*"
	regexBenchmarkParams = r"[^\/]*/((\d|\/)+)\/repeats"

	for index, row in df.iterrows():
		name = re.search(regexBenchmarkRunName, row["name"])
		params = re.search(regexBenchmarkParams, row["name"])

		if name and params:
			name = name.group(1)
			params = "/".join([millify(int(item)) for item in params.group(1).split("/")])
		else:
			continue

		benchmarkParams.append(params)
		benchmarkNames.append(name)

	df.drop(columns=["name"])
	df["name"] = benchmarkNames
	df["params"] = benchmarkParams

	# Also creating new column stddev, as stddev is needed later for 
	stddev = []
	for index, row in df.iterrows():
		if row["aggregate_name"] != "stddev":
			continue

		stddev.append(row["items_per_second"])


	df = df.drop(df[df.aggregate_name == "stddev"].index)
	df["stddev"] = stddev

	return df.groupby(["name"])



class DataPoint(object):
	x = "N/A"
	ySum = 0
	errSum = 0
	amount = 1


# Removes possible duplicates by building mean and deleting one of the duplicates
def removeDuplicates(xData, yData, yErr):

	dataPoints = {}

	# Iterating through data, checking if x-value already exists. Then either update or create new
	for x, y, err in zip(xData, yData, yErr):
		point = dataPoints.get(x)
		if not point:
			point = DataPoint()
			point.x = x
			point.ySum = y
			point.errSum = err
			dataPoints.update({x : point})
		else:
			dataPoints[x].ySum += y
			dataPoints[x].errSum += err
			dataPoints[x].amount += 1


	xNew = []
	yNew = []
	errNew = []

	for dp in dataPoints.values():
		xNew.append(dp.x)
		yNew.append(dp.ySum / dp.amount)
		errNew.append(dp.errSum / dp.amount)

	return xNew, yNew, errNew


def plotData(groups, plotFolder):
	# Plotting data as a bar plot with error 
	sns.set()
	for i, (benchmarkName, gbf) in enumerate(groups):

		xDataFull, yDataFull, yErrFull = removeDuplicates(gbf["params"], gbf["items_per_second"], gbf["stddev"])

		# Creating data chunks so that later each chunk can be plotted in a separate subfigure
		xDataChunks = [xDataFull[x:x+CHUNK_SIZE] for x in range(0, len(xDataFull), CHUNK_SIZE)]
		yDataChunks = [yDataFull[x:x+CHUNK_SIZE] for x in range(0, len(yDataFull), CHUNK_SIZE)]
		yErrChunks = [yErrFull[x:x+CHUNK_SIZE] for x in range(0, len(yErrFull), CHUNK_SIZE)]

		fig = plt.figure(figsize=(18,max(5, 3*len(xDataChunks))))
		paramLen = 0
		for i, (xData, yData, yErr) in enumerate(zip(xDataChunks, yDataChunks, yErrChunks)):
			ax = fig.add_subplot(len(xDataChunks), 1, i+1)
			rects = ax.bar(xData, yData, yerr=yErr, width=0.25)

			print(benchmarkName, i, list(xData))
			print(list(yData))
			print()


			for rect in rects:
				height = rect.get_height()
				ax.text(rect.get_x() + rect.get_width()/2., 1.02*height, millify(height), ha='center', va='bottom')
				
			# Getting the max number of Params, needed to portray correct xlabel
			paramLen = max(paramLen, list(xData)[0].count("/"))

		fig.suptitle(benchmarkName.replace("_", " ")[3:])
		plt.xlabel(" / ".join(BENCHMARK_PARAMS[:paramLen+1]))

		plt.tight_layout()
		plt.savefig(f"{plotFolder}/{benchmarkName}.pdf")
		plt.clf()



if __name__ == '__main__':
	options = parseArguments()

	if not fileFolderExists(options.benchmark):
		print(f"{options.benchmark} does not exist!")
		exit(1)

	today = datetime.now()
	benchmarkName = str(options.benchmark).split("/")[-1]
	plotFolder = os.path.join(options.plotFolder, today.strftime('%Y%m%d_%H%M%S'))
	logFile = os.path.join(plotFolder, f"{benchmarkName}_log.json")

	createFolder(plotFolder)
	runBenchmark(options.benchmark, logFile)
	benchmarkDataFrame = loadDataIntoDataFrame(logFile)
	benchmarkGroups = createPlotableGroups(benchmarkDataFrame)

	plotData(benchmarkGroups, plotFolder)