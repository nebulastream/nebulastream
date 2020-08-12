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


BENCHMARK_PARAMS = ["Updates", "Keys", "Type of Distribution", "Threads"]

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
			params = "/".join(["{:.2e}".format(int(item)) for item in params.group(1).split("/")])
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


def plotData(groups, plotFolder):
	# Plotting data as a bar plot with error 
	sns.set()
	for i, (benchmarkName, gbf) in enumerate(groups):
		plt.figure()
		gbf.plot(x="params", y="items_per_second", kind="bar", yerr="stddev", 
				 label="$\\frac{Keys}{s}$", figsize=(8,5))
		
		title = benchmarkName
		plt.title(title.replace("_", " ")[3:])
		plt.xlabel("/".join(BENCHMARK_PARAMS[:int(str(gbf["params"]).count("/")/len(gbf["params"]))+1]))
		plt.xticks(rotation=45)
		plt.tight_layout()

		plt.savefig(f"{plotFolder}/{title}.pdf")
		plt.clf()



if __name__ == '__main__':
	options = parseArguments()

	if not fileFolderExists(options.benchmark):
		print(f"{options.benchmark} does not exist!")
		exit(1)

	today = datetime.now()
	benchmarkName = str(options.benchmark).split("/")[-1]
	plotFolder = os.path.join(options.plotFolder, today.strftime('%Y%m%d_%H%M%S'))
	#plotFolder = os.path.join(options.plotFolder, "20200812_100717")
	logFile = os.path.join(plotFolder, f"{benchmarkName}_log.log")

	createFolder(plotFolder)
	runBenchmark(options.benchmark, logFile)
	benchmarkDataFrame = loadDataIntoDataFrame(logFile)
	benchmarkGroups = createPlotableGroups(benchmarkDataFrame)

	plotData(benchmarkGroups, plotFolder)