#!/usr/bin/env python
# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from argparse import ArgumentParser
from argparse import ArgumentTypeError
from datetime import datetime

import os
import stat
import subprocess
import re

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np
import math

LOG_FILE = "run_benchmarks_general.log"


class Benchmark(object):
	"""docstring for Benchmark"""
	def __init__(self, name, executable, customPlottingFile):
		self.name = name
		self.executable = executable
		self.customPlottingFile = customPlottingFile
		self.resultCsvFiles = set([])

	def __str__(self):
		return self.getInfo()

	def __repr__(self):
		return self.getInfo()

	def getInfo(self):
		return f"<{self.name}>\n\t<{self.executable}>\n\t<{self.customPlottingFile}>\n\t<{self.resultCsvFiles}>"
		

class SimpleDataPoint(object):
	"""docstring for SimpleDataPoint"""
	def __init__(self, ingestionRate, workerThreads, yValue, yErr):
		self.ingestionRate = ingestionRate
		self.workerThreads = workerThreads
		self.yErr = yErr
		self.yValue = yValue

#Params:
#	<benchmark names> (names of executables that will be run, if this is empty all possible benchmark executables will be run)
#	<executables folder> (folder in which the executables reside)

########################Start of helper functions########################
class bcolors:
	HEADER = '\033[95m'
	OKBLUE = '\033[94m'
	OKCYAN = '\033[96m'
	OKGREEN = '\033[92m'
	OKMAGENTA = '\033[35m'
	WARNING = '\033[93m'
	FAIL = '\033[91m'
	ENDC = '\033[0m'
	BOLD = '\033[1m'
	UNDERLINE = '\033[4m'

def printWithColor(message, color):
	print(f"{color}{message}{bcolors.ENDC}")

def printSuccess(message):
	printWithColor(message, bcolors.OKGREEN)

def printFail(message):
	printWithColor(message, bcolors.FAIL)

def printHighlight(message):
	printWithColor(message, bcolors.OKCYAN)

def print2Log(message, file=__file__):
	with open(LOG_FILE, 'a+') as f:
		f.write(f"\n----------------------{file}----------------------\n")
		f.write(f"{message}")
		f.write(f"\n----------------------{file}----------------------\n")


def parseArguments():
	parser = ArgumentParser()
	parser.add_argument("-b", "--benchmarks", nargs="+", action="store", dest="benchmarkNames", help="Name of benchmark executables that should be run, if none then all benchmark executables are used. Regex is supported")
	parser.add_argument("-f", "--folder", action="store", dest="benchmarkFolder", help="Folder in which all benchmark executables lie", required=True)
	parser.add_argument("-nc", "--no-confirmation", action="store_false", dest="noConfirmation", help="If this flag is set then the script will ask if it should run the found benchmarks")
	parser.add_argument("-m", "--message", action="store", dest="runMessage", help="If a message is present, then this will be written into the log file. This may help if one executes different benchmarks")

	args = parser.parse_args()
	return (args)

def fileFolderExists(file):
	return os.path.exists(file)

def createFolder(folder):
	if fileFolderExists(folder):
		print(f"{folder} already exists")
	else:
		os.mkdir(folder)
		print(f"{folder} was created!")

def confirmInput(message):
	yes = {'yes','y', 'ye', ''}
	no = {'no','n'}
	while True:
		answer = input(message).lower()
		if answer in yes:
			return True
		elif answer in no:
			return False
		else:
			print(f"\nConfirmation: {yes}, Refusing: {no}")

def getAllFolders(directory):
	return [file for file in os.listdir(directory) if os.path.isdir(os.path.join(directory, file))]

def getAllFiles(directory):
	return [file for file in os.listdir(directory) if os.path.isfile(os.path.join(directory, file))]


def changeWorkingDirectory(newDirectory):
	print(f"Changing directory into {os.path.abspath(newDirectory)}...")
	os.chdir(newDirectory)

def millify(n):
	PRE_SI_UNIT = ["", "k", "M", "B"]
	n = float(n)
	idx = max(0,min(len(PRE_SI_UNIT)-1, int(math.floor(0 if n == 0 else math.log10(abs(n))/3))))

	return '{:.1f}{}'.format(n / 10**(3 * idx), PRE_SI_UNIT[idx])

def autolabel(rects, ax):
	"""Attach a text label above each bar in *rects*, displaying its height."""
	for rect in rects:
		height = rect.get_height()
		ax.annotate('{}'.format(millify(height)),
					xy=(rect.get_x() + rect.get_width() / 2, height),
					xytext=(0, 3),  # 3 points vertical offset
					textcoords="offset points",
					ha='center', va='bottom')


def customExecPythonFile(file, globals=None, locals=None):
	if globals is None:
		globals = {}
	if locals is None:
		locals = {}

	globals.update({"__file__": file, "__name__": "__main__", "print2Log" : print2Log})
	with open(file, 'rb') as f:
		exec(compile(f.read(), file, 'exec'), globals, locals)


########################End of helper functions##########################



# returns all benchmark (as a dict()) that should be executed and can be executed
# the executable has to have "benchmark" in its name
# @param fileDirectory: directory in which this file lies
def findAllValidBenchmarks(benchmarkFolder, validBenchmarkNames, fileDirectory):
	validBenchmarksDict = {}
	for fileName in os.listdir(benchmarkFolder):

		fullFileNamePath = os.path.join(benchmarkFolder, fileName)
		if "benchmark" in fileName and os.path.isfile(fullFileNamePath) and (stat.S_IXUSR & os.stat(fullFileNamePath)[stat.ST_MODE]):
			benchmark = Benchmark(fileName, fullFileNamePath, customPlottingFile(fileName, fileDirectory))
			validBenchmarksDict.update({benchmark.name : benchmark})

	return {benchmarkName : benchmark 	for benchmarkName,benchmark in validBenchmarksDict.items() 
											for regex in validBenchmarkNames 
												if re.match(regex, benchmarkName)}

# returns custom plotting file (<benchmark executable name>.py) or None
def customPlottingFile(fileName, fileDirectory):
	return os.path.join(fileDirectory, f"{fileName}.py") if f"{fileName}.py" in os.listdir(fileDirectory) else None



# prints all benchmarks to stdout, expects allBenchmarks as a dict()
def printAllBenchmarks(allBenchmarks):
	for benchmark in allBenchmarks.values():
		print(benchmark.getInfo())


def runAllBenchmarks(allBenchmarks, benchmarkFolder):
	cntErr = 0
	for benchmarkName in allBenchmarks:
		startTimeStamp = datetime.now()
		benchmark = allBenchmarks[benchmarkName]
		beforeFolders = getAllFolders(os.getcwd())
		
		print(f"\nRunning benchmark {benchmark.name}...")
		
		cmdString = f"{benchmark.executable}"
		try:
			with open("/dev/null") as f:
				subprocess.check_call(cmdString.split(), stdout=f)
		except Exception as e:
			cntErr += 1
			printFail(e)
			printFail(f"Could not run {benchmark.name}!!!\n")
			continue

		delta = datetime.now() - startTimeStamp
		hours = int(delta.total_seconds() // 3600)
		minutes = int(delta.total_seconds() % 3600) // 60
		printSuccess(f"Done running benchmark {benchmark.name} in {hours}h{minutes}m!\n")

		newFolders = [f for f in getAllFolders(os.getcwd()) if not f in beforeFolders and os.path.isdir(f)]

		for folder in newFolders:
			fullPath = os.path.join(os.getcwd(), folder)
			for csvFile in [os.path.join(fullPath, file) for file in getAllFiles(fullPath) if str(os.path.join(fullPath, file)).endswith(".csv")]:
				benchmark.resultCsvFiles.add(csvFile)

	return cntErr

def plotDataAllBenchmarks(allBenchmarks):
	cntErr = 0
	for benchmark in allBenchmarks.values():
		print(f"\nPlotting {benchmark.name}...")

		if benchmark.customPlottingFile:
			try:
				for csvFile in benchmark.resultCsvFiles:
					customExecPythonFile(benchmark.customPlottingFile, locals={"resultCsvFile" : csvFile}, 
										globals={	"millify" : millify, 
													"SimpleDataPoint" : SimpleDataPoint, 
													"built" : __builtins__,
													"printHighlight" : printHighlight,
													"autolabel" : autolabel})
			except Exception as e:
				cntErr += 1
				printFail(f"Could not plot {benchmark.name} with custom plottingFile!!!")
				printFail(e)
				continue

		else:
			try:
				defaultPlotBenchmark(benchmark)
			except Exception as e:
				cntErr += 1
				printFail(f"Could not plot {benchmark.name} with default plotting!!!")
				printFail(e)
				raise e
				continue

		printSuccess(f"Done plotting {benchmark.name}!")

	return cntErr

def defaultPlotBenchmark(benchmark):
	print(f"Default plotting {benchmark}...")
	for counterCsvFile, csvFile in enumerate(benchmark.resultCsvFiles):

		# Load data into data frame 
		folder = os.path.dirname(csvFile)
		fileDataFrame = pd.read_csv(csvFile)

		# Delete all rows that contain header
		headerAsList = list(fileDataFrame)
		duplicateHeaderIndexes = []
		for i, row in fileDataFrame.iterrows():
			if list(row) == headerAsList:
				duplicateHeaderIndexes.append(i)

		fileDataFrame = fileDataFrame.drop(index=duplicateHeaderIndexes)
		fileDataFrame.to_csv(csvFile, index=False)
		fileDataFrame = pd.read_csv(csvFile)
		
		# Adding bytesPerSecond and tuplesPerSecond columns
		fileDataFrame["TuplesPerSecond"] = fileDataFrame["ProcessedTuples"] / fileDataFrame["PeriodLength"]
		fileDataFrame["BytesPerSecond"] = fileDataFrame["ProcessedBytes"] / fileDataFrame["PeriodLength"]
		
		fileDataFrame.to_csv(csvFile, index=False)
		fileDataFrame = pd.read_csv(csvFile)

		# Calculate avg throughput for one ingestionrate
		groups = fileDataFrame.groupby(by=["Ingestionrate", "WorkerThreads"], sort=True)
		allDataPoints = []

		for i, ((ingestionRate, workerThreads), gbf) in enumerate(groups):
			print("Ingestionrate {:e} has throughput of {:e} tup/s with {} workerThreads".format(float(ingestionRate), gbf["TuplesPerSecond"].mean(), workerThreads))
			dataPoint = SimpleDataPoint(ingestionRate, workerThreads, gbf["TuplesPerSecond"].mean(), gbf["TuplesPerSecond"].std())
			allDataPoints.append(dataPoint)

		
		highestTuplesPerSecondIngestrate = groups["TuplesPerSecond"].mean().keys().to_list()[groups["TuplesPerSecond"].mean().argmax()]
		highestAvgThrougput = groups["TuplesPerSecond"].mean().max()
		avgHighestThroughputStr = f"Highest avg throughput of {highestAvgThrougput:e} tup/s was achieved with (ingestionrate,workerThreads) of {highestTuplesPerSecondIngestrate}"
		
		printHighlight(avgHighestThroughputStr)
		print2Log(avgHighestThroughputStr)

		# Get maximum throughput
		overallHighestThroughput = fileDataFrame["TuplesPerSecond"].max()
		overallHighestThroughputRow = str(fileDataFrame.iloc[fileDataFrame["TuplesPerSecond"].argmax()].drop("BM_Name").to_dict())
		overallHighestThroughputStr = "Overall highest throughput of {:e} tup/s was achieved with {}".format(overallHighestThroughput, overallHighestThroughputRow)
		printHighlight(overallHighestThroughputStr)
		print2Log(overallHighestThroughputStr)

		allWorkerThreads 	= set([dataPoint.workerThreads for dataPoint in allDataPoints])
		allIngestionRate 	= { workerThreads : [] for workerThreads in allWorkerThreads }
		allyErr 			= { workerThreads : [] for workerThreads in allWorkerThreads }
		allyValues			= { workerThreads : [] for workerThreads in allWorkerThreads }

		for workerThreads in allWorkerThreads:
			for dataPoint in allDataPoints:
				if dataPoint.workerThreads == workerThreads:
					allIngestionRate[workerThreads].append(dataPoint.ingestionRate)
					allyErr[workerThreads].append(dataPoint.yErr)
					allyValues[workerThreads].append(dataPoint.yValue)

		for workerThreads in allWorkerThreads:
			fig, ax = plt.subplots(figsize=(24, 8))
			rects = ax.bar(np.arange(0, len(allIngestionRate[workerThreads])), allyValues[workerThreads], yerr=allyErr[workerThreads], width=0.35)
			ax.set_xticks(np.arange(0, len(allIngestionRate[workerThreads])))
			ax.set_xticklabels([f"{millify(x)}" for x in allIngestionRate[workerThreads]])
			ax.set_xlabel("Ingestionrate")
			ax.set_ylabel("Throughput [tup/s]")
			autolabel(rects, ax)

			plt.title(f"WorkerThreads: {workerThreads}")
			plt.savefig(os.path.join(folder, f"avg_througput_{benchmark.name}_{workerThreads}.png"))


if __name__ == '__main__':

	options = parseArguments()

	if not fileFolderExists(options.benchmarkFolder):
		print(f"{options.benchmarkFolder} does not exist!")
		exit(1)

	fileDirectory = os.path.dirname(os.path.realpath(__file__))
	oldCWD = os.getcwd()
	todayString = datetime.now().strftime('%Y%m%d_%H%M%S')
	LOG_FILE = os.path.abspath(os.path.join(todayString, LOG_FILE))
	createFolder(todayString)
	
	changeWorkingDirectory(todayString)

	benchmarkFolder = os.path.join(oldCWD, options.benchmarkFolder)
	validBenchmarkNames = options.benchmarkNames if options.benchmarkNames else "."
	validBenchmarks = findAllValidBenchmarks(benchmarkFolder, validBenchmarkNames, fileDirectory)
	
	printAllBenchmarks(validBenchmarks)
	if not validBenchmarks:
		print("Could not find any benchmarks. Please adjust -b param!")
		exit(1)

	if options.noConfirmation:
		confirmation = confirmInput("Do you want to execute above benchmarks? [Y/n]: ")
		if not confirmation:
			print("Exiting script now...")
			exit(1)


	if options.runMessage:
		print2Log(options.runMessage)
	
	startTimeStamp = datetime.now()
	errRunBenchmarks = runAllBenchmarks(validBenchmarks, benchmarkFolder)
	
	changeWorkingDirectory(oldCWD)

	errPlotting = plotDataAllBenchmarks(validBenchmarks)

	endTimeStamp = datetime.now()

	print("\n\n---------------------------------------------------")
	message = f"Running benchmark errors = {errRunBenchmarks}"
	if errRunBenchmarks == 0:
		printSuccess(message)
	else:
		printFail(message)

	message = f"Plotting errors = {errPlotting}"
	if errPlotting == 0:
		printSuccess(message)
	else:
		printFail(message)

	print("---------------------------------------------------")

	delta = endTimeStamp - startTimeStamp
	
	hours = int(delta.total_seconds() // 3600)
	minutes = int(delta.total_seconds() % 3600) // 60
	message = f"Running benchmarks and plotting took: {hours}h{minutes}m"
	printHighlight(message)
	print2Log(message)
