#!/usr/bin/env python

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/env python
# coding: utf-8
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np
import os
import math


# This python script plots the throughput in bytes over different buffer sizes. 
# For each number of workerthreads a own image is created


class CustomSimpleDataPoint(object):
	"""docstring for CustomSimpleDataPoint"""
	def __init__(self, ingestionRate, workerThreads, tupleSize, yValue, yErr):
		self.ingestionRate = ingestionRate
		self.workerThreads = workerThreads
		self.yErr = yErr
		self.yValue = yValue
		self.tupleSize = tupleSize

# Load data into data frame 
folder = os.path.dirname(resultCsvFile)
fileDataFrame = pd.read_csv(resultCsvFile)

# Create new folder for all plots
fileName = os.path.splitext(os.path.basename(resultCsvFile))[0]
createFolder(os.path.join(folder, fileName))

# Delete all rows that contain header
headerAsList = list(fileDataFrame)
duplicateHeaderIndexes = []
for i, row in fileDataFrame.iterrows():
	if list(row) == headerAsList:
		duplicateHeaderIndexes.append(i)

fileDataFrame = fileDataFrame.drop(index=duplicateHeaderIndexes)
fileDataFrame.to_csv(resultCsvFile, index=False)
fileDataFrame = pd.read_csv(resultCsvFile)

# Adding bytesPerSecond and tuplesPerSecond columns
fileDataFrame["TuplesPerSecond"] = fileDataFrame["ProcessedTuples"] / fileDataFrame["PeriodLength"]
fileDataFrame["BytesPerSecond"] = fileDataFrame["ProcessedBytes"] / fileDataFrame["PeriodLength"]
fileDataFrame["TuplesPerBuffer"] = fileDataFrame["BufferSize"] / fileDataFrame["SchemaSize"]

fileDataFrame.to_csv(resultCsvFile, index=False)
fileDataFrame = pd.read_csv(resultCsvFile)

# Calculate avg throughput for one ingestionrate
groups = fileDataFrame.groupby(by=["Ingestionrate", "WorkerThreads", "SchemaSize"], sort=True)
allDataPoints = []

# Converting from dataframe groups to CustomSimpleDataPoint as this helps plotting the data
for i, ((ingestionRate, workerThreads, tupleSize), gbf) in enumerate(groups):
	print("Ingestionrate {:e} has throughput of {:e} B/s with {} workerThreads".format(float(ingestionRate), gbf["BytesPerSecond"].mean(), workerThreads))
	dataPoint = CustomSimpleDataPoint(ingestionRate, workerThreads, tupleSize, gbf["BytesPerSecond"].mean(), gbf["BytesPerSecond"].std())
	allDataPoints.append(dataPoint)


# Gets highest throughput of each group
highestBytesPerSecondIngestrate = groups["BytesPerSecond"].mean().keys().to_list()[groups["BytesPerSecond"].mean().argmax()]
highestAvgThrougput = groups["BytesPerSecond"].mean().max()
avgHighestThroughputStr = f"Highest avg throughput of {highestAvgThrougput:e} B/s was achieved with (ingestionrate,workerThreads) of {highestBytesPerSecondIngestrate}"
print2Log(avgHighestThroughputStr)

# Get maximum throughput over all datapoints
overallHighestThroughput = fileDataFrame["BytesPerSecond"].max()
overallHighestThroughputRow = str(fileDataFrame.iloc[fileDataFrame["BytesPerSecond"].argmax()].drop("BM_Name").to_dict())
overallHighestThroughputStr = "Overall highest throughput of {:e} B/s was achieved with {}".format(overallHighestThroughput, overallHighestThroughputRow)
print2Log(overallHighestThroughputStr)

allWorkerThreads 	= set([dataPoint.workerThreads for dataPoint in allDataPoints])
allTupleSizes		= { workerThreads : [] for workerThreads in allWorkerThreads }
allIngestionRate 	= { workerThreads : [] for workerThreads in allWorkerThreads }
allyErr 			= { workerThreads : [] for workerThreads in allWorkerThreads }
allyValues			= { workerThreads : [] for workerThreads in allWorkerThreads }

for workerThreads in allWorkerThreads:
	for dataPoint in allDataPoints:
		if dataPoint.workerThreads == workerThreads:
			allIngestionRate[workerThreads].append(dataPoint.ingestionRate)
			allyErr[workerThreads].append(dataPoint.yErr)
			allyValues[workerThreads].append(dataPoint.yValue)
			allTupleSizes[workerThreads].append(dataPoint.tupleSize)

for workerThreads in allWorkerThreads:
	fig, ax = plt.subplots(figsize=(24, 12))
	rects = ax.bar(np.arange(0, len(allIngestionRate[workerThreads])), allyValues[workerThreads], yerr=allyErr[workerThreads], width=0.35)
	ax.set_xticks(np.arange(0, len(allIngestionRate[workerThreads])))
	ax.set_xticklabels([f"{millify(x)} / {millify(y)}" for x,y in zip(allIngestionRate[workerThreads], allTupleSizes[workerThreads])], rotation=45, ha="right")
	ax.set_xlabel("Ingestionrate / TupleSize [B]")
	ax.set_ylabel("Throughput [B/s]")
	autolabel(rects, ax)

	plt.title(f"WorkerThreads: {workerThreads}")
	plt.savefig(os.path.join(folder, os.path.join(fileName, f"avg_througput_tuple_size_query_{workerThreads}_{fileName}.png")))
	plt.close(fig)
