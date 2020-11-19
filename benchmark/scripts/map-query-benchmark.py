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

#!/usr/bin/env python
# coding: utf-8
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np
import os
import math


# Load data into data frame 
folder = os.path.dirname(resultCsvFile)
fileDataFrame = pd.read_csv(resultCsvFile)

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

fileDataFrame.to_csv(resultCsvFile, index=False)
fileDataFrame = pd.read_csv(resultCsvFile)

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
print2Log(avgHighestThroughputStr)

# Get maximum throughput
overallHighestThroughput = fileDataFrame["TuplesPerSecond"].max()
overallHighestThroughputRow = str(fileDataFrame.iloc[fileDataFrame["TuplesPerSecond"].argmax()].drop("BM_Name").to_dict())
overallHighestThroughputStr = "Overall highest throughput of {:e} tup/s was achieved with {}".format(overallHighestThroughput, overallHighestThroughputRow)
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
	plt.savefig(os.path.join(folder, f"avg_througput_map_query_{workerThreads}.png"))
