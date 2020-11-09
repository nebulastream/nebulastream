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
#resultCsvFile = "/media/sf_NES/nebulastream/benchmark/scripts/20201030_093604/MapQueries_30-10-2020_09-36-14_v0.0.161/BM_SimpleMapQuery_results.csv"
folder = os.path.dirname(resultCsvFile)
mapQueryFileDF = pd.read_csv(resultCsvFile)

# Adding bytesPerSecond and tuplesPerSecond columns
mapQueryFileDF["TuplesPerSecond"] = mapQueryFileDF["ProcessedTuples"] / mapQueryFileDF["PeriodLength"]
mapQueryFileDF["BytesPerSecond"] = mapQueryFileDF["ProcessedBytes"] / mapQueryFileDF["PeriodLength"]


# Calculate avg throughput for one ingestionrate
mapQueryFileDF.drop(mapQueryFileDF.loc[mapQueryFileDF['Ingestionrate'] < 1e6].index, inplace=True)
groups = mapQueryFileDF.groupby(by="Ingestionrate", sort=True)
xData = []
yData = []
yErr = []

for i, (ingestionRate, gbf) in enumerate(groups):
	print("Ingestionrate {:e} has throughput of {:e} tup/s".format(float(ingestionRate), gbf["TuplesPerSecond"].mean()))
	xData.append(ingestionRate)
	yData.append(gbf["TuplesPerSecond"].mean())
	yErr.append(gbf["TuplesPerSecond"].std())
	

highestTuplesPerSecondIngestrate = groups["TuplesPerSecond"].mean().keys().to_list()[groups["TuplesPerSecond"].mean().argmax()]

print2Log("Highest avg throughput of {:e} tup/s was achieved with ingestionrate of {:e}".format(groups["TuplesPerSecond"].mean().max(), highestTuplesPerSecondIngestrate), __file__)

# Get maximum throughput
overallHighestThroughput = mapQueryFileDF["TuplesPerSecond"].max()
overallHighestThroughputRow = str(mapQueryFileDF.iloc[mapQueryFileDF["TuplesPerSecond"].argmax()].drop("BM_Name").to_dict())
print2Log("Overall highest throughput of {:e} tup/s was achieved with {}".format(overallHighestThroughput, overallHighestThroughputRow), __file__)


plt.figure(figsize=(8, 16))
plt.barh(np.arange(0, len(xData)), yData, xerr=yErr)
plt.yticks(np.arange(0, len(xData)), [f"{millify(x)}\n{millify(y)}" for x,y in zip(xData, yData)])
plt.savefig(os.path.join(folder, "avg_througput_over_same_ingestrate_map_query.png"))


plt.figure(figsize=(8, 16))
plt.barh(np.arange(0, len(mapQueryFileDF)), mapQueryFileDF["TuplesPerSecond"])
plt.yticks(np.arange(0, len(mapQueryFileDF)), [millify(x) for x in mapQueryFileDF["TuplesPerSecond"]])
plt.savefig(os.path.join(folder, "throughput_overall_map_query.png"))
