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


# Calculate avg throughput for one ingestionrate
mapQueryFileDF.drop(mapQueryFileDF.loc[mapQueryFileDF['Ingestionrate'] < 1e6].index, inplace=True)
groups = mapQueryFileDF.groupby(by="Ingestionrate", sort=True)
xData = []
yData = []
yErr = []

for i, (ingestionRate, gbf) in enumerate(groups):
	print("Ingestionrate {:e} has throughput of {:e} tup/s".format(float(ingestionRate), gbf["ProcessedTuples"].mean()))
	xData.append(ingestionRate)
	yData.append(gbf["ProcessedTuples"].mean())
	yErr.append(gbf["ProcessedTuples"].std())
	

highestTuplesPerSecondIngestrate = groups["ProcessedTuples"].mean().keys().to_list()[groups["ProcessedTuples"].mean().argmax()]

print2Log("Highest avg throughput of {:e} tup/s was achieved with ingestionrate of {:e}".format(groups["ProcessedTuples"].mean().max(), highestTuplesPerSecondIngestrate), __file__)

# Get maximum throughput
overallHighestThroughput = mapQueryFileDF["ProcessedTuples"].max()
overallHighestThroughputRow = str(mapQueryFileDF.iloc[mapQueryFileDF["ProcessedTuples"].argmax()].drop("BM_Name").to_dict())
print2Log("Overall highest throughput of {:e} tup/s was achieved with {}".format(overallHighestThroughput, overallHighestThroughputRow), __file__)


plt.figure(figsize=(8, 16))
plt.barh(np.arange(0, len(xData)), yData, xerr=yErr)
plt.yticks(np.arange(0, len(xData)), [f"{millify(x)}\n{millify(y)}" for x,y in zip(xData, yData)])
plt.savefig(os.path.join(folder, "avg_througput_over_same_ingestrate_map_query.pdf"))


plt.figure(figsize=(8, 16))
plt.barh(np.arange(0, len(mapQueryFileDF)), mapQueryFileDF["ProcessedTuples"])
plt.yticks(np.arange(0, len(mapQueryFileDF)), [millify(x) for x in mapQueryFileDF["ProcessedTuples"]])
plt.savefig(os.path.join(folder, "throughput_overall_map_query.pdf"))