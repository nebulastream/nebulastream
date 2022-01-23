#!/usr/bin/env python3


from argparse import ArgumentParser

import pandas as pd
import seaborn as sns
import datetime


parser = ArgumentParser()
parser.add_argument("-d", "--delimiter", action="store", dest="delimiter",
                    help="Set delimiter used in CSV file.", default="\t")
parser.add_argument("dataFile",
                    help="CSV file that will be used as data for plotting")
parser.add_argument("-o", "--output", dest="outputFile",
                    default="impro_benchmark_"+datetime.datetime.now().astimezone().replace(microsecond=0).isoformat()+".png",
                    help="File to be save the resulting plot image to.")
args = parser.parse_args()

# Load data into data frame
fileDataFrame = pd.read_csv(args.dataFile, delimiter=args.delimiter)

# Configure seaborn
# - Apply the default theme
sns.set_theme()

# filter failed tests
success = fileDataFrame[fileDataFrame["query"] != "join"]
success = success[success["query"] != "tumblingWindowEventTime"]

overall = success[success["timed_unit"] == "overall runtime"]

headers_overall = overall[overall["variant"] == "header selection"]
headers_filter = headers_overall[headers_overall["query"] == "filterQuery"]

# plotting
g = sns.boxplot(x="query", hue="variant", y="time", data=overall, dodge=True)
g.figure.set_size_inches(20, 8.27)

g.figure.savefig(args.outputFile)  # dpi=1080

