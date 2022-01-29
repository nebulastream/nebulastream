#!/usr/bin/env python3
from argparse import ArgumentParser
from errno import EEXIST

import pandas as pd
import seaborn as sns
import datetime
import os

PNG = True
PDF = False
SVG = False


def plotter(meta):
    plot_overall_boxplot(meta, "all_overall_boxplot")
    plot_overall_barchart(meta, "all_overall_barchart")


def plot_overall_barchart(meta, name="all_overall_barchart"):
    overall = meta["data_overall"]
    g = sns.barplot(x="query", hue="variant", y="time", data=overall, dodge=True)

    save(meta, g, name)


def plot_overall_boxplot(meta, name="all_overall_boxplot"):
    overall = meta["data_overall"]

    # plotting
    g = sns.boxplot(x="query", hue="variant", y="time", data=overall, dodge=True)
    g.figure.set_size_inches(20, 8.27)

    save(meta, g, name)


def plot_stages(meta, name="stages"):
    dataAll = meta["data_all"]
    dataStages = dataAll[dataAll["timed_unit"] != "overall runtime"]
    # plot all stages together (stacked bar plot ?)
    for query in dataStages["query"].unique():
        stagesOfQuery = dataStages[dataStages["query"] == query]
        g = sns.barplot(x="timed_unit", y="time", hue="variant", data=stagesOfQuery, dodge=True)
        save(meta, g, f'{query}')
        #for stage in stagesOfQuery["timed_unit"].unique():
        #    dataStage = stagesOfQuery[stagesOfQuery["timed_unit"] == stage]
        #    g = sns.barplot(x="variant", y="time", data=dataStage, dodge=True)
        #    save(meta, g, f'{query}_{stage}')
        #g = sns.boxplot(x="query", hue="variant", y="time", data=overall, dodge=True)
        #g = stagesOfQuery.plot(title=query, x="variant", y="time", kind='bar', stacked=True)
        #save(meta, g, query)


def plot_pch(meta):
    dataAll = meta["data_overall"]
    for query in dataAll["query"].unique():
        dataQuery = dataAll[dataAll["query"] == query]
        g = sns.barplot(x="variant", y="time", data=dataQuery, dodge=True)
        save(meta, g, f'{query}')


def plot_filter(meta):
    data = meta["no_headers_representative"]
    g = sns.barplot(x="variant", y="time", data=data, dodge=True)
    save(meta, g, "representative_no-headers")


def plot_window(meta):
    data = meta["window_representative"]
    g = sns.barplot(x="variant", y="time", data=data, dodge=True)
    save(meta, g, "representative_window")


def calc(meta):
    dataWindow = meta["window_representative"]
    dataFilter = meta["no_headers_representative"]

    str_no_optimization = "No optimization"  # "all headers"
    str_header_selection = "Header selection"  # "header selection"
    str_pch = "Precompiled header"

    avgWindowAll = dataWindow[dataWindow["variant"] == str_no_optimization]["time"].agg('average')
    avgWindowSelection = dataWindow[dataWindow["variant"] == str_header_selection]["time"].agg('average')
    avgWindowPch = dataWindow[dataWindow["variant"] == str_pch]["time"].agg('average')

    absImprovementWindowSelection = avgWindowAll - avgWindowSelection
    relImprovementWindowSelection = 100 / avgWindowAll * absImprovementWindowSelection

    absImprovementWindowPch = avgWindowAll - avgWindowPch
    relImprovementWindowPch = 100 / avgWindowAll * absImprovementWindowPch

    print("Window - header selection absolute improvement", absImprovementWindowSelection, "ms")
    print("Window - header selection relative improvement", relImprovementWindowSelection, "%")
    print("---")
    print("Window - precompiled header absolute improvement", absImprovementWindowPch, "ms")
    print("Window - precompiled header relative improvement", relImprovementWindowPch, "%")
    print("--- ---")

    avgFilterAll = dataFilter[dataFilter["variant"] == str_no_optimization]["time"].agg('average')
    avgFilterSelection = dataFilter[dataFilter["variant"] == str_header_selection]["time"].agg('average')
    avgFilterPch = dataFilter[dataFilter["variant"] == str_pch]["time"].agg('average')

    absImprovementFilterSelection = avgFilterAll - avgFilterSelection
    relImprovementFilterSelection = 100 / avgFilterAll * absImprovementFilterSelection

    absImprovementFilterPch = avgFilterAll - avgFilterPch
    relImprovementFilterPch = 100 / avgFilterAll * absImprovementFilterPch

    print("Filter - header selection absolute improvement", absImprovementFilterSelection, "ms")
    print("Filter - header selection relative improvement", relImprovementFilterSelection, "%")
    print("---")
    print("Filter - precompiled header absolute improvement", absImprovementFilterPch, "ms")
    print("Filter - precompiled header relative improvement", relImprovementFilterPch, "%")
    print("--- ---")


def prep(args):
    # Load data into data frame
    fileDataFrame = pd.read_csv(args.dataFile, delimiter=args.delimiter).sort_values('time', ascending=False)

    # Configure seaborn
    # - Apply the default theme
    sns.set_theme()

    # filter failed tests
    success = fileDataFrame[fileDataFrame["query"] != "join"]
    success = success[success["query"] != "tumblingWindowEventTime"]

    overall = success[success["timed_unit"] == "overall runtime"]

    overallAll = fileDataFrame[fileDataFrame["timed_unit"] == "overall runtime"]

    no_headers_representative = overall[overall["query"] == "filter"]
    window_representative = overall[overall["query"] == "window"]

    # headers_overall = overall[overall["variant"] == "header selection"]
    # headers_filter = headers_overall[headers_overall["query"] == "filterQuery"]

    meta = {
        "data_all": fileDataFrame,
        "data_overall_all": overallAll,
        "data_overall": overall,
        "no_headers_representative": no_headers_representative,
        "window_representative": window_representative,
        "output_dir": args.outputDirectory
    }
    return meta


def save(meta, g, name):
    filename = meta["output_dir"] + "/" + name + "_"  + datetime.datetime.now().astimezone().replace(microsecond=0).isoformat()
    if not os.path.exists(meta["output_dir"]):
        try:
            os.makedirs(meta["output_dir"])
        except OSError as exception:
            if exception.errno != EEXIST:
                raise

    if PDF:
        filename = filename + ".pdf"
    elif PNG:
        filename = filename + ".png"
    elif SVG:
        filename = filename + ".svg"

    g.figure.savefig(filename)
    g.clear()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-d", "--delimiter", action="store", dest="delimiter",
                        help="Set delimiter used in CSV file.", default="\t")
    parser.add_argument("dataFile",
                        help="CSV file that will be used as data for plotting")
    parser.add_argument("-o", "--output", dest="outputDirectory",
                        default="benchmark/plots/",
                        help="Directory to save the resulting plot images to.")
    parser.add_argument("funs", default=[plotter], nargs='*')

    args = parser.parse_args()
    meta = prep(args)
    for funName in args.funs:
        fun = locals().get(funName)
        if fun is not None:
            fun(meta)
