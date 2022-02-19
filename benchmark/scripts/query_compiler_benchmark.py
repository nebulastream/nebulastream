#!/usr/bin/env python3
from argparse import ArgumentParser
from errno import EEXIST

import pandas as pd
import seaborn as sns
import datetime
import os

PNG = False
PDF = True
SVG = False

STR_APPROACH = "Approach"
AXES_FONTSIZE = 13


def plotter(meta):
    plot_overall_boxplot(meta, "all_overall_boxplot")
    plot_overall_barchart(meta, "all_overall_barchart")


def plot_overall_barchart(meta, name="all_overall_barchart"):
    overall = sort_data(meta["data_overall_all"])
    g = sns.barplot(x="query", hue=STR_APPROACH, y="time", data=overall, dodge=True)
    g.figure.set_size_inches(20, 8.27)
    g.legend(title="Approach", fontsize=AXES_FONTSIZE)

    save(meta, g, name)


def plot_overall_boxplot(meta, name="all_overall_boxplot"):
    overall = meta["data_overall"]

    # plotting
    g = sns.boxplot(x="query", hue=STR_APPROACH, y="time", data=overall, dodge=True)
    g.figure.set_size_inches(20, 8.27)
    g.legend(title="Approach", fontsize=AXES_FONTSIZE)

    save(meta, g, name)


def plot_stages(meta, name="stages"):
    dataAll = meta["data_all"]
    dataStages = dataAll[dataAll["timed_unit"] != "overall runtime"]
    # plot all stages together (stacked bar plot ?)
    for query in dataStages["query"].unique():
        stagesOfQuery = dataStages[dataStages["query"] == query]
        g = sns.barplot(x="timed_unit", y="time", hue=STR_APPROACH, data=stagesOfQuery, dodge=True)
        save(meta, g, f'{query}')
        #for stage in stagesOfQuery["timed_unit"].unique():
        #    dataStage = stagesOfQuery[stagesOfQuery["timed_unit"] == stage]
        #    g = sns.barplot(x=str_approach, y="time", data=dataStage, dodge=True)
        #    save(meta, g, f'{query}_{stage}')
        #g = sns.boxplot(x="query", hue=str_approach, y="time", data=overall, dodge=True)
        #g = stagesOfQuery.plot(title=query, x=str_approach, y="time", kind='bar', stacked=True)
        #save(meta, g, query)


def plot_pch(meta):
    dataAll = meta["no_headers_representative"]
    for query in dataAll["query"].unique():
        dataQuery = dataAll[dataAll["query"] == query]
        g = sns.barplot(x=STR_APPROACH, y="time", data=dataQuery, dodge=True)
        save(meta, g, f'{query}')


def plot_filter(meta):
    data = meta["data_overall_all"]
    g = sns.barplot(x=STR_APPROACH, y="time", data=data, dodge=True)
    save(meta, g, "representative_no-headers")


def plot_header_selection_only(meta):
    data = meta["class_representative"]
    data = filter_data(data, "Approach", "PCH optimized")
    data = filter_data(data, "Approach", "Hybrid")
    data = filter_data(data, "Approach", "Hybrid optimized")
    data = filter_data(data, "Approach", "Precompiled headers optimized")    
    g = sns.barplot(x="query", y="time", data=data, hue=STR_APPROACH, dodge=True)
    #g.figure.set_size_inches(20, 8.27)
    g.legend(title="Approach", fontsize=AXES_FONTSIZE)
    save(meta, g, "header_selection")

def plot_pch_only(meta):
    data = meta["class_representative"]
    data = filter_data(data, "Approach", "Header selection")
    data = filter_data(data, "Approach", "PCH optimized")
    data = filter_data(data, "Approach", "Hybrid")
    data = filter_data(data, "Approach", "Hybrid optimized")
    g = sns.barplot(x="query", y="time", data=data, hue=STR_APPROACH, dodge=True)
    #g.figure.set_size_inches(20, 8.27)
    g.legend(title="Approach", fontsize=AXES_FONTSIZE)
    save(meta, g, "pch_only")

def plot_header_selection_pch(meta):
    data = meta["class_representative"]
    data = filter_data(data, "Approach", "Hybrid")
    data = filter_data(data, "Approach", "Hybrid optimized")
    data = filter_data(data, "Approach", "PCH optimized")    
    g = sns.barplot(x="query", y="time", data=data, hue=STR_APPROACH, dodge=True)
    #g.figure.set_size_inches(20, 8.27)
    g.legend(title="Approach", fontsize=AXES_FONTSIZE)
    save(meta, g, "pch_hs")


def plot_pch_pchOpt(meta):
    data = meta["class_representative"]
    data = filter_data(data, "Approach", "Header selection")
    data = filter_data(data, "Approach", "Hybrid")
    data = filter_data(data, "Approach", "Hybrid optimized")
    data = filter_data(data, "Approach", "PCH")    
    data = sort_data(data)
    g = sns.barplot(x="query", y="time", data=data, hue=STR_APPROACH, dodge=True)
    print(g.figure.get_size_inches())
    g.legend(title="Approach", fontsize=AXES_FONTSIZE)
    save(meta, g, "pch_pchOpt")


def plot_hybrid(meta):
    data = meta["class_representative"]
    data = filter_data(data, "Approach", "Header selection")
    data = filter_data(data, "Approach", "Hybrid optimized")
    data = filter_data(data, "Approach", "PCH")
    data = sort_data(data)

    g = sns.barplot(x="query", y="time", data=data, hue=STR_APPROACH, dodge=True)
    #g.figure.set_size_inches(7, 4.8)
    g.legend(title="Approach", fontsize=AXES_FONTSIZE)
    
    # Show numbers on bars
    #for x in g.containers:
    #    g.bar_label(x)

def plot_hybridOpt(meta):
    data = meta["class_representative"]
    data = filter_data(data, "Approach", "Header selection")
    data = filter_data(data, "Approach", "PCH")
    data = sort_data(data)

    g = sns.barplot(x="query", y="time", data=data, hue=STR_APPROACH, dodge=True)
    #g.figure.set_size_inches(7, 4.8)
    g.legend(title="Approach", fontsize=AXES_FONTSIZE)
    
    # Show numbers on bars
    #for x in g.containers:
    #    g.bar_label(x)
    
    save(meta, g, "hybridOpt")

def plot_class_representative(meta):
    data = meta["class_representative"]
    data = filter_data(data, "Approach", "PCH")
    data = sort_data(data)

    g = sns.barplot(x="query", y="time", data=data, hue=STR_APPROACH, dodge=True)
    #g.figure.set_size_inches(7, 4.8)
    g.legend(title="Approach", fontsize=AXES_FONTSIZE)
    
    # Show numbers on bars
    #for x in g.containers:
    #    g.bar_label(x)
    
    save(meta, g, "class_representative")

def plot_all_class_representative(meta):
    data = meta["class_representative"]
    data = sort_data(data)

    g = sns.barplot(x="query", y="time", data=data, hue=STR_APPROACH, dodge=True)
    #g.figure.set_size_inches(7, 4.8)
    g.legend(title="Approach", fontsize=AXES_FONTSIZE)
    
    # Show numbers on bars
    #for x in g.containers:
    #    g.bar_label(x)
    
    save(meta, g, "all_class_representative")


def plot_pch(meta):
    data = meta["data_overall"]
    data = filter_data(data, "Approach", "PCH optimized")
    data = filter_data(data, "Approach", "Hybrid window optimized")
    data = filter_data(data, "Approach", "Hybrid window")
    data = filter_data(data, "Approach", "Hybrid minimal")
    data = filter_data(data, "Approach", "Hybrid minimal optimized")
    data = filter_data(data, "Approach", "Precompiled headers optimized")    
    data = filter_data(data, "Approach", "Precompiled headers")
    g = sns.barplot(x=STR_APPROACH, y="time", data=data, dodge=True)
    save(meta, g, "header_selection")


def plot_filter_pch(meta):
    data = meta["no_headers_representative"]
    data = filter_data(data, "Approach", "Precompiled headers")
    g = sns.barplot(x=STR_APPROACH, y="time", data=data, dodge=True)
    save(meta, g, "representative_no-headers")


def plot_filter_hybrid(meta):
    data = meta["no_headers_representative"]
    data = filter_data(data, "Approach", "PCH optimized")
    data = filter_data(data, "Approach", "Hybrid minimal optimized")
    g = sns.barplot(x=STR_APPROACH, y="time", data=data, dodge=True)
    save(meta, g, "representative_no-headers")


def plot_window(meta):
    data = meta["window_representative"]
    g = sns.barplot(x=STR_APPROACH, y="time", data=data, dodge=True)
    save(meta, g, "representative_window")


def plot_window_pch(meta):
    data = meta["window_representative"]
    data = filter_data(data, "Approach", "PCH optimized")
    data = filter_data(data, "Approach", "Hybrid window optimized")
    data = filter_data(data, "Approach", "Hybrid window")
    g = sns.barplot(x=STR_APPROACH, y="time", data=data, dodge=True)
    save(meta, g, "representative_window")


def plot_window_hybrid(meta):
    data = meta["window_representative"]
    data = filter_data(data, "Approach", "PCH optimized")
    data = filter_data(data, "Approach", "Hybrid window optimized")
    g = sns.barplot(x=STR_APPROACH, y="time", data=data, dodge=True)
    save(meta, g, "representative_window")


def calc(meta):
    dataWindow = meta["window_representative"]
    dataFilter = meta["no_headers_representative"]

    def calc_and_print(data, name):
        print(data, flush=True)

        def aggr_approach(approach):
            return data[data[STR_APPROACH == approach]]["time"].agg('average')

        avg_all = aggr_approach("No optimization")  # "all headers"

        for approach in ["Header selection", "Precompiled headers", "Precompiled headers optimized", "Hybrid minimal",
                         "Hybrid window", "Hybrid minimal optimized", "Hybrid window optimized"]:
            print(approach, flush=True)
            avg_approach = aggr_approach(approach)

            abs_approach = avg_all - avg_approach
            rel_approach = 100 / avg_all * abs_approach

            print(name, "-", approach, "absolute improvement", abs_approach, "ms")
            print(name, "-", approach, "relative improvement", rel_approach, "%")
            print("---")

        print("--- ---")

    for (data, name) in [(dataWindow, "Window"), (dataFilter, "Filter")]:
        calc_and_print(data, name)


def filter_data(data, column, value):
    return data[data[column] != value]


def sort_data(data):
    order = ["No optimization", "Header selection", "PCH", "PCH optimized", "Hybrid", "Hybrid optimized", "Hybrid minimal", "Hybrid window", "Hybrid minimal optimized", "Hybrid window optimized"]
    order = [s for s in order if s in data.Approach.unique()]
    tmp = pd.Series(order, name = "Approach").to_frame()
    return pd.merge(tmp, data, on = "Approach", how = "left")


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
    no_headers_representative = filter_data(no_headers_representative, STR_APPROACH, "Hybrid window")
    no_headers_representative = filter_data(no_headers_representative, STR_APPROACH, "Hybrid window optimized")
    window_representative = overall[overall["query"] == "window"]

    # only class representetives 
    class_representative = pd.concat([no_headers_representative, window_representative])
    # rename to Hybrid / Hybrid optimized
    class_representative = class_representative.replace(
        {"Hybrid minimal": "Hybrid", 
         "Hybrid window": "Hybrid",
         "Hybrid minimal optimized": "Hybrid optimized", 
         "Hybrid window optimized": "Hybrid optimized"})

    # headers_overall = overall[overall[str_approach] == "header selection"]
    # headers_filter = headers_overall[headers_overall["query"] == "filterQuery"]

    meta = {
        "data_all": fileDataFrame,
        "data_overall_all": overallAll,
        "data_overall": overall,
        "no_headers_representative": no_headers_representative,
        "window_representative": window_representative,
        "class_representative": class_representative,
        "output_dir": args.outputDirectory
    }
    return meta


def save(meta, g, name):
    filename = meta["output_dir"] + "/" + name
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

    g.set_xlabel("Query", fontsize=AXES_FONTSIZE)
    g.set_ylabel("Time [ms]", fontsize=AXES_FONTSIZE)

    g.figure.savefig(filename)
    g.clear()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-d", "--delimiter", action="store", dest="delimiter",
                        help="Set delimiter used in CSV file.", default="\t")
    parser.add_argument("dataFile",
                        help="CSV file that will be used as data for plotting")
    parser.add_argument("-o", "--output", dest="outputDirectory",
                        default="plots/",
                        help="Directory to save the resulting plot images to.")
    parser.add_argument("funs", default=[plotter], nargs='*')

    args = parser.parse_args()
    meta = prep(args)
    for funName in args.funs:
        fun = locals().get(funName)
        if fun is not None:
            fun(meta)
