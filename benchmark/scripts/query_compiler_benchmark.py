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


def plotter(args):
    meta = prep(args)
    plot_overall_boxplot(meta, "all_overall_boxplot")
    plot_overall_barchart(meta, "all_overall_barchart")


def plot_overall_barchart(meta, name):
    overall = meta["data_overall"]
    g = sns.barplot(x="query", hue="variant", y="time", data=overall, dodge=True)

    save(meta, g, name)


def plot_overall_boxplot(meta, name):
    overall = meta["data_overall"]

    # plotting
    g = sns.boxplot(x="query", hue="variant", y="time", data=overall, dodge=True)
    g.figure.set_size_inches(20, 8.27)

    save(meta, g, name)


def prep(args):
    # Load data into data frame
    fileDataFrame = pd.read_csv(args.dataFile, delimiter=args.delimiter)

    # Configure seaborn
    # - Apply the default theme
    sns.set_theme()

    # filter failed tests
    success = fileDataFrame[fileDataFrame["query"] != "join"]
    success = success[success["query"] != "tumblingWindowEventTime"]

    overall = success[success["timed_unit"] == "overall runtime"]

    #headers_overall = overall[overall["variant"] == "header selection"]
    #headers_filter = headers_overall[headers_overall["query"] == "filterQuery"]

    meta = {
        "data_overall": overall,
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
                        default="",
                        help="Directory to save the resulting plot images to.")
    args = parser.parse_args()
    plotter(args)
