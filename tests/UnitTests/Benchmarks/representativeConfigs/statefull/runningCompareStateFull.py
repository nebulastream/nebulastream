import numpy as np
import pandas as pd
from plotly.subplots import make_subplots
from pathlib import Path
import pathlib
import os

import sys
import io
import plotly.graph_objects as go
import plotly

# folder = "./"#in this folder
folders = [
    "/home/zeuchste/Dropbox/nes/statefull/",

]

versionDescriptiors = []
for f in folders:
    versionDescriptiors.append(os.path.basename(os.path.normpath(f)))


files = [
    "OneStreamOnlyFilling",
    "OneStreamFillingAndFireing",
    "YSBQuery"
]

# dfs = {}
dfs = []
EMTPYDATA = """Time,BM_Name,NES_Version,WorkerThreads,SourceCnt,ProcessedBuffersTotal,ProcessedTasksTotal,ProcessedTuplesTotal,ProcessedBytesTotal,ThroughputInTupsPerSec,ThroughputInTasksPerSec,ThroughputInMBPerSec,AvgTaskProcessingTime,AvgQueueSize,AvgAvailableGlobalBuffer,AvgAvailableFixed,NumberOfBuffersToProduce,NumberOfBuffersInGlobalBufferManager,numberOfBuffersPerPipeline,NumberOfBuffersInSourceLocalBufferPool,BufferSizeInBytes,query,InputType
2021-06-07T11:26:59.893Z,changingSourceCntBufferSize1MB,0.0.433,1,1,0,0,0,0,0,0,0,0,0,5000000,65536,1024,1024,1048576,Query::from("input").map(Attribute("value") = Attribute("value") + 1).sink(NullOutputSinkDescriptor::create());,MemoryMode
"""

for folder in folders:
    for file in files:
    # for file in files:
        path = folder + file + ".csv"
        print("proc file " + path + "\n")
        check_path = Path(path)
        if check_path.is_file():
            # dfs[folder + "_" + file] = pd.read_csv(path)
            dfs.append(pd.read_csv(path))
        else:
            print("FILE " + path + " DOES NOT EXISTS create empty data frame")
            # dfs[folder + "_" + file] = pd.read_csv(io.StringIO(EMTPYDATA), sep=",")
            dfs.append(pd.read_csv(io.StringIO(EMTPYDATA), sep=","))
# Get the color-wheel
colors = {
    0: 'limegreen',
    1: 'maroon',
    2: 'goldenrod',
    3: 'black',
    4: 'olive',
    5: 'orange',
    6: 'plum',
    7: 'red',
    8: 'brown',
    9: 'teal',
    10: 'plum',
    11: 'darkgrey',
}

# ###############################################################
fig = make_subplots(
    rows=7, cols=len(files),
    # column_widths=[3],
    # row_heights=[1],
    shared_xaxes=False,
    subplot_titles=[
        "FillWindowWithoutFire",
        "FillAndFireWindow",
        "YSB"
    ],
)

firstIteration = True
folderIndex = 0
fileIndex = 1
for df in dfs:
    print("fileIndex is=" +  str(fileIndex) + " folderidx=" + str(folderIndex))
    # print(df)
    df["src"] = "W" + df["WorkerThreads"].astype(str) + '/S' + df["SourceCnt"].astype(str)

    fig.add_trace(
        go.Scatter(x=df['src'],
                   y=df['ThroughputInTupsPerSec'],
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear',
                             color=colors[folderIndex],
                             width=2),
                       name=versionDescriptiors[folderIndex],
                       showlegend=firstIteration,
                       legendgroup=versionDescriptiors[folderIndex]
                   ),
            row=1, col=fileIndex
        )

    if(firstIteration == True):
        fig.update_yaxes(title_text="ThroughputInTupsPerSec", type="log", row=1, col=fileIndex)



    df["src"] = "W" + df["WorkerThreads"].astype(str) + '/S' + df["SourceCnt"].astype(str)
    fig.add_trace(
        go.Scatter(x=df['src'],
                   y=df['ThroughputInTasksPerSec'],
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear',
                             color=colors[folderIndex],
                             width=2),
                   name=versionDescriptiors[folderIndex],
                   showlegend=False,
                   legendgroup=versionDescriptiors[folderIndex]
                   ),
        row=2, col=fileIndex
    )

    if(firstIteration == True):
        fig.update_yaxes(title_text="ThroughputInTasksPerSec", type="log", row=2, col=fileIndex)

    fig.add_trace(
        go.Scatter(x=df['src'],
                   y=df['ThroughputInMBPerSec'],
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear',
                             color=colors[folderIndex],
                             width=2),
                   name=versionDescriptiors[folderIndex],
                   showlegend=False,
                   legendgroup=versionDescriptiors[folderIndex]
                   ),
        row=3, col=fileIndex
    )

    if(firstIteration == True):
        fig.update_yaxes(title_text="ThroughputInMBPerSec", type="log", row=3, col=fileIndex)

    df["src"] = "W" + df["WorkerThreads"].astype(str) + '/S' + df["SourceCnt"].astype(str)
    fig.add_trace(
        go.Scatter(x=df['src'],
                   y=df['AvgTaskProcessingTime'],
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear',
                             color=colors[folderIndex],
                             width=2),
                   name=versionDescriptiors[folderIndex],
                   showlegend=False,
                   legendgroup=versionDescriptiors[folderIndex]
                   ),
        row=4, col=fileIndex
    )

    if(firstIteration == True):
        fig.update_yaxes(title_text="AvgTaskProcessingTimeInMs", type="log", row=4, col=fileIndex)

    df["src"] = "W" + df["WorkerThreads"].astype(str) + '/S' + df["SourceCnt"].astype(str)
    fig.add_trace(
        go.Scatter(x=df['src'],
                   y=df['AvgQueueSize'],
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear',
                             color=colors[folderIndex],
                             width=2),
                   name=versionDescriptiors[folderIndex],
                   legendgroup=versionDescriptiors[folderIndex],
                   showlegend=False
                   ),
        row=5, col=fileIndex
    )

    if(firstIteration == True):
        fig.update_yaxes(title_text="AvgQueueSize", type="log", row=5, col=fileIndex)

    df["src"] = "W" + df["WorkerThreads"].astype(str) + '/S' + df["SourceCnt"].astype(str)
    fig.add_trace(
        go.Scatter(x=df['src'],
                   y=df['AvgAvailableGlobalBuffer'],
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear',
                             color=colors[folderIndex],
                             width=2),
                   name=versionDescriptiors[folderIndex],
                   legendgroup=versionDescriptiors[folderIndex],
                   showlegend=False
                   ),
        row=6, col=fileIndex
    )

    if(firstIteration == True):
        fig.update_yaxes(title_text="AvgFreeGlobBuffers", type="log", row=6, col=fileIndex)

    df["src"] = "W" + df["WorkerThreads"].astype(str) + '/S' + df["SourceCnt"].astype(str)
    fig.add_trace(
        go.Scatter(x=df['src'],
                   y=df['AvgAvailableFixed'],
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear',
                             color=colors[folderIndex],
                             width=2),
                   name=versionDescriptiors[folderIndex],
                   legendgroup=versionDescriptiors[folderIndex],
                   showlegend=False
                   ),
        row=7, col=fileIndex
    )

    if(firstIteration == True):
        fig.update_yaxes(title_text="AvgFreeFixBuffers", type="log", row=7, col=fileIndex)
        firstIteration = False

    # new folder will start here
    if(len(files) == fileIndex):
        fileIndex = 1
        folderIndex += 1
        firstIteration = True
    else:
        fileIndex += 1

fig.update_layout(barmode='overlay')
fig.update_layout(
    title={
        'text': "<b>NebulaStream Performance Numbers -- Set of representative state-full workloads</b><br>"
                "<span style=\"font-size:0.6em\" line-height=1em;margin=-4px>Default Config(GlobalBufferPool=65K, "
                "LocalBufferPool=1024, BufferSize=1MB, TupleSize=32Byte)"
                "<br>",
        'y': 0.98,
        'x': 0.5,
        'xanchor': 'center',
        'yanchor': 'top'},
)
fig.update_layout(
    # width=2500, height=1500,
    title_font_family="Times New Roman",
    title_font_color="RebeccaPurple",
    title_font_size=30,

    margin={'b': 80},
)

fig.update_layout(
    font=dict(
        family="Courier New, monospace",
        size=12,
        color="RebeccaPurple")
)
# fig.update_layout(legend_title_text='Worker <br> Sources')

fig.update_layout(legend=dict(
    # orientation="h",
    # yanchor="top",
    y=0.5,
    # xanchor="right",
    # x=0.6,
    font=dict(
        family="Courier New, monospace",
        size=20,
        color="RebeccaPurple"
    )
))
plotly.offline.plot(fig, filename='representativeWorkloadStateFull.html')
