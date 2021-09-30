import numpy as np
import pandas as pd
from plotly.subplots import make_subplots
from pathlib import Path
import sys
import io
import plotly.graph_objects as go
import plotly

# folder = "./"#in this folder
folder = "./"
folder_old = "/home/zeuchste/Dropbox/nes/ventura/"
folder_new = "/home/zeuchste/Dropbox/nes/ven2/"

files = [
    "changingThreadsAndSourceBaselineSourceSinkMemoryMode",
    "changingThreadsAndSourceBaselineReadOnlyMemoryMode",
    "changingThreadsAndSourceBaselineReadModifyWriteMemoryMode",
    "changingThreadsAndSourceMedSelectivityMemoryMode",
    "changingThreadsAndSourceHighSelectivityMemoryMode",
    "changingThreadsAndSourceLowSelectivityMemoryMode",
    "changingThreadsAndSourceProjectionOneOutMemoryMode",
    "changingThreadsAndSourceProjectionTwoOutMemoryMode",
    "changingThreadsAndSourceMapOneFieldMemoryMode",
    "changingThreadsAndSourceMapTwoFieldMemoryMode"
]

df = {}
EMTPYDATA = """Time,BM_Name,NES_Version,WorkerThreads,SourceCnt,ProcessedBuffersTotal,ProcessedTasksTotal,ProcessedTuplesTotal,ProcessedBytesTotal,ThroughputInTupsPerSec,ThroughputInTasksPerSec,ThroughputInMBPerSec,AvgTaskProcessingTime,AvgQueueSize,NumberOfBuffersToProduce,NumberOfBuffersInGlobalBufferManager,numberOfBuffersPerPipeline,NumberOfBuffersInSourceLocalBufferPool,BufferSizeInBytes,query,InputType
2021-06-07T11:26:59.893Z,changingSourceCntBufferSize1MB,0.0.433,1,1,0,0,0,0,0,0,0,0,0,5000000,65536,1024,1024,1048576,Query::from("input").map(Attribute("value") = Attribute("value") + 1).sink(NullOutputSinkDescriptor::create());,MemoryMode
"""

for file in files:
    print("proc file" + file + "\n")
    res_file_new = Path(folder_new + file + ".csv")
    if res_file_new.is_file():
        df[file + "_new"] = pd.read_csv(folder_new + file + ".csv")
    else:
        print("FILE " + file + " DOES NOT EXISTS create empty data frame")
        df[file] = pd.read_csv(io.StringIO(EMTPYDATA), sep=",")

    res_file_old = Path(folder_old + file + ".csv")
    if res_file_old.is_file():
        df[file + "_old"] = pd.read_csv(folder_old + file + ".csv")
    else:
        print("FILE " + file + " DOES NOT EXISTS create empty data frame")
        df[file] = pd.read_csv(io.StringIO(EMTPYDATA), sep=",")


# ###############################################################
fig = make_subplots(
    rows=5, cols=10,
    column_widths=[3, 3, 3, 3, 3, 3, 3, 3, 3, 3],
    row_heights=[1, 1, 1, 1,1],
    shared_xaxes=False,
    subplot_titles=[
        'Baseline Source/Sink',
        'Baseline ReadOnly',
        'Baseline ReadModifyWrite',
        'Selection Low Selectivity',
        'Selection Med Selectivity',
        'Selection High Selectivity',
        'Project 2 out of 3 out',
        'Project 1 out of 3 out',
        'Map add 1 field',
        'Map add 2 field',
    ],
)

# Get the color-wheel
dictOfNames = {
    1: 'maroon',
    2: 'goldenrod',
    3: '',
    4: 'olive',
    5: '',
    6: '',
    7: '',
    8: 'brown',
    9: '',
    10: '',
    11: 'plum',
    12: 'darkgrey',
}

# Get the color-wheel
dictOfNamesSrc = {
    1: 'limegreen',
    2: 'teal',
    3: '',
    4: 'black',
    5: '',
    6: '',
    7: '',
    8: 'orange',
    9: '',
    10: '',
    11: '',
    12: 'plum'
}

############################################# df_changingThreadsAndSourceNoProc #######################################################

df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"]["WorkerThreads"].astype(str) + '/S' + \
                                                                        df[
                                                                            "changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"][
                                                                            "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="New",
               showlegend=True
               ),
    row=1, col=1
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt Source/Sink", row=1, col=1)
fig.update_yaxes(title_text="ThroughputInTupsPerSec", type="log", row=1, col=1)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=1
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt Source/Sink", row=2, col=1)
fig.update_yaxes(title_text="ThroughputInMBPerSec", type="log", row=2, col=1)

df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"]["WorkerThreads"].astype(str) + '/S' + \
                                                                        df[
                                                                            "changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"][
                                                                            "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=1
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt Source/Sink", row=3, col=1)
fig.update_yaxes(title_text="AvgTaskProcessingTime", type="log", row=3, col=1)

df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"]["WorkerThreads"].astype(str) + '/S' + \
                                                                        df[
                                                                            "changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"][
                                                                            "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=4, col=1
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt Source/Sink", row=4, col=1)
fig.update_yaxes(title_text="AvgQueueSize", type="log", row=4, col=1)


df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"]["WorkerThreads"].astype(str) + '/S' + \
                                                                        df[
                                                                            "changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"][
                                                                            "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_new"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=1
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt Source/Sink", row=5, col=1)
fig.update_yaxes(title_text="ThroughputInTasksPerSec", type="log", row=5, col=1)

############################################# df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"] #######################################################

df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"]["WorkerThreads"].astype(str) + '/S' + \
                                                                  df[
                                                                      "changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"][
                                                                      "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=2
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadOnly", row=1, col=2)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=2
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadOnly", row=2, col=2)

df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"]["WorkerThreads"].astype(str) + '/S' + \
                                                                  df[
                                                                      "changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"][
                                                                      "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=2
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadOnly", row=3, col=2)

df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"]["WorkerThreads"].astype(str) + '/S' + \
                                                                  df[
                                                                      "changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"][
                                                                      "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgQueueSize",
               showlegend=False
               ),
    row=4, col=2
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadOnly", row=4, col=2)

df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"]["WorkerThreads"].astype(str) + '/S' + \
                                                                      df[
                                                                          "changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"][
                                                                          "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_new"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=2
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadOnly", row=5, col=2)

############################################# df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"] #######################################################

df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"]["WorkerThreads"].astype(str) + '/S' + \
                                                                         df[
                                                                             "changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"][
                                                                             "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=3
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadModifyWrite", row=1, col=3)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=3
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadModifyWrite", row=2, col=3)

df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"]["WorkerThreads"].astype(str) + '/S' + \
                                                                         df[
                                                                             "changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"][
                                                                             "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=3
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadModifyWrite", row=3, col=3)

df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"]["WorkerThreads"].astype(str) + '/S' + \
                                                                         df[
                                                                             "changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"][
                                                                             "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgQueueSize",
               showlegend=False
               ),
    row=4, col=3
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadModifyWrite", row=4, col=3)

df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"]["WorkerThreads"].astype(str) + '/S' + \
                                                                             df[
                                                                                 "changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"][
                                                                                 "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_new"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=3
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadModifyWrite", row=5, col=3)
############################################# df["changingThreadsAndSourceLowSelectivityMemoryMode_new"] #######################################################

df["changingThreadsAndSourceLowSelectivityMemoryMode_new"]["src"] = "W" + \
                                                                df["changingThreadsAndSourceLowSelectivityMemoryMode_new"][
                                                                    "WorkerThreads"].astype(str) + '/S' + \
                                                                df["changingThreadsAndSourceLowSelectivityMemoryMode_new"][
                                                                    "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceLowSelectivityMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceLowSelectivityMemoryMode_new"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=4
)
fig.update_xaxes(title_text="Filter (10%)", row=1, col=4)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceLowSelectivityMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceLowSelectivityMemoryMode_new"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=4
)
fig.update_xaxes(title_text="Filter (10%)", row=2, col=4)

df["changingThreadsAndSourceLowSelectivityMemoryMode_new"]["src"] = "W" + \
                                                                df["changingThreadsAndSourceLowSelectivityMemoryMode_new"][
                                                                    "WorkerThreads"].astype(str) + '/S' + \
                                                                df["changingThreadsAndSourceLowSelectivityMemoryMode_new"][
                                                                    "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceLowSelectivityMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceLowSelectivityMemoryMode_new"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=4
)
fig.update_xaxes(title_text="Filter (10%)", row=3, col=4)

df["changingThreadsAndSourceLowSelectivityMemoryMode_new"]["src"] = "W" + \
                                                                df["changingThreadsAndSourceLowSelectivityMemoryMode_new"][
                                                                    "WorkerThreads"].astype(str) + '/S' + \
                                                                df["changingThreadsAndSourceLowSelectivityMemoryMode_new"][
                                                                    "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceLowSelectivityMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceLowSelectivityMemoryMode_new"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgQueueSize",
               showlegend=False
               ),
    row=4, col=4
)
fig.update_xaxes(title_text="Filter (10%)", row=4, col=4)


df["changingThreadsAndSourceLowSelectivityMemoryMode_new"]["src"] = "W" + \
                                                                    df["changingThreadsAndSourceLowSelectivityMemoryMode_new"][
                                                                        "WorkerThreads"].astype(str) + '/S' + \
                                                                    df["changingThreadsAndSourceLowSelectivityMemoryMode_new"][
                                                                        "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceLowSelectivityMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceLowSelectivityMemoryMode_new"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=4
)
fig.update_xaxes(title_text="Filter (10%)", row=5, col=4)

####################################################################################################


############################################# df["changingThreadsAndSourceMedSelectivityMemoryMode_new"] #######################################################

df["changingThreadsAndSourceMedSelectivityMemoryMode_new"]["src"] = "W" + \
                                                                df["changingThreadsAndSourceMedSelectivityMemoryMode_new"][
                                                                    "WorkerThreads"].astype(str) + '/S' + \
                                                                df["changingThreadsAndSourceMedSelectivityMemoryMode_new"][
                                                                    "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMedSelectivityMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceMedSelectivityMemoryMode_new"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=5
)
fig.update_xaxes(title_text="Filter (50%)", row=1, col=5)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMedSelectivityMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceMedSelectivityMemoryMode_new"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=5
)
fig.update_xaxes(title_text="Filter (50%)", row=2, col=5)

df["changingThreadsAndSourceMedSelectivityMemoryMode_new"]["src"] = "W" + \
                                                                df["changingThreadsAndSourceMedSelectivityMemoryMode_new"][
                                                                    "WorkerThreads"].astype(str) + '/S' + \
                                                                df["changingThreadsAndSourceMedSelectivityMemoryMode_new"][
                                                                    "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMedSelectivityMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceMedSelectivityMemoryMode_new"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=5
)
fig.update_xaxes(title_text="Filter (50%)", row=3, col=5)

df["changingThreadsAndSourceMedSelectivityMemoryMode_new"]["src"] = "W" + \
                                                                df["changingThreadsAndSourceMedSelectivityMemoryMode_new"][
                                                                    "WorkerThreads"].astype(str) + '/S' + \
                                                                df["changingThreadsAndSourceMedSelectivityMemoryMode_new"][
                                                                    "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMedSelectivityMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceMedSelectivityMemoryMode_new"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgQueueSize",
               showlegend=False
               ),
    row=4, col=5
)
fig.update_xaxes(title_text="Filter (50%)", row=4, col=5)

df["changingThreadsAndSourceMedSelectivityMemoryMode_new"]["src"] = "W" + \
                                                                    df["changingThreadsAndSourceMedSelectivityMemoryMode_new"][
                                                                        "WorkerThreads"].astype(str) + '/S' + \
                                                                    df["changingThreadsAndSourceMedSelectivityMemoryMode_new"][
                                                                        "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMedSelectivityMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceMedSelectivityMemoryMode_new"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=5
)
fig.update_xaxes(title_text="Filter (50%)", row=5, col=5)
####################################################################################################


############################################# df["changingThreadsAndSourceHighSelectivityMemoryMode_new"] #######################################################

df["changingThreadsAndSourceHighSelectivityMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceHighSelectivityMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceHighSelectivityMemoryMode_new"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceHighSelectivityMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceHighSelectivityMemoryMode_new"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=6
)
fig.update_xaxes(title_text="Filter (90%)", row=1, col=6)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceHighSelectivityMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceHighSelectivityMemoryMode_new"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=6
)
fig.update_xaxes(title_text="Filter (90%)", row=2, col=6)

df["changingThreadsAndSourceHighSelectivityMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceHighSelectivityMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceHighSelectivityMemoryMode_new"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceHighSelectivityMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceHighSelectivityMemoryMode_new"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=6
)
fig.update_xaxes(title_text="Filter (90%)", row=3, col=6)

df["changingThreadsAndSourceHighSelectivityMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceHighSelectivityMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceHighSelectivityMemoryMode_new"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceHighSelectivityMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceHighSelectivityMemoryMode_new"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgQueueSize",
               showlegend=False
               ),
    row=4, col=6
)
fig.update_xaxes(title_text="Filter (90%)", row=4, col=6)

df["changingThreadsAndSourceHighSelectivityMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceHighSelectivityMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceHighSelectivityMemoryMode_new"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceHighSelectivityMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceHighSelectivityMemoryMode_new"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=6
)
fig.update_xaxes(title_text="Filter (90%)", row=5, col=6)
############################################# df["changingThreadsAndSourceProjectionOneOutMemoryMode_new"] #######################################################

df["changingThreadsAndSourceProjectionOneOutMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceProjectionOneOutMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionOneOutMemoryMode_new"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionOneOutMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceProjectionOneOutMemoryMode_new"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=7
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=1, col=7)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionOneOutMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceProjectionOneOutMemoryMode_new"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=7
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=2, col=7)

df["changingThreadsAndSourceProjectionOneOutMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceProjectionOneOutMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionOneOutMemoryMode_new"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionOneOutMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceProjectionOneOutMemoryMode_new"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=7
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=3, col=7)

df["changingThreadsAndSourceProjectionOneOutMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceProjectionOneOutMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionOneOutMemoryMode_new"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionOneOutMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceProjectionOneOutMemoryMode_new"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgQueueSize",
               showlegend=False
               ),
    row=4, col=7
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=4, col=7)

df["changingThreadsAndSourceProjectionOneOutMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceProjectionOneOutMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionOneOutMemoryMode_new"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionOneOutMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceProjectionOneOutMemoryMode_new"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=7
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=5, col=7)

############################################# df["changingThreadsAndSourceProjectionTwoOutMemoryMode_new"] #######################################################

df["changingThreadsAndSourceProjectionTwoOutMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceProjectionTwoOutMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionTwoOutMemoryMode_new"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_new"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=8
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=1, col=8)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_new"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=8
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=2, col=8)

df["changingThreadsAndSourceProjectionTwoOutMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceProjectionTwoOutMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionTwoOutMemoryMode_new"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_new"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=8
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=3, col=8)

df["changingThreadsAndSourceProjectionTwoOutMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceProjectionTwoOutMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionTwoOutMemoryMode_new"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_new"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgQueueSize",
               showlegend=False
               ),
    row=4, col=8
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=4, col=8)

df["changingThreadsAndSourceProjectionTwoOutMemoryMode_new"]["src"] = "W" + df[
    "changingThreadsAndSourceProjectionTwoOutMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionTwoOutMemoryMode_new"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_new"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=8
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=5, col=8)
############################################# df["changingThreadsAndSourceMapOneFieldMemoryMode_new"] #######################################################

df["changingThreadsAndSourceMapOneFieldMemoryMode_new"]["src"] = "W" + df["changingThreadsAndSourceMapOneFieldMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapOneFieldMemoryMode_new"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapOneFieldMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceMapOneFieldMemoryMode_new"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=9
)
fig.update_xaxes(title_text="Map add 1 field", row=1, col=9)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapOneFieldMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceMapOneFieldMemoryMode_new"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=9
)
fig.update_xaxes(title_text="Map add 1 field", row=2, col=9)

df["changingThreadsAndSourceMapOneFieldMemoryMode_new"]["src"] = "W" + df["changingThreadsAndSourceMapOneFieldMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapOneFieldMemoryMode_new"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapOneFieldMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceMapOneFieldMemoryMode_new"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=9
)
fig.update_xaxes(title_text="Map add 1 field", row=3, col=9)

df["changingThreadsAndSourceMapOneFieldMemoryMode_new"]["src"] = "W" + df["changingThreadsAndSourceMapOneFieldMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapOneFieldMemoryMode_new"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapOneFieldMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceMapOneFieldMemoryMode_new"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=4, col=9
)
fig.update_xaxes(title_text="Map add 1 field", row=4, col=9)

df["changingThreadsAndSourceMapOneFieldMemoryMode_new"]["src"] = "W" + df["changingThreadsAndSourceMapOneFieldMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapOneFieldMemoryMode_new"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapOneFieldMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceMapOneFieldMemoryMode_new"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=9
)
fig.update_xaxes(title_text="Map add 1 field", row=5, col=9)
############################################# df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"] #######################################################

df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"]["src"] = "W" + df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=10
)
fig.update_xaxes(title_text="Map add 2 fields", row=1, col=10)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=10
)
fig.update_xaxes(title_text="Map add 2 fields", row=2, col=10)

df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"]["src"] = "W" + df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=10
)
fig.update_xaxes(title_text="Map add 2 fields", row=3, col=10)

df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"]["src"] = "W" + df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="AvgQueueSize",
               showlegend=False
               ),
    row=4, col=10
)
fig.update_xaxes(title_text="Map add 2 fields", row=4, col=10)

df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"]["src"] = "W" + df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"]['src'],
               y=df["changingThreadsAndSourceMapTwoFieldMemoryMode_new"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=10
)
fig.update_xaxes(title_text="Map add 2 fields", row=5, col=10)
###############

################ OLD




 ####


############################################# df_changingThreadsAndSourceNoProc #######################################################

df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"]["WorkerThreads"].astype(str) + '/S' + \
                                                                        df[
                                                                            "changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"][
                                                                            "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="Old",
               showlegend=True
               ),
    row=1, col=1
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt Source/Sink", row=1, col=1)
fig.update_yaxes(title_text="ThroughputInTupsPerSec", type="log", row=1, col=1)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=1
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt Source/Sink", row=2, col=1)
fig.update_yaxes(title_text="ThroughputInMBPerSec", type="log", row=2, col=1)

df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"]["WorkerThreads"].astype(str) + '/S' + \
                                                                        df[
                                                                            "changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"][
                                                                            "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=1
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt Source/Sink", row=3, col=1)
fig.update_yaxes(title_text="AvgTaskProcessingTime", type="log", row=3, col=1)

df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"]["WorkerThreads"].astype(str) + '/S' + \
                                                                        df[
                                                                            "changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"][
                                                                            "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=4, col=1
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt Source/Sink", row=4, col=1)
fig.update_yaxes(title_text="AvgQueueSize", type="log", row=4, col=1)


df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"]["WorkerThreads"].astype(str) + '/S' + \
                                                                        df[
                                                                            "changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"][
                                                                            "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode_old"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=1
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt Source/Sink", row=5, col=1)
fig.update_yaxes(title_text="ThroughputInTasksPerSec", type="log", row=5, col=1)

############################################# df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"] #######################################################

df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"]["WorkerThreads"].astype(str) + '/S' + \
                                                                      df[
                                                                          "changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"][
                                                                          "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=2
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadOnly", row=1, col=2)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=2
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadOnly", row=2, col=2)

df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"]["WorkerThreads"].astype(str) + '/S' + \
                                                                      df[
                                                                          "changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"][
                                                                          "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=2
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadOnly", row=3, col=2)

df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"]["WorkerThreads"].astype(str) + '/S' + \
                                                                      df[
                                                                          "changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"][
                                                                          "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgQueueSize",
               showlegend=False
               ),
    row=4, col=2
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadOnly", row=4, col=2)

df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"]["WorkerThreads"].astype(str) + '/S' + \
                                                                      df[
                                                                          "changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"][
                                                                          "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode_old"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=2
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadOnly", row=5, col=2)

############################################# df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"] #######################################################

df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"]["WorkerThreads"].astype(str) + '/S' + \
                                                                             df[
                                                                                 "changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"][
                                                                                 "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=3
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadModifyWrite", row=1, col=3)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=3
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadModifyWrite", row=2, col=3)

df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"]["WorkerThreads"].astype(str) + '/S' + \
                                                                             df[
                                                                                 "changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"][
                                                                                 "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=3
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadModifyWrite", row=3, col=3)

df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"]["WorkerThreads"].astype(str) + '/S' + \
                                                                             df[
                                                                                 "changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"][
                                                                                 "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgQueueSize",
               showlegend=False
               ),
    row=4, col=3
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadModifyWrite", row=4, col=3)

df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"]["WorkerThreads"].astype(str) + '/S' + \
                                                                             df[
                                                                                 "changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"][
                                                                                 "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode_old"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=3
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadModifyWrite", row=5, col=3)
############################################# df["changingThreadsAndSourceLowSelectivityMemoryMode_old"] #######################################################

df["changingThreadsAndSourceLowSelectivityMemoryMode_old"]["src"] = "W" + \
                                                                    df["changingThreadsAndSourceLowSelectivityMemoryMode_old"][
                                                                        "WorkerThreads"].astype(str) + '/S' + \
                                                                    df["changingThreadsAndSourceLowSelectivityMemoryMode_old"][
                                                                        "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceLowSelectivityMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceLowSelectivityMemoryMode_old"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=4
)
fig.update_xaxes(title_text="Filter (10%)", row=1, col=4)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceLowSelectivityMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceLowSelectivityMemoryMode_old"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=4
)
fig.update_xaxes(title_text="Filter (10%)", row=2, col=4)

df["changingThreadsAndSourceLowSelectivityMemoryMode_old"]["src"] = "W" + \
                                                                    df["changingThreadsAndSourceLowSelectivityMemoryMode_old"][
                                                                        "WorkerThreads"].astype(str) + '/S' + \
                                                                    df["changingThreadsAndSourceLowSelectivityMemoryMode_old"][
                                                                        "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceLowSelectivityMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceLowSelectivityMemoryMode_old"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=4
)
fig.update_xaxes(title_text="Filter (10%)", row=3, col=4)

df["changingThreadsAndSourceLowSelectivityMemoryMode_old"]["src"] = "W" + \
                                                                    df["changingThreadsAndSourceLowSelectivityMemoryMode_old"][
                                                                        "WorkerThreads"].astype(str) + '/S' + \
                                                                    df["changingThreadsAndSourceLowSelectivityMemoryMode_old"][
                                                                        "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceLowSelectivityMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceLowSelectivityMemoryMode_old"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgQueueSize",
               showlegend=False
               ),
    row=4, col=4
)
fig.update_xaxes(title_text="Filter (10%)", row=4, col=4)


df["changingThreadsAndSourceLowSelectivityMemoryMode_old"]["src"] = "W" + \
                                                                    df["changingThreadsAndSourceLowSelectivityMemoryMode_old"][
                                                                        "WorkerThreads"].astype(str) + '/S' + \
                                                                    df["changingThreadsAndSourceLowSelectivityMemoryMode_old"][
                                                                        "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceLowSelectivityMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceLowSelectivityMemoryMode_old"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=4
)
fig.update_xaxes(title_text="Filter (10%)", row=5, col=4)

####################################################################################################


############################################# df["changingThreadsAndSourceMedSelectivityMemoryMode_old"] #######################################################

df["changingThreadsAndSourceMedSelectivityMemoryMode_old"]["src"] = "W" + \
                                                                    df["changingThreadsAndSourceMedSelectivityMemoryMode_old"][
                                                                        "WorkerThreads"].astype(str) + '/S' + \
                                                                    df["changingThreadsAndSourceMedSelectivityMemoryMode_old"][
                                                                        "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMedSelectivityMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceMedSelectivityMemoryMode_old"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=5
)
fig.update_xaxes(title_text="Filter (50%)", row=1, col=5)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMedSelectivityMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceMedSelectivityMemoryMode_old"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=5
)
fig.update_xaxes(title_text="Filter (50%)", row=2, col=5)

df["changingThreadsAndSourceMedSelectivityMemoryMode_old"]["src"] = "W" + \
                                                                    df["changingThreadsAndSourceMedSelectivityMemoryMode_old"][
                                                                        "WorkerThreads"].astype(str) + '/S' + \
                                                                    df["changingThreadsAndSourceMedSelectivityMemoryMode_old"][
                                                                        "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMedSelectivityMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceMedSelectivityMemoryMode_old"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=5
)
fig.update_xaxes(title_text="Filter (50%)", row=3, col=5)

df["changingThreadsAndSourceMedSelectivityMemoryMode_old"]["src"] = "W" + \
                                                                    df["changingThreadsAndSourceMedSelectivityMemoryMode_old"][
                                                                        "WorkerThreads"].astype(str) + '/S' + \
                                                                    df["changingThreadsAndSourceMedSelectivityMemoryMode_old"][
                                                                        "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMedSelectivityMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceMedSelectivityMemoryMode_old"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgQueueSize",
               showlegend=False
               ),
    row=4, col=5
)
fig.update_xaxes(title_text="Filter (50%)", row=4, col=5)

df["changingThreadsAndSourceMedSelectivityMemoryMode_old"]["src"] = "W" + \
                                                                    df["changingThreadsAndSourceMedSelectivityMemoryMode_old"][
                                                                        "WorkerThreads"].astype(str) + '/S' + \
                                                                    df["changingThreadsAndSourceMedSelectivityMemoryMode_old"][
                                                                        "SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMedSelectivityMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceMedSelectivityMemoryMode_old"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=5
)
fig.update_xaxes(title_text="Filter (50%)", row=5, col=5)
####################################################################################################


############################################# df["changingThreadsAndSourceHighSelectivityMemoryMode_old"] #######################################################

df["changingThreadsAndSourceHighSelectivityMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceHighSelectivityMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceHighSelectivityMemoryMode_old"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceHighSelectivityMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceHighSelectivityMemoryMode_old"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=6
)
fig.update_xaxes(title_text="Filter (90%)", row=1, col=6)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceHighSelectivityMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceHighSelectivityMemoryMode_old"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=6
)
fig.update_xaxes(title_text="Filter (90%)", row=2, col=6)

df["changingThreadsAndSourceHighSelectivityMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceHighSelectivityMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceHighSelectivityMemoryMode_old"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceHighSelectivityMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceHighSelectivityMemoryMode_old"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=6
)
fig.update_xaxes(title_text="Filter (90%)", row=3, col=6)

df["changingThreadsAndSourceHighSelectivityMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceHighSelectivityMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceHighSelectivityMemoryMode_old"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceHighSelectivityMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceHighSelectivityMemoryMode_old"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgQueueSize",
               showlegend=False
               ),
    row=4, col=6
)
fig.update_xaxes(title_text="Filter (90%)", row=4, col=6)

df["changingThreadsAndSourceHighSelectivityMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceHighSelectivityMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceHighSelectivityMemoryMode_old"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceHighSelectivityMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceHighSelectivityMemoryMode_old"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=6
)
fig.update_xaxes(title_text="Filter (90%)", row=5, col=6)
############################################# df["changingThreadsAndSourceProjectionOneOutMemoryMode_old"] #######################################################

df["changingThreadsAndSourceProjectionOneOutMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceProjectionOneOutMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionOneOutMemoryMode_old"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionOneOutMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceProjectionOneOutMemoryMode_old"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=7
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=1, col=7)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionOneOutMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceProjectionOneOutMemoryMode_old"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=7
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=2, col=7)

df["changingThreadsAndSourceProjectionOneOutMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceProjectionOneOutMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionOneOutMemoryMode_old"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionOneOutMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceProjectionOneOutMemoryMode_old"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=7
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=3, col=7)

df["changingThreadsAndSourceProjectionOneOutMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceProjectionOneOutMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionOneOutMemoryMode_old"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionOneOutMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceProjectionOneOutMemoryMode_old"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgQueueSize",
               showlegend=False
               ),
    row=4, col=7
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=4, col=7)

df["changingThreadsAndSourceProjectionOneOutMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceProjectionOneOutMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionOneOutMemoryMode_old"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionOneOutMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceProjectionOneOutMemoryMode_old"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=7
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=5, col=7)

############################################# df["changingThreadsAndSourceProjectionTwoOutMemoryMode_old"] #######################################################

df["changingThreadsAndSourceProjectionTwoOutMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceProjectionTwoOutMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionTwoOutMemoryMode_old"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_old"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=8
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=1, col=8)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_old"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=8
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=2, col=8)

df["changingThreadsAndSourceProjectionTwoOutMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceProjectionTwoOutMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionTwoOutMemoryMode_old"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_old"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=8
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=3, col=8)

df["changingThreadsAndSourceProjectionTwoOutMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceProjectionTwoOutMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionTwoOutMemoryMode_old"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_old"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgQueueSize",
               showlegend=False
               ),
    row=4, col=8
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=4, col=8)

df["changingThreadsAndSourceProjectionTwoOutMemoryMode_old"]["src"] = "W" + df[
    "changingThreadsAndSourceProjectionTwoOutMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionTwoOutMemoryMode_old"]["SourceCnt"].astype(
    str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceProjectionTwoOutMemoryMode_old"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=8
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=5, col=8)
############################################# df["changingThreadsAndSourceMapOneFieldMemoryMode_old"] #######################################################

df["changingThreadsAndSourceMapOneFieldMemoryMode_old"]["src"] = "W" + df["changingThreadsAndSourceMapOneFieldMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapOneFieldMemoryMode_old"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapOneFieldMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceMapOneFieldMemoryMode_old"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=9
)
fig.update_xaxes(title_text="Map add 1 field", row=1, col=9)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapOneFieldMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceMapOneFieldMemoryMode_old"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=9
)
fig.update_xaxes(title_text="Map add 1 field", row=2, col=9)

df["changingThreadsAndSourceMapOneFieldMemoryMode_old"]["src"] = "W" + df["changingThreadsAndSourceMapOneFieldMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapOneFieldMemoryMode_old"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapOneFieldMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceMapOneFieldMemoryMode_old"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=9
)
fig.update_xaxes(title_text="Map add 1 field", row=3, col=9)

df["changingThreadsAndSourceMapOneFieldMemoryMode_old"]["src"] = "W" + df["changingThreadsAndSourceMapOneFieldMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapOneFieldMemoryMode_old"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapOneFieldMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceMapOneFieldMemoryMode_old"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=4, col=9
)
fig.update_xaxes(title_text="Map add 1 field", row=4, col=9)

df["changingThreadsAndSourceMapOneFieldMemoryMode_old"]["src"] = "W" + df["changingThreadsAndSourceMapOneFieldMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapOneFieldMemoryMode_old"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapOneFieldMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceMapOneFieldMemoryMode_old"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=9
)
fig.update_xaxes(title_text="Map add 1 field", row=5, col=9)
############################################# df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"] #######################################################

df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"]["src"] = "W" + df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=10
)
fig.update_xaxes(title_text="Map add 2 fields", row=1, col=10)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"]['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=10
)
fig.update_xaxes(title_text="Map add 2 fields", row=2, col=10)

df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"]["src"] = "W" + df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=10
)
fig.update_xaxes(title_text="Map add 2 fields", row=3, col=10)

df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"]["src"] = "W" + df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="AvgQueueSize",
               showlegend=False
               ),
    row=4, col=10
)
fig.update_xaxes(title_text="Map add 2 fields", row=4, col=10)

df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"]["src"] = "W" + df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"]['src'],
               y=df["changingThreadsAndSourceMapTwoFieldMemoryMode_old"]['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInTasksPerSec",
               showlegend=False
               ),
    row=5, col=10
)
fig.update_xaxes(title_text="Map add 2 fields", row=5, col=10)
###############












fig.update_layout(barmode='overlay')
fig.update_layout(
    title={
        'text': "<b>NebulaStream Performance Numbers -- Memory Mode -- Stateless Queries </b><br>"
                "<span style=\"font-size:0.6em\" line-height=1em;margin=-4px>Default Config(GlobalBufferPool=16K, LocalBufferPool=1024, BufferSize=1MB, TupleSize=24Byte (3 Attributes))"
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

###############################################################
plotly.offline.plot(fig, filename='DiffOldNewForStatelessQueries.html')
