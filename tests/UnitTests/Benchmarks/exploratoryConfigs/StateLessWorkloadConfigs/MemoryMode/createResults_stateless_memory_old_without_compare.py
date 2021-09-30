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
folder = "/home/zeuchste/Dropbox/nes/ventura/"
folder = "/home/zeuchste/Dropbox/nes/ven2/"

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

df={}
EMTPYDATA="""Time,BM_Name,NES_Version,WorkerThreads,SourceCnt,ProcessedBuffersTotal,ProcessedTasksTotal,ProcessedTuplesTotal,ProcessedBytesTotal,ThroughputInTupsPerSec,ThroughputInTasksPerSec,ThroughputInMBPerSec,AvgTaskProcessingTime,AvgQueueSize,NumberOfBuffersToProduce,NumberOfBuffersInGlobalBufferManager,numberOfBuffersPerPipeline,NumberOfBuffersInSourceLocalBufferPool,BufferSizeInBytes,query,InputType
2021-06-07T11:26:59.893Z,changingSourceCntBufferSize1MB,0.0.433,1,1,0,0,0,0,0,0,0,0,0,5000000,65536,1024,1024,1048576,Query::from("input").map(Attribute("value") = Attribute("value") + 1).sink(NullOutputSinkDescriptor::create());,MemoryMode
"""

for file in files:
    print ("proc file" + file + "\n")
    res_file = Path(folder + file + ".csv")
    if res_file.is_file():
        df[file] = pd.read_csv(folder + file + ".csv")
    else:
        print("FILE " + file + " DOES NOT EXISTS create empty data frame")
        df[file] = pd.read_csv(  io.StringIO(EMTPYDATA)  , sep=",")

# ###############################################################
fig = make_subplots(
    rows=4, cols=10,
    column_widths=[3, 3, 3, 3, 3, 3, 3, 3,3,3],
    row_heights=[1, 1, 1 ,1],
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

df["changingThreadsAndSourceBaselineSourceSinkMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceBaselineSourceSinkMemoryMode"]["WorkerThreads"].astype(str) + '/S' + \
                                               df["changingThreadsAndSourceBaselineSourceSinkMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode"]['src'],
               y=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=1
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt Source/Sink", row=1, col=1)
fig.update_yaxes(title_text="ThroughputInTupsPerSec", type="log", row=1, col=1)

df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]["WorkerThreads"].astype(str) + '/S' + \
                                                                    df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]['src'],
               y=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=1
)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode"]['src'], y=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode"]['ThroughputInMBPerSec'],
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


df["changingThreadsAndSourceBaselineSourceSinkMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceBaselineSourceSinkMemoryMode"]["WorkerThreads"].astype(str) + '/S' + \
                                               df["changingThreadsAndSourceBaselineSourceSinkMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode"]['src'], y=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=1
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt Source/Sink", row=3, col=1)
fig.update_yaxes(title_text="AvgTaskProcessingTime", type="log", row=3, col=1)

df["changingThreadsAndSourceBaselineSourceSinkMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceBaselineSourceSinkMemoryMode"]["WorkerThreads"].astype(str) + '/S' + \
                                               df["changingThreadsAndSourceBaselineSourceSinkMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode"]['src'], y=df["changingThreadsAndSourceBaselineSourceSinkMemoryMode"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=4, col=1
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt Source/Sink", row=4, col=1)
fig.update_yaxes(title_text="AvgQueueSize", type="log", row=4, col=1)


############################################# df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"] #######################################################

df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]["WorkerThreads"].astype(str) + '/S' + \
                                             df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]['src'],
               y=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]['ThroughputInTupsPerSec'],
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
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]['src'], y=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]['ThroughputInMBPerSec'],
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


df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]["WorkerThreads"].astype(str) + '/S' + \
                                             df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]['src'], y=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=2
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadOnly", row=3, col=2)

df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]["WorkerThreads"].astype(str) + '/S' + \
                                             df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]['src'], y=df["changingThreadsAndSourceBaselineReadOnlyMemoryMode"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=4, col=2
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadOnly", row=4, col=2)


############################################# df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode"] #######################################################

df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode"]["WorkerThreads"].astype(str) + '/S' + \
                                                    df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode"]['src'],
               y=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode"]['ThroughputInTupsPerSec'],
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
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode"]['src'], y=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode"]['ThroughputInMBPerSec'],
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


df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode"]["WorkerThreads"].astype(str) + '/S' + \
                                                    df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode"]['src'], y=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=3
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadModifyWrite", row=3, col=3)


df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode"]["WorkerThreads"].astype(str) + '/S' + \
                                                    df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode"]['src'], y=df["changingThreadsAndSourceBaselineReadModifyWriteMemoryMode"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=4, col=3
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt ReadModifyWrite", row=4, col=3)
############################################# df["changingThreadsAndSourceLowSelectivityMemoryMode"] #######################################################

df["changingThreadsAndSourceLowSelectivityMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceLowSelectivityMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceLowSelectivityMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceLowSelectivityMemoryMode"]['src'],
               y=df["changingThreadsAndSourceLowSelectivityMemoryMode"]['ThroughputInTupsPerSec'],
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
    go.Scatter(x=df["changingThreadsAndSourceLowSelectivityMemoryMode"]['src'],
               y=df["changingThreadsAndSourceLowSelectivityMemoryMode"]['ThroughputInMBPerSec'],
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

df["changingThreadsAndSourceLowSelectivityMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceLowSelectivityMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceLowSelectivityMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceLowSelectivityMemoryMode"]['src'],
               y=df["changingThreadsAndSourceLowSelectivityMemoryMode"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=4
)
fig.update_xaxes(title_text="Filter (10%)", row=3, col=4)

df["changingThreadsAndSourceLowSelectivityMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceLowSelectivityMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceLowSelectivityMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceLowSelectivityMemoryMode"]['src'],
               y=df["changingThreadsAndSourceLowSelectivityMemoryMode"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=4, col=4
)
fig.update_xaxes(title_text="Filter (10%)", row=4, col=4)

####################################################################################################


############################################# df["changingThreadsAndSourceMedSelectivityMemoryMode"] #######################################################

df["changingThreadsAndSourceMedSelectivityMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceMedSelectivityMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMedSelectivityMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMedSelectivityMemoryMode"]['src'],
               y=df["changingThreadsAndSourceMedSelectivityMemoryMode"]['ThroughputInTupsPerSec'],
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
    go.Scatter(x=df["changingThreadsAndSourceMedSelectivityMemoryMode"]['src'],
               y=df["changingThreadsAndSourceMedSelectivityMemoryMode"]['ThroughputInMBPerSec'],
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

df["changingThreadsAndSourceMedSelectivityMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceMedSelectivityMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMedSelectivityMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMedSelectivityMemoryMode"]['src'],
               y=df["changingThreadsAndSourceMedSelectivityMemoryMode"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=5
)
fig.update_xaxes(title_text="Filter (50%)", row=3, col=5)

df["changingThreadsAndSourceMedSelectivityMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceMedSelectivityMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMedSelectivityMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMedSelectivityMemoryMode"]['src'],
               y=df["changingThreadsAndSourceMedSelectivityMemoryMode"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=4, col=5
)
fig.update_xaxes(title_text="Filter (50%)", row=4, col=5)
####################################################################################################


############################################# df["changingThreadsAndSourceHighSelectivityMemoryMode"] #######################################################

df["changingThreadsAndSourceHighSelectivityMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceHighSelectivityMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceHighSelectivityMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceHighSelectivityMemoryMode"]['src'],
               y=df["changingThreadsAndSourceHighSelectivityMemoryMode"]['ThroughputInTupsPerSec'],
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
    go.Scatter(x=df["changingThreadsAndSourceHighSelectivityMemoryMode"]['src'],
               y=df["changingThreadsAndSourceHighSelectivityMemoryMode"]['ThroughputInMBPerSec'],
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

df["changingThreadsAndSourceHighSelectivityMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceHighSelectivityMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceHighSelectivityMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceHighSelectivityMemoryMode"]['src'],
               y=df["changingThreadsAndSourceHighSelectivityMemoryMode"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=6
)
fig.update_xaxes(title_text="Filter (90%)", row=3, col=6)

df["changingThreadsAndSourceHighSelectivityMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceHighSelectivityMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceHighSelectivityMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceHighSelectivityMemoryMode"]['src'],
               y=df["changingThreadsAndSourceHighSelectivityMemoryMode"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=4, col=6
)
fig.update_xaxes(title_text="Filter (90%)", row=4, col=6)

############################################# df["changingThreadsAndSourceProjectionOneOutMemoryMode"] #######################################################

df["changingThreadsAndSourceProjectionOneOutMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceProjectionOneOutMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionOneOutMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionOneOutMemoryMode"]['src'],
               y=df["changingThreadsAndSourceProjectionOneOutMemoryMode"]['ThroughputInTupsPerSec'],
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
    go.Scatter(x=df["changingThreadsAndSourceProjectionOneOutMemoryMode"]['src'],
               y=df["changingThreadsAndSourceProjectionOneOutMemoryMode"]['ThroughputInMBPerSec'],
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

df["changingThreadsAndSourceProjectionOneOutMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceProjectionOneOutMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionOneOutMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionOneOutMemoryMode"]['src'],
               y=df["changingThreadsAndSourceProjectionOneOutMemoryMode"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=7
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=3, col=7)

df["changingThreadsAndSourceProjectionOneOutMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceProjectionOneOutMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionOneOutMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionOneOutMemoryMode"]['src'],
               y=df["changingThreadsAndSourceProjectionOneOutMemoryMode"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=4, col=7
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=4, col=7)

############################################# df["changingThreadsAndSourceProjectionTwoOutMemoryMode"] #######################################################

df["changingThreadsAndSourceProjectionTwoOutMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceProjectionTwoOutMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionTwoOutMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionTwoOutMemoryMode"]['src'],
               y=df["changingThreadsAndSourceProjectionTwoOutMemoryMode"]['ThroughputInTupsPerSec'],
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
    go.Scatter(x=df["changingThreadsAndSourceProjectionTwoOutMemoryMode"]['src'],
               y=df["changingThreadsAndSourceProjectionTwoOutMemoryMode"]['ThroughputInMBPerSec'],
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

df["changingThreadsAndSourceProjectionTwoOutMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceProjectionTwoOutMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionTwoOutMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionTwoOutMemoryMode"]['src'],
               y=df["changingThreadsAndSourceProjectionTwoOutMemoryMode"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=8
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=3, col=8)

df["changingThreadsAndSourceProjectionTwoOutMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceProjectionTwoOutMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceProjectionTwoOutMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceProjectionTwoOutMemoryMode"]['src'],
               y=df["changingThreadsAndSourceProjectionTwoOutMemoryMode"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=4, col=8
)
fig.update_xaxes(title_text="Projection 1 out of 3", row=4, col=8)
############################################# df["changingThreadsAndSourceMapOneFieldMemoryMode"] #######################################################

df["changingThreadsAndSourceMapOneFieldMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceMapOneFieldMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapOneFieldMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapOneFieldMemoryMode"]['src'],
               y=df["changingThreadsAndSourceMapOneFieldMemoryMode"]['ThroughputInTupsPerSec'],
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
    go.Scatter(x=df["changingThreadsAndSourceMapOneFieldMemoryMode"]['src'],
               y=df["changingThreadsAndSourceMapOneFieldMemoryMode"]['ThroughputInMBPerSec'],
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

df["changingThreadsAndSourceMapOneFieldMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceMapOneFieldMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapOneFieldMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapOneFieldMemoryMode"]['src'],
               y=df["changingThreadsAndSourceMapOneFieldMemoryMode"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=9
)
fig.update_xaxes(title_text="Map add 1 field", row=3, col=9)

df["changingThreadsAndSourceMapOneFieldMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceMapOneFieldMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapOneFieldMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapOneFieldMemoryMode"]['src'],
               y=df["changingThreadsAndSourceMapOneFieldMemoryMode"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=4, col=9
)
fig.update_xaxes(title_text="Map add 1 field", row=4, col=9)
############################################# df["changingThreadsAndSourceMapTwoFieldMemoryMode"] #######################################################

df["changingThreadsAndSourceMapTwoFieldMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceMapTwoFieldMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapTwoFieldMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapTwoFieldMemoryMode"]['src'],
               y=df["changingThreadsAndSourceMapTwoFieldMemoryMode"]['ThroughputInTupsPerSec'],
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
    go.Scatter(x=df["changingThreadsAndSourceMapTwoFieldMemoryMode"]['src'],
               y=df["changingThreadsAndSourceMapTwoFieldMemoryMode"]['ThroughputInMBPerSec'],
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

df["changingThreadsAndSourceMapTwoFieldMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceMapTwoFieldMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapTwoFieldMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapTwoFieldMemoryMode"]['src'],
               y=df["changingThreadsAndSourceMapTwoFieldMemoryMode"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=10
)
fig.update_xaxes(title_text="Map add 2 fields", row=3, col=10)

df["changingThreadsAndSourceMapTwoFieldMemoryMode"]["src"] = "W" + df["changingThreadsAndSourceMapTwoFieldMemoryMode"][
    "WorkerThreads"].astype(str) + '/S' + df["changingThreadsAndSourceMapTwoFieldMemoryMode"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSourceMapTwoFieldMemoryMode"]['src'],
               y=df["changingThreadsAndSourceMapTwoFieldMemoryMode"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=4, col=10
)
fig.update_xaxes(title_text="Map add 2 fields", row=4, col=10)
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
    orientation="h",
    yanchor="top",
    y=0.63,
    xanchor="right",
    x=0.6,
    font=dict(
        family="Courier New, monospace",
        size=20,
        color="RebeccaPurple"
    )
))

###############################################################
plotly.offline.plot(fig, filename='StateLessQueriesMemoryMode.html')
