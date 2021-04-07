import pandas as pd

df_changingBufferSize = pd.read_csv('./changingBufferSize.csv')
df_changingGlobalBufferCnt = pd.read_csv('./changingGlobalBufferCnt.csv')
df_changingLocalBufferSize = pd.read_csv('./changingLocalBufferSize.csv')
df_changingSourceCnt = pd.read_csv('./changingSourceCnt.csv')
df_changingThreadsAndSource = pd.read_csv('./changingThreadsAndSource.csv')
df_changingThreadsCnt = pd.read_csv('./changingThreadsWithFixSource.csv')
df_changingThreadsAndSourceOnlyFwdData = pd.read_csv('./changingThreadsAndSourceOnlyFwdData.csv')


import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly

# ###############################################################
fig = make_subplots(
    rows=3, cols=7,
    column_widths=[0.3, 0.3, 0.3, 0.3,0.3,0.3,0.3],
    row_heights=[1., 1., 1.],
    shared_xaxes=False,
    subplot_titles=[
        'Changing Buffer Size',
        'Changing Size Global Buffer Pool',
        'Changing Size Local Buffer Pool',
        'Changing Source Cnt',
        'Changing Worker Cnt',
        'Changing Workers & Sources',
        'Changing Workers & Sources No Proc.',
    ]
)


########################################## df_changingBufferSize ##########################################################
fig.add_trace(
    go.Scatter(x=df_changingBufferSize['BufferSizeInBytes'], y=df_changingBufferSize['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=1),
               name="Throughput in Tuples/sec",
               legendgroup='group1',
               showlegend=False
               ),
    row=1, col=1
)
fig.update_xaxes(title_text="BufferSizeInBytes", row=1, col=1)
fig.update_yaxes(title_text="Throughput in Tuples/sec", row=1, col=1)

fig.add_trace(
    go.Scatter(x=df_changingBufferSize['BufferSizeInBytes'], y=df_changingBufferSize['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=1),
               name="Throughput in MB/s",
               legendgroup='group2',
               showlegend=False
               ),
    row=2, col=1
)
fig.update_xaxes(title_text="BufferSizeInBytes", row=2, col=1)
fig.update_yaxes(title_text="ThroughputInMBPerSec", row=2, col=1)

fig.add_trace(
    go.Scatter(x=df_changingBufferSize['BufferSizeInBytes'], y=df_changingBufferSize['AvgLatencyInMs'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=1),
               name="Average Latency",
               legendgroup='group3',
               showlegend=False
               ),
    row=3, col=1
)
fig.update_xaxes(title_text="BufferSizeInBytes", row=3, col=1)
fig.update_yaxes(title_text="AvgLatencyInMs", row=3, col=1)

############################################# df_changingGlobalBufferCnt #######################################################
fig.add_trace(
    go.Scatter(x=df_changingGlobalBufferCnt['NumberOfBuffersInGlobalBufferManager'], y=df_changingBufferSize['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=1),
               name="Throughput in Tuples/sec",
               legendgroup='group1',
               showlegend=False
               ),
    row=1, col=2
)
fig.update_xaxes(title_text="NumberOfBuffersInGlobalBufferManager", row=1, col=2)
fig.update_yaxes(title_text="Throughput in Tuples/sec", row=1, col=2)

fig.add_trace(
    go.Scatter(x=df_changingGlobalBufferCnt['NumberOfBuffersInGlobalBufferManager'], y=df_changingBufferSize['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=1),
               name="Throughput in MB/s",
               legendgroup='group2',
               showlegend=False
               ),
    row=2, col=2
)
fig.update_xaxes(title_text="NumberOfBuffersInGlobalBufferManager", row=2, col=2)

fig.add_trace(
    go.Scatter(x=df_changingGlobalBufferCnt['NumberOfBuffersInGlobalBufferManager'], y=df_changingBufferSize['AvgLatencyInMs'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=1),
               name="Average Latency",
               legendgroup='group3',
               showlegend=False
               ),
    row=3, col=2
)
fig.update_xaxes(title_text="NumberOfBuffersInGlobalBufferManager", row=3, col=2)

####################################################################################################

############################################# df_changingLocalBufferSize #######################################################
fig.add_trace(
    go.Scatter(x=df_changingLocalBufferSize['NumberOfBuffersInSourceLocalBufferPool'], y=df_changingBufferSize['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=1),
               name="Throughput in Tuples/sec",
               legendgroup='group1',
               showlegend=False
               ),
    row=1, col=3
)
fig.update_xaxes(title_text="NumberOfBufferInLocalBufferPool", row=1, col=3)

fig.add_trace(
    go.Scatter(x=df_changingLocalBufferSize['NumberOfBuffersInSourceLocalBufferPool'], y=df_changingBufferSize['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=1),
               name="Throughput in MB/s",
               legendgroup='group2',
               showlegend=False
               ),
    row=2, col=3
)
fig.update_xaxes(title_text="NumberOfBufferInLocalBufferPool", row=2, col=3)

fig.add_trace(
    go.Scatter(x=df_changingLocalBufferSize['NumberOfBuffersInSourceLocalBufferPool'], y=df_changingBufferSize['AvgLatencyInMs'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=1),
               name="Average Latency",
               legendgroup='group3',
               showlegend=False
               ),
    row=3, col=3
)
fig.update_xaxes(title_text="NumberOfBufferInLocalBufferPool", row=3, col=3)

####################################################################################################

############################################# df_changingSourceCnt #######################################################
fig.add_trace(
    go.Scatter(x=df_changingSourceCnt['SourceCnt'], y=df_changingBufferSize['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=1),
               name="Throughput in Tuples/sec",
               legendgroup='group1',
               showlegend=False
               ),
    row=1, col=4
)
fig.update_xaxes(title_text="SourceCnt", row=1, col=4)

fig.add_trace(
    go.Scatter(x=df_changingSourceCnt['SourceCnt'], y=df_changingBufferSize['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=1),
               name="Throughput in MB/s",
               legendgroup='group2',
               showlegend=False
               ),
    row=2, col=4
)
fig.update_xaxes(title_text="SourceCnt", row=2, col=4)

fig.add_trace(
    go.Scatter(x=df_changingSourceCnt['SourceCnt'], y=df_changingBufferSize['AvgLatencyInMs'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=1),
               name="Average Latency",
               legendgroup='group3',
               showlegend=False
               ),
    row=3, col=4
)
fig.update_xaxes(title_text="SourceCnt", row=3, col=4)

####################################################################################################

########################################## df_changingThreadsCnt ##########################################################
fig.add_trace(
    go.Scatter(x=df_changingThreadsCnt['WorkerThreads'], y=df_changingThreadsCnt['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=1),
               name="Throughput in Tuples/sec",
               legendgroup='group1',
               showlegend=False
               ),
    row=1, col=5
)
fig.update_xaxes(title_text="WorkerThreads", row=1, col=5)

fig.add_trace(
    go.Scatter(x=df_changingThreadsCnt['WorkerThreads'], y=df_changingThreadsCnt['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=1),
               name="Throughput in MB/s",
               legendgroup='group2',
               showlegend=False
               ),
    row=2, col=5
)
fig.update_xaxes(title_text="WorkerThreads", row=2, col=5)

fig.add_trace(
    go.Scatter(x=df_changingThreadsCnt['WorkerThreads'], y=df_changingThreadsCnt['AvgLatencyInMs'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=1),
               name="Average Latency",
               legendgroup='group3',
               showlegend=False
               ),
    row=3, col=5
)
fig.update_xaxes(title_text="WorkerThreads", row=3, col=5)


############################################# df_changingThreadsAndSource #######################################################

df_changingThreadsAndSource["src"] = "W" + df_changingThreadsAndSource["WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSource["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSource['src'], y=df_changingThreadsAndSource['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=1),
               name="Throughput in Tuples/sec",
               legendgroup='group1',
               showlegend=False
               ),
    row=1, col=6
)
fig.update_xaxes(title_text="Worker Threads & SourceCnt", row=1, col=6)

fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSource['src'], y=df_changingThreadsAndSource['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=1),
               name="ThroughputInMBPerSec",
               legendgroup='group2',
               showlegend=False
               ),
    row=2, col=6
)
fig.update_xaxes(title_text="Worker Threads & SourceCnt", row=2, col=6)

df_changingThreadsAndSource["src"] = "W" + df_changingThreadsAndSource["WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSource["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSource['src'], y=df_changingThreadsAndSource['AvgLatencyInMs'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=1),
               name="AvgLatencyInMs",
               legendgroup='group3',
               showlegend=False
               ),
    row=3, col=6
)
fig.update_xaxes(title_text="Worker Threads & SourceCnt", row=3, col=6)


############################################# df_changingThreadsAndSourceOnlyFwdData #######################################################

df_changingThreadsAndSourceOnlyFwdData["src"] = "W" + df_changingThreadsAndSourceOnlyFwdData["WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceOnlyFwdData["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceOnlyFwdData['src'], y=df_changingThreadsAndSourceOnlyFwdData['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=1),
               name="Throughput in Tuples/sec",
               legendgroup='group1',
               showlegend=False
               ),
    row=1, col=7
)
fig.update_xaxes(title_text="Worker Threads & SourceCnt", row=1, col=7)

fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceOnlyFwdData['src'], y=df_changingThreadsAndSourceOnlyFwdData['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=1),
               name="ThroughputInMBPerSec",
               legendgroup='group2',
               showlegend=False
               ),
    row=2, col=7
)
fig.update_xaxes(title_text="Worker Threads & SourceCnt", row=2, col=7)

df_changingThreadsAndSourceOnlyFwdData["src"] = "W" + df_changingThreadsAndSourceOnlyFwdData["WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceOnlyFwdData["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceOnlyFwdData['src'], y=df_changingThreadsAndSourceOnlyFwdData['AvgLatencyInMs'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=1),
               name="AvgLatencyInMs",
               legendgroup='group3',
               showlegend=False
               ),
    row=3, col=7
)
fig.update_xaxes(title_text="Worker Threads & SourceCnt", row=3, col=7)

####################################################################################################
fig.update_layout(barmode='overlay')
fig.update_layout(
    title={
        'text': "<b>NebulaStream Performance Numbers </b><br>"
                "<span style=\"font-size:0.6em\"> Default Config(GlobalBufferPool=65536, LocalBufferPool=1024, BufferSize=4MB, Threads=8, Sources=4, TupleSize=24Byte, Query: Filter with 50% Selectivity)"
                "<br>",
        'y': 0.98,
        'x': 0.5,
        'xanchor': 'center',
        'yanchor': 'top'},
)
fig.update_layout(
    title_font_family="Times New Roman",
    title_font_color="RebeccaPurple",
    title_font_size=30
)

fig.update_layout(
    font=dict(
        family="Courier New, monospace",
        size=12,
        color="RebeccaPurple")
)

# ###############################################################


plotly.offline.plot(fig, filename='results.html')
