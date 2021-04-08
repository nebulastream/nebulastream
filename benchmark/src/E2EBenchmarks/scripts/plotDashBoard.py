import plotly.graph_objs as go
import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
import plotly
import pandas as pd
import numpy as np

df_changingBufferSize = pd.read_csv('./changingBufferSize.csv')
df_changingGlobalBufferCnt = pd.read_csv('./changingGlobalBufferCnt.csv')
df_changingLocalBufferSize = pd.read_csv('./changingLocalBufferSize.csv')
df_changingSourceCnt = pd.read_csv('./changingSourceCnt.csv')
df_changingSourceCnt = pd.read_csv('./changingSourceCnt.csv')
SourceToWorkerRatio
df_changingThreadsAndSourceMedSelectivity = pd.read_csv('./changingThreadsAndSourceMedSelectivity.csv')
df_changingThreadsAndSourceHighSelectivity = pd.read_csv('./changingThreadsAndSourceHighSelectivity.csv')
df_changingThreadsAndSourceLowSelectivity = pd.read_csv('./changingThreadsAndSourceLowSelectivity.csv')
df_changingThreadsAndSourceNoProc = pd.read_csv('./changingThreadsAndSourceNoProc.csv')

df_changingThreadsCnt = pd.read_csv('./changingThreadsWithFixSource.csv')


import plotly.graph_objects as go
import plotly

# ###############################################################
fig = make_subplots(
    rows=3, cols=7,
    column_widths=[0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3],
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


# df2.reset_index().to_dict('rows')
#
# ########################################## df_changingBufferSize ##########################################################
df_changingBufferSize_pivot = pd.pivot_table(df_changingBufferSize, values='ThroughputInTupsPerSec',
                                             index=['BufferSizeInBytes'], columns='WorkerThreads',
                                             aggfunc=np.sum)
for col in df_changingBufferSize_pivot.columns:
    fig.add_trace(go.Scatter(x=df_changingBufferSize_pivot.index, y=df_changingBufferSize_pivot[col].values,
                             name=col,
                             hoverinfo='x+y',
                             mode='markers+lines',
                             line=dict(shape='linear', color='red',
                                       width=1),
                             connectgaps=True,
                             showlegend=True
                             ),
                  row=1, col=1
                  )
fig.update_xaxes(title_text="BufferSizeInBytes", row=1, col=1)
fig.update_yaxes(title_text="ThroughputInTupsPerSec", row=1, col=1)

df_changingBufferSize_pivot = pd.pivot_table(df_changingBufferSize, values='ThroughputInMBPerSec',
                                             index=['BufferSizeInBytes'], columns='WorkerThreads',
                                             aggfunc=np.sum)
for col in df_changingBufferSize_pivot.columns:
    fig.add_trace(go.Scatter(x=df_changingBufferSize_pivot.index, y=df_changingBufferSize_pivot[col].values,
                             name=col,
                             hoverinfo='x+y',
                             mode='markers+lines',
                             line=dict(shape='linear', color='blue',
                                       width=1),
                             connectgaps=True,
                             showlegend=True
                             ),
                  row=2, col=1
                  )
fig.update_xaxes(title_text="BufferSizeInBytes", row=2, col=1)
fig.update_yaxes(title_text="ThroughputInMBPerSec", row=2, col=1)

df_changingBufferSize_pivot = pd.pivot_table(df_changingBufferSize, values='AvgLatencyInMs',
                                             index=['BufferSizeInBytes'], columns='WorkerThreads',
                                             aggfunc=np.sum)
for col in df_changingBufferSize_pivot.columns:
    fig.add_trace(go.Scatter(x=df_changingBufferSize_pivot.index, y=df_changingBufferSize_pivot[col].values,
                             name=col,
                             hoverinfo='x+y',
                             mode='markers+lines',
                             line=dict(shape='linear', color='green',
                                       width=1),
                             connectgaps=True,
                             showlegend=True
                             ),
                  row=3, col=1
                  )
    fig.update_xaxes(title_text="BufferSizeInBytes", row=3, col=1)
    fig.update_yaxes(title_text="AvgLatencyInMs", row=3, col=1)

# ########################################## df_changingGlobalBufferCnt ##########################################################
df_changingGlobalBufferCnt_pivot = pd.pivot_table(df_changingGlobalBufferCnt, values='ThroughputInTupsPerSec',
                                                  index=['NumberOfBuffersInGlobalBufferManager'],
                                                  columns='WorkerThreads',
                                                  aggfunc=np.sum)
for col in df_changingGlobalBufferCnt_pivot.columns:
    fig.add_trace(
        go.Scatter(x=df_changingGlobalBufferCnt_pivot.index, y=df_changingGlobalBufferCnt_pivot[col].values,
                   name=col,
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color='red',
                             width=1),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=1, col=2
    )
fig.update_xaxes(title_text="NumberOfBuffersInGlobalBufferManager", row=1, col=2)
fig.update_yaxes(title_text="ThroughputInTupsPerSec", row=1, col=2)

df_changingGlobalBufferCnt_pivot = pd.pivot_table(df_changingGlobalBufferCnt, values='ThroughputInMBPerSec',
                                                  index=['NumberOfBuffersInGlobalBufferManager'],
                                                  columns='WorkerThreads',
                                                  aggfunc=np.sum)
for col in df_changingGlobalBufferCnt_pivot.columns:
    fig.add_trace(
        go.Scatter(x=df_changingGlobalBufferCnt_pivot.index, y=df_changingGlobalBufferCnt_pivot[col].values,
                   name=col,
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color='blue',
                             width=1),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=2, col=2
    )
fig.update_xaxes(title_text="NumberOfBuffersInGlobalBufferManager", row=2, col=2)
fig.update_yaxes(title_text="ThroughputInMBPerSec", row=2, col=2)

df_changingGlobalBufferCnt_pivot = pd.pivot_table(df_changingGlobalBufferCnt, values='AvgLatencyInMs',
                                                  index=['NumberOfBuffersInGlobalBufferManager'],
                                                  columns='WorkerThreads',
                                                  aggfunc=np.sum)
for col in df_changingGlobalBufferCnt_pivot.columns:
    fig.add_trace(
        go.Scatter(x=df_changingGlobalBufferCnt_pivot.index, y=df_changingGlobalBufferCnt_pivot[col].values,
                   name=col,
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color='green',
                             width=1),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=3, col=2
    )
fig.update_xaxes(title_text="NumberOfBuffersInGlobalBufferManager", row=3, col=2)
fig.update_yaxes(title_text="AvgLatencyInMs", row=3, col=2)

# ########################################## df_changingLocalBufferSize ##########################################################
df_changingLocalBufferSize_pivot = pd.pivot_table(df_changingLocalBufferSize, values='ThroughputInTupsPerSec',
                                                  index=['NumberOfBuffersInSourceLocalBufferPool'],
                                                  columns='WorkerThreads',
                                                  aggfunc=np.sum)
for col in df_changingLocalBufferSize_pivot.columns:
    fig.add_trace(
        go.Scatter(x=df_changingLocalBufferSize_pivot.index, y=df_changingLocalBufferSize_pivot[col].values,
                   name=col,
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color='red',
                             width=1),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=1, col=3
    )
fig.update_xaxes(title_text="NumberOfBuffersInSourceLocalBufferPool", row=1, col=3)
fig.update_yaxes(title_text="ThroughputInTupsPerSec", row=1, col=3)

df_changingLocalBufferSize_pivot = pd.pivot_table(df_changingLocalBufferSize, values='ThroughputInMBPerSec',
                                                  index=['NumberOfBuffersInSourceLocalBufferPool'],
                                                  columns='WorkerThreads',
                                                  aggfunc=np.sum)
for col in df_changingLocalBufferSize_pivot.columns:
    fig.add_trace(
        go.Scatter(x=df_changingLocalBufferSize_pivot.index, y=df_changingLocalBufferSize_pivot[col].values,
                   name=col,
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color='blue',
                             width=1),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=2, col=3
    )
fig.update_xaxes(title_text="NumberOfBuffersInSourceLocalBufferPool", row=2, col=3)
fig.update_yaxes(title_text="ThroughputInMBPerSec", row=2, col=3)

df_changingLocalBufferSize_pivot = pd.pivot_table(df_changingLocalBufferSize, values='AvgLatencyInMs',
                                                  index=['NumberOfBuffersInSourceLocalBufferPool'],
                                                  columns='WorkerThreads',
                                                  aggfunc=np.sum)
for col in df_changingLocalBufferSize_pivot.columns:
    fig.add_trace(
        go.Scatter(x=df_changingLocalBufferSize_pivot.index, y=df_changingLocalBufferSize_pivot[col].values,
                   name=col,
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color='green',
                             width=1),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=3, col=3
    )
fig.update_xaxes(title_text="NumberOfBuffersInSourceLocalBufferPool", row=3, col=3)
fig.update_yaxes(title_text="AvgLatencyInMs", row=3, col=3)

# ########################################## df_changingSourceCnt ##########################################################
df_changingSourceCnt_pivot = pd.pivot_table(df_changingSourceCnt, values='ThroughputInTupsPerSec',
                                            index=['SourceCnt'],
                                            columns='WorkerThreads',
                                            aggfunc=np.sum)
for col in df_changingSourceCnt_pivot.columns:
    fig.add_trace(
        go.Scatter(x=df_changingSourceCnt_pivot.index, y=df_changingSourceCnt_pivot[col].values,
                   name=col,
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color='red',
                             width=1),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=1, col=4
    )
fig.update_xaxes(title_text="SourceCnt", row=1, col=4)
fig.update_yaxes(title_text="ThroughputInTupsPerSec", row=1, col=4)

df_changingSourceCnt_pivot = pd.pivot_table(df_changingSourceCnt, values='ThroughputInMBPerSec',
                                            index=['SourceCnt'],
                                            columns='WorkerThreads',
                                            aggfunc=np.sum)
for col in df_changingSourceCnt_pivot.columns:
    fig.add_trace(
        go.Scatter(x=df_changingSourceCnt_pivot.index, y=df_changingSourceCnt_pivot[col].values,
                   name=col,
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color='blue',
                             width=1),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=2, col=4
    )
fig.update_xaxes(title_text="SourceCnt", row=2, col=4)
fig.update_yaxes(title_text="ThroughputInMBPerSec", row=2, col=4)

df_changingSourceCnt_pivot = pd.pivot_table(df_changingSourceCnt, values='AvgLatencyInMs',
                                            index=['SourceCnt'],
                                            columns='WorkerThreads',
                                            aggfunc=np.sum)
for col in df_changingSourceCnt_pivot.columns:
    fig.add_trace(
        go.Scatter(x=df_changingSourceCnt_pivot.index, y=df_changingSourceCnt_pivot[col].values,
                   name=col,
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color='green',
                             width=1),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=3, col=4
    )
fig.update_xaxes(title_text="SourceCnt", row=3, col=4)
fig.update_yaxes(title_text="AvgLatencyInMs", row=3, col=4)

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

df_changingThreadsAndSourceMedSelectivity["src"] = "W" + df_changingThreadsAndSourceMedSelectivity["WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceMedSelectivity["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceMedSelectivity['src'], y=df_changingThreadsAndSourceMedSelectivity['ThroughputInTupsPerSec'],
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
    go.Scatter(x=df_changingThreadsAndSourceMedSelectivity['src'], y=df_changingThreadsAndSourceMedSelectivity['ThroughputInMBPerSec'],
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

df_changingThreadsAndSourceMedSelectivity["src"] = "W" + df_changingThreadsAndSourceMedSelectivity["WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceMedSelectivity["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceMedSelectivity['src'], y=df_changingThreadsAndSourceMedSelectivity['AvgLatencyInMs'],
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
                "<span style=\"font-size:0.6em\">Default Config(GlobalBufferPool=65536, LocalBufferPool=1024, BufferSize=4MB, Threads=8, Sources=4, TupleSize=24Byte, Query: Filter with 50% Selectivity)"
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

###############################################################

plotly.offline.plot(fig, filename='results.html')
