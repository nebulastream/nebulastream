import plotly.graph_objs as go
import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
import plotly
import pandas as pd
import numpy as np
from matplotlib.pyplot import *
import matplotlib.pyplot as plt
from itertools import permutations
from random import sample
import numpy as np

# set this if you run with DNES_BENCHMARKS_DETAILED_LATENCY_MEASUREMENT

withLatencyHistogram = False 

# folder = "./"#in this folder
folder = "./"
df_changingBufferSize = pd.read_csv(folder + 'changingBufferSize.csv')
df_changingGlobalBufferCnt = pd.read_csv(folder + 'changingGlobalBufferCnt.csv')
df_changingLocalBufferSize = pd.read_csv(folder + 'changingLocalBufferSize.csv')

df_changingSourceCnt = pd.read_csv(folder + 'changingSourceCnt.csv')
df_chaningWorkerCnt = pd.read_csv(folder + 'changingWorkerCnt.csv')

# df_changingThreadsAndSourceMedSelectivity = pd.read_csv(folder + 'changingThreadsAndSourceMedSelectivity.csv')
# df_changingThreadsAndSourceHighSelectivity = pd.read_csv(folder + 'changingThreadsAndSourceHighSelectivity.csv')
# df_changingThreadsAndSourceLowSelectivity = pd.read_csv(folder + 'changingThreadsAndSourceLowSelectivity.csv')
# df_changingThreadsAndSourceNoProc = pd.read_csv(folder + 'changingThreadsAndSourceNoProc.csv')
df_scalingLarge = pd.read_csv(folder + 'scalingLarge.csv')

if (withLatencyHistogram == True):
    df_latencyWrk1 = pd.read_csv(folder + 'latencyW1.log')
    df_latencyWrk2 = pd.read_csv(folder + 'latencyW2.log')
    df_latencyWrk4 = pd.read_csv(folder + 'latencyW4.log')
    df_latencyWrk8 = pd.read_csv(folder + 'latencyW8.log')
    df_latencyWrk12 = pd.read_csv(folder + 'latencyW12.log')

import plotly.graph_objects as go
import plotly

# ###############################################################
fig = make_subplots(
    rows=4, cols=10,
    column_widths=[3, 3, 3, 3, 3, 3, 3, 3, 3, 3],
    row_heights=[1., 1., 1., 4],
    shared_xaxes=False,
    subplot_titles=[
        'Sc. Buffer Size',
        'Sc. Global Buffer Cnt',
        'Sc. Local Buffer Cnt',
        'Sc. Source Cnt',
        'Sc. Worker Cnt',
        'Sc. Wrk/Src -- NoProc',
        'Sc. Wrk/Src -- Low Sel',
        'Sc. Wrk/Src -- Med Sel',
        'Sc. Wrk/Src -- High Sel',
        'Wrk/Src Large Scale Exp',
        'Sc. Buffer Size',
        'Sc. Global Buffer Cnt',
        'Sc. Local Buffer Cnt',
        'Sc. Source Cnt',
        'Sc. Worker Cnt',
        'Sc. Wrk/Src -- NoProc',
        'Sc. Wrk/Src -- Low Sel',
        'Sc. Wrk/Src -- Med Sel',
        'Sc. Wrk/Src -- High Sel',
        'Wrk/Src Large Scale Exp',
        'Sc. Buffer Size',
        'Sc. Global Buffer Cnt',
        'Sc. Local Buffer Cnt',
        'Sc. Source Cnt',
        'Sc. Worker Cnt',
        'Sc. Wrk/Src -- NoProc',
        'Sc. Wrk/Src -- Low Sel',
        'Sc. Wrk/Src -- Med Sel',
        'Sc. Wrk/Src -- High Sel',
        'Wrk/Src Large Scale Exp',
        'Latency TimeSeries <br> 1Wrk,NSrc',
        'Latency TimeSeries <br> 2Wrk,NSrc',
        'Latency TimeSeries <br> 4Wrk,NSrc',
        'Latency TimeSeries <br> 8Wrk,NSrc',
        'Latency TimeSeries <br> 12Wrk,NSrc',
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

# ########################################## df_changingBufferSize ##########################################################
df_changingBufferSize_pivot = pd.pivot_table(df_changingBufferSize, values='ThroughputInTupsPerSec',
                                             index=['BufferSizeInBytes'], columns='WorkerThreads',
                                             aggfunc=np.sum)

for i in range(len(df_changingBufferSize_pivot.columns)):
    col = df_changingBufferSize_pivot.columns[i]
    fig.add_trace(go.Scatter(x=df_changingBufferSize_pivot.index, y=df_changingBufferSize_pivot[col].values,
                             name=str(col),
                             # log_x=True,
                             hoverinfo='x+y',
                             mode='markers+lines',
                             line=dict(shape='linear',
                                       color=dictOfNames[col],
                                       width=2),
                             connectgaps=True,
                             showlegend=False,
                             ),
                  row=1, col=1,
                  )
fig.update_xaxes(title_text="BufferSizeInBytes", type="log", row=1, col=1)
fig.update_yaxes(title_text="ThroughputInTupsPerSec", type="log", row=1, col=1)

df_changingBufferSize_pivot = pd.pivot_table(df_changingBufferSize, values='ThroughputInMBPerSec',
                                             index=['BufferSizeInBytes'], columns='WorkerThreads',
                                             aggfunc=np.sum)
for i in range(len(df_changingBufferSize_pivot.columns)):
    col = df_changingBufferSize_pivot.columns[i]
    fig.add_trace(go.Scatter(x=df_changingBufferSize_pivot.index, y=df_changingBufferSize_pivot[col].values,
                             # name=str(col),
                             hoverinfo='x+y',
                             mode='markers+lines',
                             line=dict(shape='linear', color=dictOfNames[col],
                                       width=2),
                             connectgaps=True,
                             showlegend=False,
                             ),
                  row=2, col=1
                  )
fig.update_xaxes(title_text="BufferSizeInBytes", type="log", row=2, col=1)
fig.update_yaxes(title_text="ThroughputInMBPerSec", type="log", row=2, col=1)

df_changingBufferSize_pivot = pd.pivot_table(df_changingBufferSize, values='AvgLatencyInMs',
                                             index=['BufferSizeInBytes'], columns='WorkerThreads',
                                             aggfunc=np.sum)
for i in range(len(df_changingBufferSize_pivot.columns)):
    col = df_changingBufferSize_pivot.columns[i]
    fig.add_trace(go.Scatter(x=df_changingBufferSize_pivot.index, y=df_changingBufferSize_pivot[col].values,
                             # name=str(col),
                             hoverinfo='x+y',
                             mode='markers+lines',
                             line=dict(shape='linear', color=dictOfNames[col],
                                       width=2),
                             connectgaps=True,
                             showlegend=False,
                             ),
                  row=3, col=1
                  )
fig.update_xaxes(title_text="BufferSizeInBytes", type="log", row=3, col=1)
fig.update_yaxes(title_text="AvgLatencyInMs", type="log", row=3, col=1)

# ########################################## df_changingGlobalBufferCnt ##########################################################
df_changingGlobalBufferCnt_pivot = pd.pivot_table(df_changingGlobalBufferCnt, values='ThroughputInTupsPerSec',
                                                  index=['NumberOfBuffersInGlobalBufferManager'],
                                                  columns='WorkerThreads',
                                                  aggfunc=np.sum)
for i in range(len(df_changingGlobalBufferCnt_pivot.columns)):
    col = df_changingGlobalBufferCnt_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingGlobalBufferCnt_pivot.index, y=df_changingGlobalBufferCnt_pivot[col].values,
                   # name=str( col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False,
                   ),
        row=1, col=2
    )
fig.update_xaxes(title_text="GlobalBufferCnt", type="log", row=1, col=2)
fig.update_yaxes(type="log", row=1, col=2)

df_changingGlobalBufferCnt_pivot = pd.pivot_table(df_changingGlobalBufferCnt, values='ThroughputInMBPerSec',
                                                  index=['NumberOfBuffersInGlobalBufferManager'],
                                                  columns='WorkerThreads',
                                                  aggfunc=np.sum)

for i in range(len(df_changingGlobalBufferCnt_pivot.columns)):
    col = df_changingGlobalBufferCnt_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingGlobalBufferCnt_pivot.index, y=df_changingGlobalBufferCnt_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False,
                   ),
        row=2, col=2
    )
fig.update_xaxes(title_text="GlobalBufferCnt", type="log", row=2, col=2)
fig.update_yaxes(type="log", row=2, col=2)

df_changingGlobalBufferCnt_pivot = pd.pivot_table(df_changingGlobalBufferCnt, values='AvgLatencyInMs',
                                                  index=['NumberOfBuffersInGlobalBufferManager'],
                                                  columns='WorkerThreads',
                                                  aggfunc=np.sum)
for i in range(len(df_changingGlobalBufferCnt_pivot.columns)):
    col = df_changingGlobalBufferCnt_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingGlobalBufferCnt_pivot.index, y=df_changingGlobalBufferCnt_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False,
                   ),
        row=3, col=2
    )
fig.update_xaxes(title_text="GlobalBufferCnt", type="log", row=3, col=2)
fig.update_yaxes(type="log", row=3, col=2)

# ########################################## df_changingLocalBufferSize ##########################################################
df_changingLocalBufferSize_pivot = pd.pivot_table(df_changingLocalBufferSize, values='ThroughputInTupsPerSec',
                                                  index=['NumberOfBuffersInSourceLocalBufferPool'],
                                                  columns='WorkerThreads',
                                                  aggfunc=np.sum)
for i in range(len(df_changingLocalBufferSize_pivot.columns)):
    col = df_changingLocalBufferSize_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingLocalBufferSize_pivot.index, y=df_changingLocalBufferSize_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False,
                   ),
        row=1, col=3
    )
fig.update_xaxes(title_text="LocalBufferCnt", type="log", row=1, col=3)
fig.update_yaxes(type="log", row=1, col=3)

df_changingLocalBufferSize_pivot = pd.pivot_table(df_changingLocalBufferSize, values='ThroughputInMBPerSec',
                                                  index=['NumberOfBuffersInSourceLocalBufferPool'],
                                                  columns='WorkerThreads',
                                                  aggfunc=np.sum)
for i in range(len(df_changingLocalBufferSize_pivot.columns)):
    col = df_changingLocalBufferSize_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingLocalBufferSize_pivot.index, y=df_changingLocalBufferSize_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False,
                   ),
        row=2, col=3
    )
fig.update_xaxes(title_text="LocalBufferCnt", type="log", row=2, col=3)
fig.update_yaxes(type="log", row=2, col=3)

df_changingLocalBufferSize_pivot = pd.pivot_table(df_changingLocalBufferSize, values='AvgLatencyInMs',
                                                  index=['NumberOfBuffersInSourceLocalBufferPool'],
                                                  columns='WorkerThreads',
                                                  aggfunc=np.sum)

for i in range(len(df_changingLocalBufferSize_pivot.columns)):
    col = df_changingLocalBufferSize_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingLocalBufferSize_pivot.index, y=df_changingLocalBufferSize_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False,
                   ),
        row=3, col=3
    )
fig.update_xaxes(title_text="LocalBufferCnt", row=3, col=3)
fig.update_yaxes(row=3, col=3)

# ########################################## df_changingSourceCnt ##########################################################
df_changingSourceCnt_pivot = pd.pivot_table(df_changingSourceCnt, values='ThroughputInTupsPerSec',
                                            index=['SourceCnt'],
                                            columns='WorkerThreads',
                                            aggfunc=np.sum)

for i in range(len(df_changingSourceCnt_pivot.columns)):
    col = df_changingSourceCnt_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCnt_pivot.index, y=df_changingSourceCnt_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=1, col=4
    )
fig.update_xaxes(title_text="SourceCnt", row=1, col=4)

df_changingSourceCnt_pivot = pd.pivot_table(df_changingSourceCnt, values='ThroughputInMBPerSec',
                                            index=['SourceCnt'],
                                            columns='WorkerThreads',
                                            aggfunc=np.sum)
for i in range(len(df_changingSourceCnt_pivot.columns)):
    col = df_changingSourceCnt_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCnt_pivot.index, y=df_changingSourceCnt_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=2, col=4
    )
fig.update_xaxes(title_text="SourceCnt", row=2, col=4)

df_changingSourceCnt_pivot = pd.pivot_table(df_changingSourceCnt, values='AvgLatencyInMs',
                                            index=['SourceCnt'],
                                            columns='WorkerThreads',
                                            aggfunc=np.sum)
for i in range(len(df_changingSourceCnt_pivot.columns)):
    col = df_changingSourceCnt_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCnt_pivot.index, y=df_changingSourceCnt_pivot[col].values,
                   name=str('Wrk' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=True
                   ),
        row=3, col=4
    )
fig.update_xaxes(title_text="SourceCnt", row=3, col=4)

# ########################################## df_chaningWorkerCnt ##########################################################
df_chaningWorkerCnt_pivot = pd.pivot_table(df_chaningWorkerCnt, values='ThroughputInTupsPerSec',
                                           index=['WorkerThreads'],
                                           columns='SourceCnt',
                                           aggfunc=np.sum)

for i in range(len(df_chaningWorkerCnt_pivot.columns)):
    col = df_chaningWorkerCnt_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_chaningWorkerCnt_pivot.index, y=df_chaningWorkerCnt_pivot[col].values,
                   name=str('Src' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=True
                   ),
        row=1, col=5
    )
fig.update_xaxes(title_text="WorkerCnt <br>Legend=Src-X", row=1, col=5)

df_chaningWorkerCnt_pivot = pd.pivot_table(df_chaningWorkerCnt, values='ThroughputInMBPerSec',
                                           index=['WorkerThreads'],
                                           columns='SourceCnt',
                                           aggfunc=np.sum)
for i in range(len(df_chaningWorkerCnt_pivot.columns)):
    col = df_chaningWorkerCnt_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_chaningWorkerCnt_pivot.index, y=df_chaningWorkerCnt_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=2, col=5
    )
fig.update_xaxes(title_text="WorkerCnt <br>Legend=Src-X", row=2, col=5)

df_chaningWorkerCnt_pivot = pd.pivot_table(df_chaningWorkerCnt, values='AvgLatencyInMs',
                                           index=['WorkerThreads'],
                                           columns='SourceCnt',
                                           aggfunc=np.sum)
for i in range(len(df_chaningWorkerCnt_pivot.columns)):
    col = df_chaningWorkerCnt_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_chaningWorkerCnt_pivot.index, y=df_chaningWorkerCnt_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=3, col=5
    )
fig.update_xaxes(title_text="WorkerCnt <br>Legend=Src-X", row=3, col=5)

############################################# df_changingThreadsAndSourceNoProc #######################################################

df_changingThreadsAndSourceNoProc["src"] = "W" + df_changingThreadsAndSourceNoProc["WorkerThreads"].astype(str) + '/S' + \
                                           df_changingThreadsAndSourceNoProc["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceNoProc['src'],
               y=df_changingThreadsAndSourceNoProc['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=6
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt NoProc", row=1, col=6)

fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceNoProc['src'], y=df_changingThreadsAndSourceNoProc['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=6
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt NoProc", row=2, col=6)

df_changingThreadsAndSourceNoProc["src"] = "W" + df_changingThreadsAndSourceNoProc["WorkerThreads"].astype(str) + '/S' + \
                                           df_changingThreadsAndSourceNoProc["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceNoProc['src'], y=df_changingThreadsAndSourceNoProc['AvgLatencyInMs'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgLatencyInMs",
               showlegend=False
               ),
    row=3, col=6
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt NoProc", row=3, col=6)

############################################# df_changingThreadsAndSourceLowSelectivity #######################################################

df_changingThreadsAndSourceLowSelectivity["src"] = "W" + df_changingThreadsAndSourceLowSelectivity[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceLowSelectivity["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceLowSelectivity['src'],
               y=df_changingThreadsAndSourceLowSelectivity['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=7
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt (10%)", row=1, col=7)

fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceLowSelectivity['src'],
               y=df_changingThreadsAndSourceLowSelectivity['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=7
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt (10%)", row=2, col=7)

df_changingThreadsAndSourceLowSelectivity["src"] = "W" + df_changingThreadsAndSourceLowSelectivity[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceLowSelectivity["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceLowSelectivity['src'],
               y=df_changingThreadsAndSourceLowSelectivity['AvgLatencyInMs'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgLatencyInMs",
               showlegend=False
               ),
    row=3, col=7
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt (10%)", row=3, col=7)

####################################################################################################


############################################# df_changingThreadsAndSourceMedSelectivity #######################################################

df_changingThreadsAndSourceMedSelectivity["src"] = "W" + df_changingThreadsAndSourceMedSelectivity[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceMedSelectivity["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceMedSelectivity['src'],
               y=df_changingThreadsAndSourceMedSelectivity['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=8
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt (50%)", row=1, col=8)

fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceMedSelectivity['src'],
               y=df_changingThreadsAndSourceMedSelectivity['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=8
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt (50%)", row=2, col=8)

df_changingThreadsAndSourceMedSelectivity["src"] = "W" + df_changingThreadsAndSourceMedSelectivity[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceMedSelectivity["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceMedSelectivity['src'],
               y=df_changingThreadsAndSourceMedSelectivity['AvgLatencyInMs'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgLatencyInMs",
               showlegend=False
               ),
    row=3, col=8
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt (50%)", row=3, col=8)

####################################################################################################


############################################# df_changingThreadsAndSourceHighSelectivity #######################################################

df_changingThreadsAndSourceHighSelectivity["src"] = "W" + df_changingThreadsAndSourceHighSelectivity[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceHighSelectivity["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceHighSelectivity['src'],
               y=df_changingThreadsAndSourceHighSelectivity['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=9
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt (90%)", row=1, col=9)

fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceHighSelectivity['src'],
               y=df_changingThreadsAndSourceHighSelectivity['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=9
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt (90%)", row=2, col=9)

df_changingThreadsAndSourceHighSelectivity["src"] = "W" + df_changingThreadsAndSourceHighSelectivity[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceHighSelectivity["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceHighSelectivity['src'],
               y=df_changingThreadsAndSourceHighSelectivity['AvgLatencyInMs'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgLatencyInMs",
               showlegend=False
               ),
    row=3, col=9
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt (90%)", row=3, col=9)

############################################# df_scalingLarge #######################################################

df_scalingLarge["src"] = "W" + df_scalingLarge[
    "WorkerThreads"].astype(str) + '/S' + df_scalingLarge["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_scalingLarge['src'],
               y=df_scalingLarge['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=10
)
fig.update_xaxes(title_text="Wrk/Src LC NoProc", row=1, col=10)

fig.add_trace(
    go.Scatter(x=df_scalingLarge['src'],
               y=df_scalingLarge['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=10
)
fig.update_xaxes(title_text="Wrk/Src LC NoProc", row=2, col=10)

fig.add_trace(
    go.Scatter(x=df_scalingLarge['src'],
               y=df_scalingLarge['AvgLatencyInMs'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgLatencyInMs",
               showlegend=False
               ),
    row=3, col=10
)
fig.update_xaxes(title_text="Wrk/Src LC NoProc", row=3, col=10)

########################################## Latency measurements ##########################################################
if (withLatencyHistogram == True):
    df_latencyWrk1_pivot = pd.pivot_table(df_latencyWrk1, values='latency',
                                          index=['ts'],
                                          columns='SourceCnt',
                                          aggfunc=np.sum)
    for i in range(len(df_latencyWrk1_pivot.columns)):
        col = df_latencyWrk1_pivot.columns[i]
        fig.add_trace(
            go.Scatter(x=df_latencyWrk1_pivot.index, y=df_latencyWrk1_pivot[col].values,
                       name=str('Src' + str(col)),
                       hoverinfo='x+y',
                       mode='markers+lines',
                       line=dict(shape='linear', color=dictOfNamesSrc[col],
                                 width=2),
                       connectgaps=True,
                       showlegend=False,
                       ),
            row=4, col=1
        )
    fig.update_xaxes(title_text="ts (Wrk 1)", row=4, col=1)
    fig.update_yaxes(title_text="Latency in MS <br>Legend=Src-X", row=4, col=1)

    fig.add_shape(go.layout.Shape(
        type="line",
        yref="paper",
        xref="x",
        x0=50,
        # y0=0,
        y0=df_latencyWrk1_pivot.index.min() * 1.2,
        x1=50,
        y1=df_latencyWrk1['latency'].max() * 1.2,
        line=dict(color="blue", width=2),
    )
        , row=4, col=1)

    df_latencyWrk2_pivot = pd.pivot_table(df_latencyWrk2, values='latency',
                                      index=['ts'],
                                      columns='SourceCnt',
                                      aggfunc=np.sum)
    for i in range(len(df_latencyWrk2_pivot.columns)):
        col = df_latencyWrk2_pivot.columns[i]
        fig.add_trace(
            go.Scatter(x=df_latencyWrk2_pivot.index, y=df_latencyWrk2_pivot[col].values,
                       name=str('Src' + str(col)),
                       hoverinfo='x+y',
                       mode='markers+lines',
                       line=dict(shape='linear', color=dictOfNamesSrc[col],
                                 width=2),
                       connectgaps=True,
                       showlegend=False
                       ),
            row=4, col=2
        )
    fig.update_xaxes(title_text="ts (Wrk 2)", row=4, col=2)

    fig.add_shape(go.layout.Shape(
        type="line",
        yref="paper",
        xref="x",
        x0=50,
        # y0=0,
        y0=df_latencyWrk2_pivot.index.min() * 1.2,
        x1=50,
        y1=df_latencyWrk2['latency'].max() * 1.2,
        line=dict(color="blue", width=2),
    )
        , row=4, col=2)

    # fig.add_annotation(go.layout.Annotation(
    #     x=50,
    #     y=df_latencyWrk2['latency'].max() * 1.2,
    #     yref='paper',
    #     showarrow=True,
    #     text='Start'
    # )
    #     , row=4, col=2)

    df_latencyWrk4_pivot = pd.pivot_table(df_latencyWrk4, values='latency',
                                          index=['ts'],
                                          columns='SourceCnt',
                                          aggfunc=np.sum)
    for i in range(len(df_latencyWrk4_pivot.columns)):
        col = df_latencyWrk4_pivot.columns[i]
        fig.add_trace(
            go.Scatter(x=df_latencyWrk4_pivot.index, y=df_latencyWrk4_pivot[col].values,
                       name=str('Src' + str(col)),
                       hoverinfo='x+y',
                       mode='markers+lines',
                       line=dict(shape='linear', color=dictOfNamesSrc[col],
                                 width=2),
                       connectgaps=True,
                       showlegend=False
                       ),
            row=4, col=3
        )
    fig.update_xaxes(title_text="ts (Wrk 4)", row=4, col=3)
    fig.add_shape(go.layout.Shape(
        type="line",
        yref="paper",
        xref="x",
        x0=50,
        # y0=0,
        y0=df_latencyWrk4_pivot.index.min() * 1.2,
        x1=50,
        y1=df_latencyWrk4['latency'].max() * 1.2,
        line=dict(color="blue", width=2),
    )
        , row=4, col=3)

    # fig.add_annotation(go.layout.Annotation(
    #     x=50,
    #     y=df_latencyWrk4['latency'].max() * 1.2,
    #     yref='paper',
    #     showarrow=True,
    #     text='Start'
    # )
    #     , row=4, col=3)

    df_latencyWrk8_pivot = pd.pivot_table(df_latencyWrk8, values='latency',
                                          index=['ts'],
                                          columns='SourceCnt',
                                          aggfunc=np.sum)
    for i in range(len(df_latencyWrk8_pivot.columns)):
        col = df_latencyWrk8_pivot.columns[i]
        fig.add_trace(
            go.Scatter(x=df_latencyWrk8_pivot.index, y=df_latencyWrk8_pivot[col].values,
                       name=str('Src' + str(col)),
                       hoverinfo='x+y',
                       mode='markers+lines',
                       line=dict(shape='linear', color=dictOfNamesSrc[col],
                                 width=2),
                       connectgaps=True,
                       showlegend=False
                       ),
            row=4, col=4
        )
    fig.update_xaxes(title_text="ts (Wrk 8)", row=4, col=4)
    fig.update_yaxes(row=4, col=4)  # type="log",
    fig.add_shape(go.layout.Shape(
        type="line",
        yref="paper",
        xref="x",
        x0=50,
        # y0=0,
        y0=df_latencyWrk8_pivot.index.min() * 1.2,
        x1=50,
        y1=df_latencyWrk8['latency'].max() * 1.2,
        line=dict(color="blue", width=2),
    )
        , row=4, col=4)

    # fig.add_annotation(go.layout.Annotation(
    #     x=50,
    #     y=df_latencyWrk8['latency'].max() * 1.2,
    #     yref='paper',
    #     showarrow=True,
    #     text='Start'
    # )
    #     , row=4, col=4)


    df_latencyWrk12_pivot = pd.pivot_table(df_latencyWrk12, values='latency',
                                          index=['ts'],
                                          columns='SourceCnt',
                                          aggfunc=np.sum)
    for i in range(len(df_latencyWrk12_pivot.columns)):
        col = df_latencyWrk12_pivot.columns[i]
        fig.add_trace(
            go.Scatter(x=df_latencyWrk12_pivot.index, y=df_latencyWrk12_pivot[col].values,
                       name=str('Src' + str(col)),
                       hoverinfo='x+y',
                       mode='markers+lines',
                       line=dict(shape='linear', color=dictOfNamesSrc[col],
                                 width=2),
                       connectgaps=True,
                       showlegend=False
                       ),
            row=4, col=5
        )
    fig.update_xaxes(title_text="ts (Wrk 12)", row=4, col=5)
    fig.update_yaxes(row=4, col=5)  # type="log",
    fig.add_shape(go.layout.Shape(
        type="line",
        yref="paper",
        xref="x",
        x0=50,
        # y0=0,
        y0=df_latencyWrk8_pivot.index.min() * 1.2,
        x1=50,
        y1=df_latencyWrk8['latency'].max() * 1.2,
        line=dict(color="blue", width=2),
    )
        , row=4, col=5)

# fig.add_annotation(go.layout.Annotation(
#     x=50,
#     y=df_latencyWrk8['latency'].max() * 1.2,
#     yref='paper',
#     showarrow=True,
#     text='Start'
# )
#     , row=4, col=4)

fig.update_layout(barmode='overlay')
fig.update_layout(
    title={
        'text': "<b>NebulaStream Performance Numbers </b><br>"
                "<span style=\"font-size:0.6em\" line-height=1em;margin=-4px>Default Config(GlobalBufferPool=65536, LocalBufferPool=1024, BufferSize=4MB, Threads=1, Source=1, TupleSize=24Byte, Query: Only Forward)"
                "<br>",
        'y': 0.98,
        'x': 0.5,
        'xanchor': 'center',
        'yanchor': 'top'},
)
fig.update_layout(
    width=2500, height=1500,
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
plotly.offline.plot(fig, filename='results.html')
