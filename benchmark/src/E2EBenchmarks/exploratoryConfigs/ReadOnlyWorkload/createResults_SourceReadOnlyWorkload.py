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

# folder = "./"#in this folder
folder = "./"
df_changingBufferSize = pd.read_csv(folder + 'changingBufferSizeReadOnly.csv')
df_changingGlobalBufferCnt = pd.read_csv(folder + 'changingGlobalBufferCntReadOnly.csv')
df_changingLocalBufferSize = pd.read_csv(folder + 'changingLocalBufferSizeReadOnly.csv')

df_changingSourceCnt = pd.read_csv(folder + 'changingSourceCntReadOnly.csv')
df_chaningWorkerCnt = pd.read_csv(folder + 'changingWorkerCntReadOnly.csv')
df_changingThreadsAndSourceNoProc = pd.read_csv(folder + 'changingThreadsAndSourceNoProcReadOnly.csv')
df_scalingLarge = pd.read_csv(folder + 'scalingLargeReadOnly.csv')


import plotly.graph_objects as go
import plotly

# ###############################################################
fig = make_subplots(
    rows=4, cols=7,
    column_widths=[3, 3, 3, 3, 3, 3, 3],
    row_heights=[1, 1, 1, 1],
    shared_xaxes=False,
    subplot_titles=[
        'Sc. Buffer Size',
        'Sc. Global Buffer Cnt',
        'Sc. Local Buffer Cnt',
        'Sc. Source Cnt',
        'Sc. Worker Cnt',
        'Sc. Wrk/Src equal',
        'Sc. Wrk/Src Large',
        'Sc. Buffer Size',
        'Sc. Global Buffer Cnt',
        'Sc. Local Buffer Cnt',
        'Sc. Source Cnt',
        'Sc. Worker Cnt',
        'Sc. Wrk/Src equal',
        'Sc. Wrk/Src Large',
        'Sc. Buffer Size',
        'Sc. Global Buffer Cnt',
        'Sc. Local Buffer Cnt',
        'Sc. Source Cnt',
        'Sc. Worker Cnt',
        'Sc. Wrk/Src equal',
        'Sc. Wrk/Src Large'
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

df_changingBufferSize_pivot = pd.pivot_table(df_changingBufferSize, values='ThroughputInTasksPerSec',
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
                  row=2, col=1,
                  )
fig.update_xaxes(title_text="BufferSizeInBytes", type="log", row=2, col=1)
fig.update_yaxes(title_text="ThroughputInTasksPerSec", type="log", row=2, col=1)

df_changingBufferSize_pivot = pd.pivot_table(df_changingBufferSize, values='ThroughputInTasksPerSec',
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
fig.update_yaxes(title_text="ThroughputInMBPerSec", type="log", row=3, col=1)

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
                  row=4, col=1
                  )
fig.update_xaxes(title_text="BufferSizeInBytes", type="log", row=4, col=1)
fig.update_yaxes(title_text="AvgLatencyInMs", type="log", row=4, col=1)

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

df_changingGlobalBufferCnt_pivot = pd.pivot_table(df_changingGlobalBufferCnt, values='ThroughputInTasksPerSec',
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
        row=2, col=2
    )
fig.update_xaxes(title_text="GlobalBufferCnt", type="log", row=2, col=2)
fig.update_yaxes(type="log", row=2, col=2)

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
        row=3, col=2
    )
fig.update_xaxes(title_text="GlobalBufferCnt", type="log", row=3, col=2)
fig.update_yaxes(type="log", row=3, col=2)

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
        row=4, col=2
    )
fig.update_xaxes(title_text="GlobalBufferCnt", type="log", row=4, col=2)
fig.update_yaxes(type="log", row=4, col=2)

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

df_changingLocalBufferSize_pivot = pd.pivot_table(df_changingLocalBufferSize, values='ThroughputInTasksPerSec',
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
        row=3, col=3
    )
fig.update_xaxes(title_text="LocalBufferCnt", type="log", row=3, col=3)
fig.update_yaxes(type="log", row=3, col=3)

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
        row=4, col=3
    )
fig.update_xaxes(title_text="LocalBufferCnt", row=4, col=3)
fig.update_yaxes(row=4, col=3)

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

df_changingSourceCnt_pivot = pd.pivot_table(df_changingSourceCnt, values='ThroughputInTasksPerSec',
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
        row=3, col=4
    )
fig.update_xaxes(title_text="SourceCnt", row=3, col=4)

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
        row=4, col=4
    )
fig.update_xaxes(title_text="SourceCnt", row=4, col=4)

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

df_chaningWorkerCnt_pivot = pd.pivot_table(df_chaningWorkerCnt, values='ThroughputInTasksPerSec',
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
        row=2, col=5
    )
fig.update_xaxes(title_text="WorkerCnt <br>Legend=Src-X", row=2, col=5)

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
        row=3, col=5
    )
fig.update_xaxes(title_text="WorkerCnt <br>Legend=Src-X", row=3, col=5)

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
        row=4, col=5
    )
fig.update_xaxes(title_text="WorkerCnt <br>Legend=Src-X", row=4, col=5)

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
fig.update_xaxes(title_text="WrkCnt & SrcCnt", row=1, col=6)

df_changingThreadsAndSourceNoProc["src"] = "W" + df_changingThreadsAndSourceNoProc["WorkerThreads"].astype(str) + '/S' + \
                                           df_changingThreadsAndSourceNoProc["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceNoProc['src'],
               y=df_changingThreadsAndSourceNoProc['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=2, col=6
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt", row=2, col=6)

fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceNoProc['src'], y=df_changingThreadsAndSourceNoProc['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=3, col=6
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt", row=3, col=6)

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
    row=4, col=6
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt", row=4, col=6)

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
    row=1, col=7
)
fig.update_xaxes(title_text="Wrk/Src Large Scale", row=1, col=7)

df_scalingLarge["src"] = "W" + df_scalingLarge[
    "WorkerThreads"].astype(str) + '/S' + df_scalingLarge["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_scalingLarge['src'],
               y=df_scalingLarge['ThroughputInTasksPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=2, col=7
)
fig.update_xaxes(title_text="Wrk/Src Large Scale", row=2, col=7)

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
    row=3, col=7
)
fig.update_xaxes(title_text="Wrk/Src Large Scale", row=3, col=7)

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
    row=4, col=7
)
fig.update_xaxes(title_text="Wrk/Src Large Scale", row=4, col=7)

fig.update_layout(barmode='overlay')
fig.update_layout(
    title={
        'text': "<b>NebulaStream Performance Numbers -- MemoryMode -- ReadOnlyWorkload </b><br>"
                "<span style=\"font-size:0.6em\" line-height=1em;margin=-4px>Default Config(GlobalBufferPool=65536, "
                "LocalBufferPool=1024, BufferSize=4MB, Threads=1, Source=1, TupleSize=24Byte, Query::from(input).filter(Attribute(value) > 10000).sink(NullOutputSinkDescriptor::create());' 0% Selectivity)"
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
plotly.offline.plot(fig, filename='ReadOnlyWorkload.html')
