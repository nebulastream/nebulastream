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
df_changingSourceCntBufferSize128KB = pd.read_csv(folder + 'changingSourceCntBufferSize128KB.csv')
df_changingSourceCntBufferSize16KB = pd.read_csv(folder + 'changingSourceCntBufferSize16KB.csv')
df_changingSourceCntBufferSize1MB = pd.read_csv(folder + 'changingSourceCntBufferSize1MB.csv')
df_changingSourceCntBufferSize4MB = pd.read_csv(folder + 'changingSourceCntBufferSize4MB.csv')
df_changingSourceCntBufferSize512KB = pd.read_csv(folder + 'changingSourceCntBufferSize512KB.csv')
df_changingSourceCntBufferSize64KB = pd.read_csv(folder + 'changingSourceCntBufferSize64KB.csv')

df_changingWorkerCntBufferSize128KB = pd.read_csv(folder + 'changingWorkerCntBufferSize128KB.csv')
df_changingWorkerCntBufferSize16KB = pd.read_csv(folder + 'changingWorkerCntBufferSize16KB.csv')
df_changingWorkerCntBufferSize1MB = pd.read_csv(folder + 'changingWorkerCntBufferSize1MB.csv')
df_changingWorkerCntBufferSize4MB = pd.read_csv(folder + 'changingWorkerCntBufferSize4MB.csv')
df_changingWorkerCntBufferSize512KB = pd.read_csv(folder + 'changingWorkerCntBufferSize512KB.csv')
df_changingWorkerCntBufferSize64KB = pd.read_csv(folder + 'changingWorkerCntBufferSize64KB.csv')
import plotly.graph_objects as go
import plotly

# ###############################################################
fig = make_subplots(
    rows=4, cols=12,
    column_widths=[1,1,1,1,1,1,1,1,1,1,1,1],
    row_heights=[3, 3, 3, 3],
    shared_xaxes=False,
    subplot_titles=[

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
############


# ########################################## df_changingSourceCntBufferSize16KBBufferSize16KB ##########################################################
df_changingSourceCntBufferSize16KB_pivot = pd.pivot_table(df_changingSourceCntBufferSize16KB, values='ThroughputInTupsPerSec',
                                                          index=['SourceCnt'],
                                                          columns='WorkerThreads',
                                                          aggfunc=np.sum)

for i in range(len(df_changingSourceCntBufferSize16KB_pivot.columns)):
    col = df_changingSourceCntBufferSize16KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize16KB_pivot.index, y=df_changingSourceCntBufferSize16KB_pivot[col].values,
                   name=str('Wrk' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=True
                   ),
        row=1, col=1
    )
fig.update_xaxes(title_text="SourceCnt <br>16KB Buffer", row=1, col=1)
fig.update_yaxes(title_text="ThroughputInTupsPerSec", row=1, col=1)

df_changingSourceCntBufferSize16KB_pivot = pd.pivot_table(df_changingSourceCntBufferSize16KB, values='ThroughputInTasksPerSec',
                                                          index=['SourceCnt'],
                                                          columns='WorkerThreads',
                                                          aggfunc=np.sum)

for i in range(len(df_changingSourceCntBufferSize16KB_pivot.columns)):
    col = df_changingSourceCntBufferSize16KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize16KB_pivot.index, y=df_changingSourceCntBufferSize16KB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=2, col=1
    )
fig.update_xaxes(title_text="SourceCnt <br>16KB Buffer", row=2, col=1)
fig.update_yaxes(title_text="ThroughputInTasksPerSec", row=2, col=1)

df_changingSourceCntBufferSize16KB_pivot = pd.pivot_table(df_changingSourceCntBufferSize16KB, values='ThroughputInMBPerSec',
                                                          index=['SourceCnt'],
                                                          columns='WorkerThreads',
                                                          aggfunc=np.sum)
for i in range(len(df_changingSourceCntBufferSize16KB_pivot.columns)):
    col = df_changingSourceCntBufferSize16KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize16KB_pivot.index, y=df_changingSourceCntBufferSize16KB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=3, col=1
    )
fig.update_xaxes(title_text="SourceCnt <br>16KB Buffer", row=3, col=1)
fig.update_yaxes(title_text="ThroughputInMBPerSec", row=3, col=1)

df_changingSourceCntBufferSize16KB_pivot = pd.pivot_table(df_changingSourceCntBufferSize16KB, values='AvgLatencyInMs',
                                                          index=['SourceCnt'],
                                                          columns='WorkerThreads',
                                                          aggfunc=np.sum)
for i in range(len(df_changingSourceCntBufferSize16KB_pivot.columns)):
    col = df_changingSourceCntBufferSize16KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize16KB_pivot.index, y=df_changingSourceCntBufferSize16KB_pivot[col].values,
                   name=str('Wrk' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=4, col=1
    )
fig.update_xaxes(title_text="SourceCnt <br>16KB Buffer", row=4, col=1)
fig.update_yaxes(title_text="AvgLatencyInMs", row=4, col=1)

# ########################################## df_changingWorkerCntBufferSize16KB ##########################################################
df_changingWorkerCntBufferSize16KB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize16KB, values='ThroughputInTupsPerSec',
                                                          index=['WorkerThreads'],
                                                          columns='SourceCnt',
                                                          aggfunc=np.sum)

for i in range(len(df_changingWorkerCntBufferSize16KB_pivot.columns)):
    col = df_changingWorkerCntBufferSize16KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize16KB_pivot.index, y=df_changingWorkerCntBufferSize16KB_pivot[col].values,
                   name=str('Src' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=True
                   ),
        row=1, col=2
    )
fig.update_xaxes(title_text="WorkerCnt <br>16KB Buffer <br>Legend=Src-X", row=1, col=2)

df_changingWorkerCntBufferSize16KB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize16KB, values='ThroughputInTasksPerSec',
                                                          index=['WorkerThreads'],
                                                          columns='SourceCnt',
                                                          aggfunc=np.sum)

for i in range(len(df_changingWorkerCntBufferSize16KB_pivot.columns)):
    col = df_changingWorkerCntBufferSize16KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize16KB_pivot.index, y=df_changingWorkerCntBufferSize16KB_pivot[col].values,
                   name=str('Src' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=2, col=2
    )
fig.update_xaxes(title_text="WorkerCnt <br>16KB Buffer <br>Legend=Src-X", row=2, col=2)

df_changingWorkerCntBufferSize16KB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize16KB, values='ThroughputInMBPerSec',
                                                          index=['WorkerThreads'],
                                                          columns='SourceCnt',
                                                          aggfunc=np.sum)
for i in range(len(df_changingWorkerCntBufferSize16KB_pivot.columns)):
    col = df_changingWorkerCntBufferSize16KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize16KB_pivot.index, y=df_changingWorkerCntBufferSize16KB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=3, col=2
    )
fig.update_xaxes(title_text="WorkerCnt <br>16KB Buffer <br>Legend=Src-X", row=3, col=2)

df_changingWorkerCntBufferSize16KB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize16KB, values='AvgLatencyInMs',
                                                          index=['WorkerThreads'],
                                                          columns='SourceCnt',
                                                          aggfunc=np.sum)
for i in range(len(df_changingWorkerCntBufferSize16KB_pivot.columns)):
    col = df_changingWorkerCntBufferSize16KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize16KB_pivot.index, y=df_changingWorkerCntBufferSize16KB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=4, col=2
    )
fig.update_xaxes(title_text="WorkerCnt <br>16KB Buffer <br>Legend=Src-X", row=4, col=2)


# ########################################## df_changingSourceCntBufferSize64KBBufferSize64KB ##########################################################
df_changingSourceCntBufferSize64KB_pivot = pd.pivot_table(df_changingSourceCntBufferSize64KB, values='ThroughputInTupsPerSec',
                                                          index=['SourceCnt'],
                                                          columns='WorkerThreads',
                                                          aggfunc=np.sum)

for i in range(len(df_changingSourceCntBufferSize64KB_pivot.columns)):
    col = df_changingSourceCntBufferSize64KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize64KB_pivot.index, y=df_changingSourceCntBufferSize64KB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=1, col=3
    )
fig.update_xaxes(title_text="SourceCnt <br>64KB Buffer", row=1, col=3)

df_changingSourceCntBufferSize64KB_pivot = pd.pivot_table(df_changingSourceCntBufferSize64KB, values='ThroughputInTasksPerSec',
                                                          index=['SourceCnt'],
                                                          columns='WorkerThreads',
                                                          aggfunc=np.sum)

for i in range(len(df_changingSourceCntBufferSize64KB_pivot.columns)):
    col = df_changingSourceCntBufferSize64KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize64KB_pivot.index, y=df_changingSourceCntBufferSize64KB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=2, col=3
    )
fig.update_xaxes(title_text="SourceCnt <br>64KB Buffer", row=2, col=3)

df_changingSourceCntBufferSize64KB_pivot = pd.pivot_table(df_changingSourceCntBufferSize64KB, values='ThroughputInMBPerSec',
                                                          index=['SourceCnt'],
                                                          columns='WorkerThreads',
                                                          aggfunc=np.sum)
for i in range(len(df_changingSourceCntBufferSize64KB_pivot.columns)):
    col = df_changingSourceCntBufferSize64KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize64KB_pivot.index, y=df_changingSourceCntBufferSize64KB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=3, col=3
    )
fig.update_xaxes(title_text="SourceCnt <br>64KB Buffer", row=3, col=3)

df_changingSourceCntBufferSize64KB_pivot = pd.pivot_table(df_changingSourceCntBufferSize64KB, values='AvgLatencyInMs',
                                                          index=['SourceCnt'],
                                                          columns='WorkerThreads',
                                                          aggfunc=np.sum)
for i in range(len(df_changingSourceCntBufferSize64KB_pivot.columns)):
    col = df_changingSourceCntBufferSize64KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize64KB_pivot.index, y=df_changingSourceCntBufferSize64KB_pivot[col].values,
                   name=str('Wrk' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=4, col=3
    )
fig.update_xaxes(title_text="SourceCnt <br>64KB Buffer", row=4, col=3)

# ########################################## df_changingWorkerCntBufferSize64KB ##########################################################
df_changingWorkerCntBufferSize64KB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize64KB, values='ThroughputInTupsPerSec',
                                                          index=['WorkerThreads'],
                                                          columns='SourceCnt',
                                                          aggfunc=np.sum)

for i in range(len(df_changingWorkerCntBufferSize64KB_pivot.columns)):
    col = df_changingWorkerCntBufferSize64KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize64KB_pivot.index, y=df_changingWorkerCntBufferSize64KB_pivot[col].values,
                   name=str('Src' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=1, col=4
    )
fig.update_xaxes(title_text="WorkerCnt <br>64KB Buffer <br>Legend=Src-X", row=1, col=4)

df_changingWorkerCntBufferSize64KB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize64KB, values='ThroughputInTasksPerSec',
                                                          index=['WorkerThreads'],
                                                          columns='SourceCnt',
                                                          aggfunc=np.sum)

for i in range(len(df_changingWorkerCntBufferSize64KB_pivot.columns)):
    col = df_changingWorkerCntBufferSize64KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize64KB_pivot.index, y=df_changingWorkerCntBufferSize64KB_pivot[col].values,
                   name=str('Src' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=2, col=4
    )
fig.update_xaxes(title_text="WorkerCnt <br>64KB Buffer <br>Legend=Src-X", row=2, col=4)

df_changingWorkerCntBufferSize64KB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize64KB, values='ThroughputInMBPerSec',
                                                          index=['WorkerThreads'],
                                                          columns='SourceCnt',
                                                          aggfunc=np.sum)
for i in range(len(df_changingWorkerCntBufferSize64KB_pivot.columns)):
    col = df_changingWorkerCntBufferSize64KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize64KB_pivot.index, y=df_changingWorkerCntBufferSize64KB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=3, col=4
    )
fig.update_xaxes(title_text="WorkerCnt <br>64KB Buffer <br>Legend=Src-X", row=3, col=4)

df_changingWorkerCntBufferSize64KB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize64KB, values='AvgLatencyInMs',
                                                          index=['WorkerThreads'],
                                                          columns='SourceCnt',
                                                          aggfunc=np.sum)
for i in range(len(df_changingWorkerCntBufferSize64KB_pivot.columns)):
    col = df_changingWorkerCntBufferSize64KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize64KB_pivot.index, y=df_changingWorkerCntBufferSize64KB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=4, col=4
    )
fig.update_xaxes(title_text="WorkerCnt <br>64KB Buffer <br>Legend=Src-X", row=4, col=4)


# ########################################## df_changingSourceCntBufferSize128KBBufferSize128KB ##########################################################
df_changingSourceCntBufferSize128KB_pivot = pd.pivot_table(df_changingSourceCntBufferSize128KB, values='ThroughputInTupsPerSec',
                                                           index=['SourceCnt'],
                                                           columns='WorkerThreads',
                                                           aggfunc=np.sum)

for i in range(len(df_changingSourceCntBufferSize128KB_pivot.columns)):
    col = df_changingSourceCntBufferSize128KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize128KB_pivot.index, y=df_changingSourceCntBufferSize128KB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=1, col=5
    )
fig.update_xaxes(title_text="SourceCnt <br>128KB Buffer", row=1, col=5)

df_changingSourceCntBufferSize128KB_pivot = pd.pivot_table(df_changingSourceCntBufferSize128KB, values='ThroughputInTasksPerSec',
                                                           index=['SourceCnt'],
                                                           columns='WorkerThreads',
                                                           aggfunc=np.sum)

for i in range(len(df_changingSourceCntBufferSize128KB_pivot.columns)):
    col = df_changingSourceCntBufferSize128KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize128KB_pivot.index, y=df_changingSourceCntBufferSize128KB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=2, col=5
    )
fig.update_xaxes(title_text="SourceCnt <br>128KB Buffer", row=2, col=5)

df_changingSourceCntBufferSize128KB_pivot = pd.pivot_table(df_changingSourceCntBufferSize128KB, values='ThroughputInMBPerSec',
                                                           index=['SourceCnt'],
                                                           columns='WorkerThreads',
                                                           aggfunc=np.sum)
for i in range(len(df_changingSourceCntBufferSize128KB_pivot.columns)):
    col = df_changingSourceCntBufferSize128KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize128KB_pivot.index, y=df_changingSourceCntBufferSize128KB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=3, col=5
    )
fig.update_xaxes(title_text="SourceCnt <br>128KB Buffer", row=3, col=5)

df_changingSourceCntBufferSize128KB_pivot = pd.pivot_table(df_changingSourceCntBufferSize128KB, values='AvgLatencyInMs',
                                                           index=['SourceCnt'],
                                                           columns='WorkerThreads',
                                                           aggfunc=np.sum)
for i in range(len(df_changingSourceCntBufferSize128KB_pivot.columns)):
    col = df_changingSourceCntBufferSize128KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize128KB_pivot.index, y=df_changingSourceCntBufferSize128KB_pivot[col].values,
                   name=str('Wrk' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=4, col=5
    )
fig.update_xaxes(title_text="SourceCnt <br>128KB Buffer", row=4, col=5)

# ########################################## df_changingWorkerCntBufferSize128KB ##########################################################
df_changingWorkerCntBufferSize128KB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize128KB, values='ThroughputInTupsPerSec',
                                                           index=['WorkerThreads'],
                                                           columns='SourceCnt',
                                                           aggfunc=np.sum)

for i in range(len(df_changingWorkerCntBufferSize128KB_pivot.columns)):
    col = df_changingWorkerCntBufferSize128KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize128KB_pivot.index, y=df_changingWorkerCntBufferSize128KB_pivot[col].values,
                   name=str('Src' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=1, col=6
    )
fig.update_xaxes(title_text="WorkerCnt <br>128KB Buffer <br>Legend=Src-X", row=1, col=6)

df_changingWorkerCntBufferSize128KB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize128KB, values='ThroughputInTasksPerSec',
                                                           index=['WorkerThreads'],
                                                           columns='SourceCnt',
                                                           aggfunc=np.sum)

for i in range(len(df_changingWorkerCntBufferSize128KB_pivot.columns)):
    col = df_changingWorkerCntBufferSize128KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize128KB_pivot.index, y=df_changingWorkerCntBufferSize128KB_pivot[col].values,
                   name=str('Src' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=2, col=6
    )
fig.update_xaxes(title_text="WorkerCnt <br>128KB Buffer <br>Legend=Src-X", row=2, col=6)

df_changingWorkerCntBufferSize128KB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize128KB, values='ThroughputInMBPerSec',
                                                           index=['WorkerThreads'],
                                                           columns='SourceCnt',
                                                           aggfunc=np.sum)
for i in range(len(df_changingWorkerCntBufferSize128KB_pivot.columns)):
    col = df_changingWorkerCntBufferSize128KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize128KB_pivot.index, y=df_changingWorkerCntBufferSize128KB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=3, col=6
    )
fig.update_xaxes(title_text="WorkerCnt <br>128KB Buffer <br>Legend=Src-X", row=3, col=6)

df_changingWorkerCntBufferSize128KB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize128KB, values='AvgLatencyInMs',
                                                           index=['WorkerThreads'],
                                                           columns='SourceCnt',
                                                           aggfunc=np.sum)
for i in range(len(df_changingWorkerCntBufferSize128KB_pivot.columns)):
    col = df_changingWorkerCntBufferSize128KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize128KB_pivot.index, y=df_changingWorkerCntBufferSize128KB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=4, col=6
    )
fig.update_xaxes(title_text="WorkerCnt <br>128KB Buffer <br>Legend=Src-X", row=4, col=6)


# ########################################## df_changingSourceCntBufferSize512KBBufferSize512KB ##########################################################
df_changingSourceCntBufferSize512KB_pivot = pd.pivot_table(df_changingSourceCntBufferSize512KB, values='ThroughputInTupsPerSec',
                                                           index=['SourceCnt'],
                                                           columns='WorkerThreads',
                                                           aggfunc=np.sum)

for i in range(len(df_changingSourceCntBufferSize512KB_pivot.columns)):
    col = df_changingSourceCntBufferSize512KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize512KB_pivot.index, y=df_changingSourceCntBufferSize512KB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=1, col=7
    )
fig.update_xaxes(title_text="SourceCnt <br>512KB Buffer", row=1, col=7)

df_changingSourceCntBufferSize512KB_pivot = pd.pivot_table(df_changingSourceCntBufferSize512KB, values='ThroughputInTasksPerSec',
                                                           index=['SourceCnt'],
                                                           columns='WorkerThreads',
                                                           aggfunc=np.sum)

for i in range(len(df_changingSourceCntBufferSize512KB_pivot.columns)):
    col = df_changingSourceCntBufferSize512KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize512KB_pivot.index, y=df_changingSourceCntBufferSize512KB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=2, col=7
    )
fig.update_xaxes(title_text="SourceCnt <br>512KB Buffer", row=2, col=7)

df_changingSourceCntBufferSize512KB_pivot = pd.pivot_table(df_changingSourceCntBufferSize512KB, values='ThroughputInMBPerSec',
                                                           index=['SourceCnt'],
                                                           columns='WorkerThreads',
                                                           aggfunc=np.sum)
for i in range(len(df_changingSourceCntBufferSize512KB_pivot.columns)):
    col = df_changingSourceCntBufferSize512KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize512KB_pivot.index, y=df_changingSourceCntBufferSize512KB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=3, col=7
    )
fig.update_xaxes(title_text="SourceCnt <br>512KB Buffer", row=3, col=7)

df_changingSourceCntBufferSize512KB_pivot = pd.pivot_table(df_changingSourceCntBufferSize512KB, values='AvgLatencyInMs',
                                                           index=['SourceCnt'],
                                                           columns='WorkerThreads',
                                                           aggfunc=np.sum)
for i in range(len(df_changingSourceCntBufferSize512KB_pivot.columns)):
    col = df_changingSourceCntBufferSize512KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize512KB_pivot.index, y=df_changingSourceCntBufferSize512KB_pivot[col].values,
                   name=str('Wrk' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=4, col=7
    )
fig.update_xaxes(title_text="SourceCnt <br>512KB Buffer", row=4, col=7)

# ########################################## df_changingWorkerCntBufferSize512KB ##########################################################
df_changingWorkerCntBufferSize512KB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize512KB, values='ThroughputInTupsPerSec',
                                                           index=['WorkerThreads'],
                                                           columns='SourceCnt',
                                                           aggfunc=np.sum)

for i in range(len(df_changingWorkerCntBufferSize512KB_pivot.columns)):
    col = df_changingWorkerCntBufferSize512KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize512KB_pivot.index, y=df_changingWorkerCntBufferSize512KB_pivot[col].values,
                   name=str('Src' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=1, col=8
    )
fig.update_xaxes(title_text="WorkerCnt <br>512KB Buffer <br>Legend=Src-X", row=1, col=8)

df_changingWorkerCntBufferSize512KB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize512KB, values='ThroughputInTasksPerSec',
                                                           index=['WorkerThreads'],
                                                           columns='SourceCnt',
                                                           aggfunc=np.sum)

for i in range(len(df_changingWorkerCntBufferSize512KB_pivot.columns)):
    col = df_changingWorkerCntBufferSize512KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize512KB_pivot.index, y=df_changingWorkerCntBufferSize512KB_pivot[col].values,
                   name=str('Src' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=2, col=8
    )
fig.update_xaxes(title_text="WorkerCnt <br>512KB Buffer <br>Legend=Src-X", row=2, col=8)

df_changingWorkerCntBufferSize512KB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize512KB, values='ThroughputInMBPerSec',
                                                           index=['WorkerThreads'],
                                                           columns='SourceCnt',
                                                           aggfunc=np.sum)
for i in range(len(df_changingWorkerCntBufferSize512KB_pivot.columns)):
    col = df_changingWorkerCntBufferSize512KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize512KB_pivot.index, y=df_changingWorkerCntBufferSize512KB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=3, col=8
    )
fig.update_xaxes(title_text="WorkerCnt <br>512KB Buffer <br>Legend=Src-X", row=3, col=8)

df_changingWorkerCntBufferSize512KB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize512KB, values='AvgLatencyInMs',
                                                           index=['WorkerThreads'],
                                                           columns='SourceCnt',
                                                           aggfunc=np.sum)
for i in range(len(df_changingWorkerCntBufferSize512KB_pivot.columns)):
    col = df_changingWorkerCntBufferSize512KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize512KB_pivot.index, y=df_changingWorkerCntBufferSize512KB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=4, col=8
    )
fig.update_xaxes(title_text="WorkerCnt <br>512KB Buffer <br>Legend=Src-X", row=4, col=8)


# ########################################## df_changingSourceCntBufferSize1MBufferSize1MB ##########################################################
df_changingSourceCntBufferSize1MB_pivot = pd.pivot_table(df_changingSourceCntBufferSize1MB, values='ThroughputInTupsPerSec',
                                                        index=['SourceCnt'],
                                                        columns='WorkerThreads',
                                                        aggfunc=np.sum)

for i in range(len(df_changingSourceCntBufferSize1MB_pivot.columns)):
    col = df_changingSourceCntBufferSize1MB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize1MB_pivot.index, y=df_changingSourceCntBufferSize1MB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=1, col=9
    )
fig.update_xaxes(title_text="SourceCnt <br>1MB Buffer", row=1, col=9)

df_changingSourceCntBufferSize1MB_pivot = pd.pivot_table(df_changingSourceCntBufferSize1MB, values='ThroughputInTasksPerSec',
                                                        index=['SourceCnt'],
                                                        columns='WorkerThreads',
                                                        aggfunc=np.sum)

for i in range(len(df_changingSourceCntBufferSize1MB_pivot.columns)):
    col = df_changingSourceCntBufferSize1MB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize1MB_pivot.index, y=df_changingSourceCntBufferSize1MB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=2, col=9
    )
fig.update_xaxes(title_text="SourceCnt <br>1MB Buffer", row=2, col=9)

df_changingSourceCntBufferSize1MB_pivot = pd.pivot_table(df_changingSourceCntBufferSize1MB, values='ThroughputInMBPerSec',
                                                        index=['SourceCnt'],
                                                        columns='WorkerThreads',
                                                        aggfunc=np.sum)
for i in range(len(df_changingSourceCntBufferSize1MB_pivot.columns)):
    col = df_changingSourceCntBufferSize1MB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize1MB_pivot.index, y=df_changingSourceCntBufferSize1MB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=3, col=9
    )
fig.update_xaxes(title_text="SourceCnt <br>1MB Buffer", row=3, col=9)

df_changingSourceCntBufferSize1MB_pivot = pd.pivot_table(df_changingSourceCntBufferSize1MB, values='AvgLatencyInMs',
                                                        index=['SourceCnt'],
                                                        columns='WorkerThreads',
                                                        aggfunc=np.sum)
for i in range(len(df_changingSourceCntBufferSize1MB_pivot.columns)):
    col = df_changingSourceCntBufferSize1MB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize1MB_pivot.index, y=df_changingSourceCntBufferSize1MB_pivot[col].values,
                   name=str('Wrk' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=4, col=9
    )
fig.update_xaxes(title_text="SourceCnt <br>1MB Buffer", row=4, col=9)

# ########################################## df_changingWorkerCntBufferSize1MB ##########################################################
df_changingWorkerCntBufferSize1MB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize1MB, values='ThroughputInTupsPerSec',
                                                        index=['WorkerThreads'],
                                                        columns='SourceCnt',
                                                        aggfunc=np.sum)

for i in range(len(df_changingWorkerCntBufferSize1MB_pivot.columns)):
    col = df_changingWorkerCntBufferSize1MB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize1MB_pivot.index, y=df_changingWorkerCntBufferSize1MB_pivot[col].values,
                   name=str('Src' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=1, col=10
    )
fig.update_xaxes(title_text="WorkerCnt <br>1MB Buffer <br>Legend=Src-X", row=1, col=10)

df_changingWorkerCntBufferSize1MB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize1MB, values='ThroughputInTasksPerSec',
                                                        index=['WorkerThreads'],
                                                        columns='SourceCnt',
                                                        aggfunc=np.sum)

for i in range(len(df_changingWorkerCntBufferSize1MB_pivot.columns)):
    col = df_changingWorkerCntBufferSize1MB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize1MB_pivot.index, y=df_changingWorkerCntBufferSize1MB_pivot[col].values,
                   name=str('Src' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=2, col=10
    )
fig.update_xaxes(title_text="WorkerCnt <br>1MB Buffer <br>Legend=Src-X", row=2, col=10)

df_changingWorkerCntBufferSize1MB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize1MB, values='ThroughputInMBPerSec',
                                                        index=['WorkerThreads'],
                                                        columns='SourceCnt',
                                                        aggfunc=np.sum)
for i in range(len(df_changingWorkerCntBufferSize1MB_pivot.columns)):
    col = df_changingWorkerCntBufferSize1MB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize1MB_pivot.index, y=df_changingWorkerCntBufferSize1MB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=3, col=10
    )
fig.update_xaxes(title_text="WorkerCnt <br>1MB Buffer <br>Legend=Src-X", row=3, col=10)

df_changingWorkerCntBufferSize1MB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize1MB, values='AvgLatencyInMs',
                                                        index=['WorkerThreads'],
                                                        columns='SourceCnt',
                                                        aggfunc=np.sum)
for i in range(len(df_changingWorkerCntBufferSize1MB_pivot.columns)):
    col = df_changingWorkerCntBufferSize1MB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize1MB_pivot.index, y=df_changingWorkerCntBufferSize1MB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=4, col=10
    )
fig.update_xaxes(title_text="WorkerCnt <br>1MB Buffer <br>Legend=Src-X", row=4, col=10)


# ########################################## df_changingSourceCntBufferSize4MBufferSize4MB ##########################################################
df_changingSourceCntBufferSize4MB_pivot = pd.pivot_table(df_changingSourceCntBufferSize4MB, values='ThroughputInTupsPerSec',
                                                        index=['SourceCnt'],
                                                        columns='WorkerThreads',
                                                        aggfunc=np.sum)

for i in range(len(df_changingSourceCntBufferSize4MB_pivot.columns)):
    col = df_changingSourceCntBufferSize4MB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize4MB_pivot.index, y=df_changingSourceCntBufferSize4MB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=1, col=12
    )
fig.update_xaxes(title_text="SourceCnt <br>4MB Buffer", row=1, col=12)

df_changingSourceCntBufferSize4MB_pivot = pd.pivot_table(df_changingSourceCntBufferSize4MB, values='ThroughputInTasksPerSec',
                                                        index=['SourceCnt'],
                                                        columns='WorkerThreads',
                                                        aggfunc=np.sum)

for i in range(len(df_changingSourceCntBufferSize4MB_pivot.columns)):
    col = df_changingSourceCntBufferSize4MB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize4MB_pivot.index, y=df_changingSourceCntBufferSize4MB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=2, col=12
    )
fig.update_xaxes(title_text="SourceCnt <br>4MB Buffer", row=2, col=12)

df_changingSourceCntBufferSize4MB_pivot = pd.pivot_table(df_changingSourceCntBufferSize4MB, values='ThroughputInMBPerSec',
                                                        index=['SourceCnt'],
                                                        columns='WorkerThreads',
                                                        aggfunc=np.sum)
for i in range(len(df_changingSourceCntBufferSize4MB_pivot.columns)):
    col = df_changingSourceCntBufferSize4MB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize4MB_pivot.index, y=df_changingSourceCntBufferSize4MB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=3, col=12
    )
fig.update_xaxes(title_text="SourceCnt <br>4MB Buffer", row=3, col=12)

df_changingSourceCntBufferSize4MB_pivot = pd.pivot_table(df_changingSourceCntBufferSize4MB, values='AvgLatencyInMs',
                                                        index=['SourceCnt'],
                                                        columns='WorkerThreads',
                                                        aggfunc=np.sum)
for i in range(len(df_changingSourceCntBufferSize4MB_pivot.columns)):
    col = df_changingSourceCntBufferSize4MB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingSourceCntBufferSize4MB_pivot.index, y=df_changingSourceCntBufferSize4MB_pivot[col].values,
                   name=str('Wrk' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=4, col=12
    )
fig.update_xaxes(title_text="SourceCnt <br>4MB Buffer", row=4, col=12)

# ########################################## df_changingWorkerCntBufferSize4MB ##########################################################
df_changingWorkerCntBufferSize4MB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize4MB, values='ThroughputInTupsPerSec',
                                                        index=['WorkerThreads'],
                                                        columns='SourceCnt',
                                                        aggfunc=np.sum)

for i in range(len(df_changingWorkerCntBufferSize4MB_pivot.columns)):
    col = df_changingWorkerCntBufferSize4MB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize4MB_pivot.index, y=df_changingWorkerCntBufferSize4MB_pivot[col].values,
                   name=str('Src' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=1, col=11
    )
fig.update_xaxes(title_text="WorkerCnt <br>4MB Buffer <br>Legend=Src-X", row=1, col=11)

df_changingWorkerCntBufferSize4MB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize4MB, values='ThroughputInTasksPerSec',
                                                        index=['WorkerThreads'],
                                                        columns='SourceCnt',
                                                        aggfunc=np.sum)

for i in range(len(df_changingWorkerCntBufferSize4MB_pivot.columns)):
    col = df_changingWorkerCntBufferSize4MB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize4MB_pivot.index, y=df_changingWorkerCntBufferSize4MB_pivot[col].values,
                   name=str('Src' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=2, col=11
    )
fig.update_xaxes(title_text="WorkerCnt <br>4MB Buffer <br>Legend=Src-X", row=2, col=11)

df_changingWorkerCntBufferSize4MB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize4MB, values='ThroughputInMBPerSec',
                                                        index=['WorkerThreads'],
                                                        columns='SourceCnt',
                                                        aggfunc=np.sum)
for i in range(len(df_changingWorkerCntBufferSize4MB_pivot.columns)):
    col = df_changingWorkerCntBufferSize4MB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize4MB_pivot.index, y=df_changingWorkerCntBufferSize4MB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=3, col=11
    )
fig.update_xaxes(title_text="WorkerCnt <br>4MB Buffer <br>Legend=Src-X", row=3, col=11)

df_changingWorkerCntBufferSize4MB_pivot = pd.pivot_table(df_changingWorkerCntBufferSize4MB, values='AvgLatencyInMs',
                                                        index=['WorkerThreads'],
                                                        columns='SourceCnt',
                                                        aggfunc=np.sum)
for i in range(len(df_changingWorkerCntBufferSize4MB_pivot.columns)):
    col = df_changingWorkerCntBufferSize4MB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=df_changingWorkerCntBufferSize4MB_pivot.index, y=df_changingWorkerCntBufferSize4MB_pivot[col].values,
                   name=str(col),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNamesSrc[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=4, col=11
    )
fig.update_xaxes(title_text="WorkerCnt <br>4MB Buffer <br>Legend=Src-X", row=4, col=11)

###############


fig.update_layout(barmode='overlay')
fig.update_layout(
    title={
        'text': "<b>NebulaStream Performance Numbers -- Buffer Workload -- ReadModifyWrite </b><br>"
                "<span style=\"font-size:0.6em\" line-height=1em;margin=-4px>Default Config(GlobalBufferPool=65536, LocalBufferPool=1024,  TupleSize=24Byte 'Query::from(input).map(Attribute(value) = Attribute(value) + 1).sink(NullOutputSinkDescriptor::create()))"
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
plotly.offline.plot(fig, filename='ScalingBufferWorkload.html')
