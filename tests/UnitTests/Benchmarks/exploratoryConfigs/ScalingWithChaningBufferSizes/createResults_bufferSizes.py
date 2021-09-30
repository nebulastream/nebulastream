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
# folder = "/home/zeuchste/Dropbox/nes/folly/"
folder = "/home/zeuchste/Dropbox/nes/ventura/"

files = [
    "changingSourceCntBufferSize16KB",
    "changingSourceCntBufferSize128KB",
    "changingSourceCntBufferSize64KB",
    "changingSourceCntBufferSize512KB",
    "changingSourceCntBufferSize1MB",
    "changingSourceCntBufferSize4MB",
    "changingWorkerCntBufferSize16KB",
    "changingWorkerCntBufferSize64KB",
    "changingWorkerCntBufferSize128KB",
    "changingWorkerCntBufferSize512KB",
    "changingWorkerCntBufferSize1MB",
    "changingWorkerCntBufferSize4MB",
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
    rows=5, cols=13,
    column_widths=[1,1,1,1,1,1,1,1,1,1,1,1,1],
    row_heights=[3, 3, 3, 3, 3],
    shared_xaxes=False,
    subplot_titles=[
    ],
)

# Get the color-wheel
dictOfNames = {
    1: 'darkblue',
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

# ########################################## changingSourceCntBufferSize16KBBufferSize16KB ##########################################################
changingSourceCntBufferSize16KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize16KB"], values='ThroughputInTupsPerSec',
                                                          index=['SourceCnt'],
                                                          columns='WorkerThreads',
                                                          aggfunc=np.sum)

for i in range(len(changingSourceCntBufferSize16KB_pivot.columns)):
    col = changingSourceCntBufferSize16KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=changingSourceCntBufferSize16KB_pivot.index, y=changingSourceCntBufferSize16KB_pivot[col].values,
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
fig.update_xaxes(title_text="x=SourceCnt <br>16KB Buffer", row=1, col=1)
fig.update_yaxes(title_text="ThroughputInTupsPerSec", row=1, col=1)

changingSourceCntBufferSize16KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize16KB"], values='ThroughputInTasksPerSec',
                                                          index=['SourceCnt'],
                                                          columns='WorkerThreads',
                                                          aggfunc=np.sum)

for i in range(len(changingSourceCntBufferSize16KB_pivot.columns)):
    col = changingSourceCntBufferSize16KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=changingSourceCntBufferSize16KB_pivot.index, y=changingSourceCntBufferSize16KB_pivot[col].values,
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
fig.update_xaxes(title_text="x=SourceCnt <br>16KB Buffer", row=2, col=1)
fig.update_yaxes(title_text="ThroughputInTasksPerSec", row=2, col=1)

changingSourceCntBufferSize16KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize16KB"], values='ThroughputInMBPerSec',
                                                          index=['SourceCnt'],
                                                          columns='WorkerThreads',
                                                          aggfunc=np.sum)
for i in range(len(changingSourceCntBufferSize16KB_pivot.columns)):
    col = changingSourceCntBufferSize16KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=changingSourceCntBufferSize16KB_pivot.index, y=changingSourceCntBufferSize16KB_pivot[col].values,
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
fig.update_xaxes(title_text="x=SourceCnt <br>16KB Buffer", row=3, col=1)
fig.update_yaxes(title_text="ThroughputInMBPerSec", row=3, col=1)

changingSourceCntBufferSize16KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize16KB"], values='AvgTaskProcessingTime',
                                                          index=['SourceCnt'],
                                                          columns='WorkerThreads',
                                                          aggfunc=np.sum)
for i in range(len(changingSourceCntBufferSize16KB_pivot.columns)):
    col = changingSourceCntBufferSize16KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=changingSourceCntBufferSize16KB_pivot.index, y=changingSourceCntBufferSize16KB_pivot[col].values,
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
fig.update_xaxes(title_text="x=SourceCnt <br>16KB Buffer", row=4, col=1)
fig.update_yaxes(title_text="AvgTaskProcessingTime", row=4, col=1)



changingSourceCntBufferSize16KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize16KB"], values='AvgQueueSize',
                                                          index=['SourceCnt'],
                                                          columns='WorkerThreads',
                                                          aggfunc=np.sum)
for i in range(len(changingSourceCntBufferSize16KB_pivot.columns)):
    col = changingSourceCntBufferSize16KB_pivot.columns[i]
    fig.add_trace(
        go.Scatter(x=changingSourceCntBufferSize16KB_pivot.index, y=changingSourceCntBufferSize16KB_pivot[col].values,
                   name=str('Wrk' + str(col)),
                   hoverinfo='x+y',
                   mode='markers+lines',
                   line=dict(shape='linear', color=dictOfNames[col],
                             width=2),
                   connectgaps=True,
                   showlegend=False
                   ),
        row=5, col=1
    )
fig.update_xaxes(title_text="x=SourceCnt <br>16KB Buffer", row=5, col=1)
fig.update_yaxes(title_text="AvgQueueSize", row=5, col=1)

# ########################################## df_changingSourceCntBufferSize64KBBufferSize64KB ##########################################################
df_changingSourceCntBufferSize64KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize64KB"], values='ThroughputInTupsPerSec',
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
        row=1, col=2
    )
fig.update_xaxes(title_text="x=SourceCnt <br>64KB Buffer", row=1, col=2)

df_changingSourceCntBufferSize64KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize64KB"], values='ThroughputInTasksPerSec',
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
        row=2, col=2
    )
fig.update_xaxes(title_text="x=SourceCnt <br>64KB Buffer", row=2, col=2)

df_changingSourceCntBufferSize64KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize64KB"], values='ThroughputInMBPerSec',
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
        row=3, col=2
    )
fig.update_xaxes(title_text="x=SourceCnt <br>64KB Buffer", row=3, col=2)

df_changingSourceCntBufferSize64KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize64KB"], values='AvgTaskProcessingTime',
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
        row=4, col=2
    )
fig.update_xaxes(title_text="x=SourceCnt <br>64KB Buffer", row=4, col=2)

df_changingSourceCntBufferSize64KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize64KB"], values='AvgQueueSize',
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
        row=5, col=2
    )
fig.update_xaxes(title_text="x=SourceCnt <br>64KB Buffer", row=5, col=2)
# ########################################## df_changingSourceCntBufferSize128KBBufferSize128KB ##########################################################
df_changingSourceCntBufferSize128KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize128KB"], values='ThroughputInTupsPerSec',
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
        row=1, col=3
    )
fig.update_xaxes(title_text="x=SourceCnt <br>128KB Buffer", row=1, col=3)

df_changingSourceCntBufferSize128KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize128KB"], values='ThroughputInTasksPerSec',
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
        row=2, col=3
    )
fig.update_xaxes(title_text="x=SourceCnt <br>128KB Buffer", row=2, col=3)

df_changingSourceCntBufferSize128KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize128KB"], values='ThroughputInMBPerSec',
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
        row=3, col=3
    )
fig.update_xaxes(title_text="x=SourceCnt <br>128KB Buffer", row=3, col=3)

df_changingSourceCntBufferSize128KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize128KB"], values='AvgTaskProcessingTime',
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
        row=4, col=3
    )
fig.update_xaxes(title_text="x=SourceCnt <br>128KB Buffer", row=4, col=3)

df_changingSourceCntBufferSize128KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize128KB"], values='AvgQueueSize',
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
        row=5, col=3
    )
fig.update_xaxes(title_text="x=SourceCnt <br>128KB Buffer", row=5, col=3)

# ########################################## df_changingSourceCntBufferSize512KBBufferSize512KB ##########################################################
df_changingSourceCntBufferSize512KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize512KB"], values='ThroughputInTupsPerSec',
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
        row=1, col=4
    )
fig.update_xaxes(title_text="x=SourceCnt <br>512KB Buffer", row=1, col=4)

df_changingSourceCntBufferSize512KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize512KB"], values='ThroughputInTasksPerSec',
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
        row=2, col=4
    )
fig.update_xaxes(title_text="x=SourceCnt <br>512KB Buffer", row=2, col=4)

df_changingSourceCntBufferSize512KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize512KB"], values='ThroughputInMBPerSec',
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
        row=3, col=4
    )
fig.update_xaxes(title_text="x=SourceCnt <br>512KB Buffer", row=3, col=4)

df_changingSourceCntBufferSize512KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize512KB"], values='AvgTaskProcessingTime',
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
        row=4, col=4
    )
fig.update_xaxes(title_text="x=SourceCnt <br>512KB Buffer", row=4, col=4)

df_changingSourceCntBufferSize512KB_pivot = pd.pivot_table(df["changingSourceCntBufferSize512KB"], values='AvgQueueSize',
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
        row=5, col=4
    )
fig.update_xaxes(title_text="x=SourceCnt <br>512KB Buffer", row=5, col=4)
# ########################################## df_changingSourceCntBufferSize1MBufferSize1MB ##########################################################
df_changingSourceCntBufferSize1MB_pivot = pd.pivot_table(df["changingSourceCntBufferSize1MB"], values='ThroughputInTupsPerSec',
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
        row=1, col=5
    )
fig.update_xaxes(title_text="x=SourceCnt <br>1MB Buffer", row=1, col=5)

df_changingSourceCntBufferSize1MB_pivot = pd.pivot_table(df["changingSourceCntBufferSize1MB"], values='ThroughputInTasksPerSec',
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
        row=2, col=5
    )
fig.update_xaxes(title_text="x=SourceCnt <br>1MB Buffer", row=2, col=5)

df_changingSourceCntBufferSize1MB_pivot = pd.pivot_table(df["changingSourceCntBufferSize1MB"], values='ThroughputInMBPerSec',
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
        row=3, col=5
    )
fig.update_xaxes(title_text="x=SourceCnt <br>1MB Buffer", row=3, col=5)

df_changingSourceCntBufferSize1MB_pivot = pd.pivot_table(df["changingSourceCntBufferSize1MB"], values='AvgTaskProcessingTime',
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
        row=4, col=5
    )
fig.update_xaxes(title_text="x=SourceCnt <br>1MB Buffer", row=4, col=5)


df_changingSourceCntBufferSize1MB_pivot = pd.pivot_table(df["changingSourceCntBufferSize1MB"], values='AvgQueueSize',
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
        row=5, col=5
    )
fig.update_xaxes(title_text="x=SourceCnt <br>1MB Buffer", row=4, col=5)

# ########################################## df_changingSourceCntBufferSize4MBufferSize4MB ##########################################################
df_changingSourceCntBufferSize4MB_pivot = pd.pivot_table(df["changingSourceCntBufferSize4MB"], values='ThroughputInTupsPerSec',
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
        row=1, col=6
    )
fig.update_xaxes(title_text="x=SourceCnt <br>4MB Buffer", row=1, col=6)

df_changingSourceCntBufferSize4MB_pivot = pd.pivot_table(df["changingSourceCntBufferSize4MB"], values='ThroughputInTasksPerSec',
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
        row=2, col=6
    )
fig.update_xaxes(title_text="x=SourceCnt <br>4MB Buffer", row=2, col=6)

df_changingSourceCntBufferSize4MB_pivot = pd.pivot_table(df["changingSourceCntBufferSize4MB"], values='ThroughputInMBPerSec',
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
        row=3, col=6
    )
fig.update_xaxes(title_text="x=SourceCnt <br>4MB Buffer", row=3, col=6)

df_changingSourceCntBufferSize4MB_pivot = pd.pivot_table(df["changingSourceCntBufferSize4MB"], values='AvgTaskProcessingTime',
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
        row=4, col=6
    )
fig.update_xaxes(title_text="x=SourceCnt <br>4MB Buffer", row=4, col=6)

df_changingSourceCntBufferSize4MB_pivot = pd.pivot_table(df["changingSourceCntBufferSize4MB"], values='AvgQueueSize',
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
        row=5, col=6
    )
fig.update_xaxes(title_text="x=SourceCnt <br>4MB Buffer", row=5, col=6)


# ########################################## df_changingWorkerCntBufferSize16KB ##########################################################
df_changingWorkerCntBufferSize16KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize16KB"], values='ThroughputInTupsPerSec',
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
        row=1, col=8
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>16KB Buffer ", row=1, col=8)

df_changingWorkerCntBufferSize16KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize16KB"], values='ThroughputInTasksPerSec',
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
        row=2, col=8
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>16KB Buffer ", row=2, col=8)

df_changingWorkerCntBufferSize16KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize16KB"], values='ThroughputInMBPerSec',
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
        row=3, col=8
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>16KB Buffer ", row=3, col=8)

df_changingWorkerCntBufferSize16KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize16KB"], values='AvgTaskProcessingTime',
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
        row=4, col=8
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>16KB Buffer ", row=4, col=8)

df_changingWorkerCntBufferSize16KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize16KB"], values='AvgQueueSize',
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
        row=5, col=8
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>16KB Buffer ", row=5, col=8)


# ########################################## df_changingWorkerCntBufferSize64KB ##########################################################
df_changingWorkerCntBufferSize64KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize64KB"], values='ThroughputInTupsPerSec',
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
        row=1, col=9
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>64KB Buffer ", row=1, col=9)

df_changingWorkerCntBufferSize64KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize64KB"], values='ThroughputInTasksPerSec',
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
        row=2, col=9
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>64KB Buffer ", row=2, col=9)

df_changingWorkerCntBufferSize64KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize64KB"], values='ThroughputInMBPerSec',
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
        row=3, col=9
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>64KB Buffer ", row=3, col=9)

df_changingWorkerCntBufferSize64KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize64KB"], values='AvgTaskProcessingTime',
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
        row=4, col=9
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>64KB Buffer ", row=4, col=9)

df_changingWorkerCntBufferSize64KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize64KB"], values='AvgQueueSize',
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
        row=5, col=9
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>64KB Buffer ", row=5, col=9)
# ########################################## df_changingWorkerCntBufferSize128KB ##########################################################
df_changingWorkerCntBufferSize128KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize128KB"], values='ThroughputInTupsPerSec',
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
        row=1, col=10
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>128KB Buffer ", row=1, col=10)

df_changingWorkerCntBufferSize128KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize128KB"], values='ThroughputInTasksPerSec',
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
        row=2, col=10
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>128KB Buffer ", row=2, col=10)

df_changingWorkerCntBufferSize128KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize128KB"], values='ThroughputInMBPerSec',
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
        row=3, col=10
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>128KB Buffer ", row=3, col=10)

df_changingWorkerCntBufferSize128KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize128KB"], values='AvgTaskProcessingTime',
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
        row=4, col=10
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>128KB Buffer ", row=4, col=10)

df_changingWorkerCntBufferSize128KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize128KB"], values='AvgQueueSize',
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
        row=5, col=10
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>128KB Buffer ", row=5, col=10)

# ########################################## df_changingWorkerCntBufferSize512KB ##########################################################
df_changingWorkerCntBufferSize512KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize512KB"], values='ThroughputInTupsPerSec',
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
        row=1, col=11
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>512KB Buffer ", row=1, col=11)

df_changingWorkerCntBufferSize512KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize512KB"], values='ThroughputInTasksPerSec',
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
        row=2, col=11
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>512KB Buffer ", row=2, col=11)

df_changingWorkerCntBufferSize512KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize512KB"], values='ThroughputInMBPerSec',
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
        row=3, col=11
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>512KB Buffer ", row=3, col=11)

df_changingWorkerCntBufferSize512KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize512KB"], values='AvgTaskProcessingTime',
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
        row=4, col=11
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>512KB Buffer ", row=4, col=11)

df_changingWorkerCntBufferSize512KB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize512KB"], values='AvgQueueSize',
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
        row=5, col=11
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>512KB Buffer ", row=5, col=11)


# ########################################## df_changingWorkerCntBufferSize1MB ##########################################################
df_changingWorkerCntBufferSize1MB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize1MB"], values='ThroughputInTupsPerSec',
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
        row=1, col=12
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>1MB Buffer ", row=1, col=12)

df_changingWorkerCntBufferSize1MB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize1MB"], values='ThroughputInTasksPerSec',
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
        row=2, col=12
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>1MB Buffer ", row=2, col=12)

df_changingWorkerCntBufferSize1MB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize1MB"], values='ThroughputInMBPerSec',
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
        row=3, col=12
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>1MB Buffer ", row=3, col=12)

df_changingWorkerCntBufferSize1MB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize1MB"], values='AvgTaskProcessingTime',
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
        row=4, col=12
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>1MB Buffer ", row=4, col=12)

df_changingWorkerCntBufferSize1MB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize1MB"], values='AvgQueueSize',
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
        row=5, col=12
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>1MB Buffer ", row=5, col=12)

# ########################################## df_changingWorkerCntBufferSize4MB ##########################################################
df_changingWorkerCntBufferSize4MB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize4MB"], values='ThroughputInTupsPerSec',
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
        row=1, col=13
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>4MB Buffer ", row=1, col=13)

df_changingWorkerCntBufferSize4MB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize4MB"], values='ThroughputInTasksPerSec',
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
        row=2, col=13
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>4MB Buffer ", row=2, col=13)

df_changingWorkerCntBufferSize4MB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize4MB"], values='ThroughputInMBPerSec',
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
        row=3, col=13
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>4MB Buffer ", row=3, col=13)

df_changingWorkerCntBufferSize4MB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize4MB"], values='AvgTaskProcessingTime',
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
        row=4, col=13
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>4MB Buffer ", row=4, col=13)

df_changingWorkerCntBufferSize4MB_pivot = pd.pivot_table(df["changingWorkerCntBufferSize4MB"], values='AvgQueueSize',
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
        row=5, col=13
    )
fig.update_xaxes(title_text="x=WorkerCnt <br>4MB Buffer ", row=5, col=13)
###############


fig.update_layout(barmode='overlay')
fig.update_layout(
    title={
        'text': "<b>NebulaStream Performance Numbers -- Buffer Workload -- ReadModifyWrite -- Change: Pipeline</b><br>"
                "<span style=\"font-size:0.6em\" line-height=1em;margin=-4px>Default Config(GlobalBufferPool=16K, LocalBufferPool=1024,  TupleSize=24Byte 'Query::from(input).map(Attribute(value) = Attribute(value) + 1).sink(NullOutputSinkDescriptor::create()))"
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
plotly.offline.plot(fig, filename=folder + "ScalingBufferWorkload.html")
