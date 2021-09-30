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

files = [
    "changingBufferSizeSourceSink",
    "changingGlobalBufferCntSourceSink",
    "changingLocalBufferSizeSourceSink",
    "changingSourceCntSourceSink",
    "changingWorkerCntSourceSink",
    "changingThreadsAndSource_SourceSink",
    "scalingLargeSourceSink"
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
    rows=5, cols=7,
    column_widths=[3, 3, 3, 3, 3, 3, 3],
    row_heights=[1, 1, 1, 1, 1],
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
df_changingBufferSize_pivot = pd.pivot_table(df["changingBufferSizeSourceSink"], values='ThroughputInTupsPerSec',
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

df_changingBufferSize_pivot = pd.pivot_table(df["changingBufferSizeSourceSink"], values='ThroughputInTasksPerSec',
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

df_changingBufferSize_pivot = pd.pivot_table(df["changingBufferSizeSourceSink"], values='ThroughputInTasksPerSec',
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

df_changingBufferSize_pivot = pd.pivot_table(df["changingBufferSizeSourceSink"], values='AvgTaskProcessingTime',
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
fig.update_yaxes(title_text="AvgTaskProcessingTime", type="log", row=4, col=1)

df_changingBufferSize_pivot = pd.pivot_table(df["changingBufferSizeSourceSink"], values='AvgQueueSize',
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
                  row=5, col=1
                  )
fig.update_xaxes(title_text="BufferSizeInBytes", row=5, col=1)
fig.update_yaxes(title_text="AvgQueueSize", row=5, col=1)

# ########################################## df_changingGlobalBufferCnt ##########################################################
df_changingGlobalBufferCnt_pivot = pd.pivot_table(df["changingGlobalBufferCntSourceSink"], values='ThroughputInTupsPerSec',
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

df_changingGlobalBufferCnt_pivot = pd.pivot_table(df["changingGlobalBufferCntSourceSink"], values='ThroughputInTasksPerSec',
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

df_changingGlobalBufferCnt_pivot = pd.pivot_table(df["changingGlobalBufferCntSourceSink"], values='ThroughputInMBPerSec',
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

df_changingGlobalBufferCnt_pivot = pd.pivot_table(df["changingGlobalBufferCntSourceSink"], values='AvgTaskProcessingTime',
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

df_changingGlobalBufferCnt_pivot = pd.pivot_table(df["changingGlobalBufferCntSourceSink"], values='AvgQueueSize',
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
        row=5, col=2
    )
fig.update_xaxes(title_text="GlobalBufferCnt", type="log", row=5, col=2)
fig.update_yaxes(type="log", row=5, col=2)

# ########################################## df_changingLocalBufferSize ##########################################################
df_changingLocalBufferSize_pivot = pd.pivot_table(df["changingLocalBufferSizeSourceSink"], values='ThroughputInTupsPerSec',
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

df_changingLocalBufferSize_pivot = pd.pivot_table(df["changingLocalBufferSizeSourceSink"], values='ThroughputInTasksPerSec',
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

df_changingLocalBufferSize_pivot = pd.pivot_table(df["changingLocalBufferSizeSourceSink"], values='ThroughputInMBPerSec',
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

df_changingLocalBufferSize_pivot = pd.pivot_table(df["changingLocalBufferSizeSourceSink"], values='AvgTaskProcessingTime',
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


df_changingLocalBufferSize_pivot = pd.pivot_table(df["changingLocalBufferSizeSourceSink"], values='AvgQueueSize',
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
        row=5, col=3
    )
fig.update_xaxes(title_text="LocalBufferCnt", row=5, col=3)
fig.update_yaxes(row=5, col=3)

# ########################################## df_changingSourceCnt ##########################################################
df_changingSourceCnt_pivot = pd.pivot_table(df["changingSourceCntSourceSink"], values='ThroughputInTupsPerSec',
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

df_changingSourceCnt_pivot = pd.pivot_table(df["changingSourceCntSourceSink"], values='ThroughputInTasksPerSec',
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

df_changingSourceCnt_pivot = pd.pivot_table(df["changingSourceCntSourceSink"], values='ThroughputInMBPerSec',
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

df_changingSourceCnt_pivot = pd.pivot_table(df["changingSourceCntSourceSink"], values='AvgTaskProcessingTime',
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

df_changingSourceCnt_pivot = pd.pivot_table(df["changingSourceCntSourceSink"], values='AvgQueueSize',
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
        row=5, col=4
    )
fig.update_xaxes(title_text="SourceCnt", row=5, col=4)


# ########################################## df_chaningWorkerCnt ##########################################################
df_chaningWorkerCnt_pivot = pd.pivot_table(df["changingWorkerCntSourceSink"], values='ThroughputInTupsPerSec',
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

df_chaningWorkerCnt_pivot = pd.pivot_table(df["changingWorkerCntSourceSink"], values='ThroughputInTasksPerSec',
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

df_chaningWorkerCnt_pivot = pd.pivot_table(df["changingWorkerCntSourceSink"], values='ThroughputInMBPerSec',
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

df_chaningWorkerCnt_pivot = pd.pivot_table(df["changingWorkerCntSourceSink"], values='AvgTaskProcessingTime',
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

df_chaningWorkerCnt_pivot = pd.pivot_table(df["changingWorkerCntSourceSink"], values='AvgQueueSize',
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
        row=5, col=5
    )
fig.update_xaxes(title_text="WorkerCnt <br>Legend=Src-X", row=5, col=5)

############################################# df["changingThreadsAndSource_SourceSink"] #######################################################
df["changingThreadsAndSource_SourceSink"]["src"] = "W" + df["changingThreadsAndSource_SourceSink"]["WorkerThreads"].astype(str) + '/S' + \
                                           df["changingThreadsAndSource_SourceSink"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSource_SourceSink"]['src'],
               y=df["changingThreadsAndSource_SourceSink"]['ThroughputInTupsPerSec'],
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

df["changingThreadsAndSource_SourceSink"]["src"] = "W" + df["changingThreadsAndSource_SourceSink"]["WorkerThreads"].astype(str) + '/S' + \
                                           df["changingThreadsAndSource_SourceSink"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSource_SourceSink"]['src'],
               y=df["changingThreadsAndSource_SourceSink"]['ThroughputInTasksPerSec'],
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
    go.Scatter(x=df["changingThreadsAndSource_SourceSink"]['src'], y=df["changingThreadsAndSource_SourceSink"]['ThroughputInMBPerSec'],
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

df["changingThreadsAndSource_SourceSink"]["src"] = "W" + df["changingThreadsAndSource_SourceSink"]["WorkerThreads"].astype(str) + '/S' + \
                                           df["changingThreadsAndSource_SourceSink"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSource_SourceSink"]['src'], y=df["changingThreadsAndSource_SourceSink"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=4, col=6
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt", row=4, col=6)

fig.add_trace(
    go.Scatter(x=df["changingThreadsAndSource_SourceSink"]['src'], y=df["changingThreadsAndSource_SourceSink"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgQueueSize",
               showlegend=False
               ),
    row=5, col=6
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt", row=5, col=6)

############################################# df["scalingLargeSourceSink"] #######################################################
df["scalingLargeSourceSink"]["src"] = "W" + df["scalingLargeSourceSink"][
    "WorkerThreads"].astype(str) + '/S' + df["scalingLargeSourceSink"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["scalingLargeSourceSink"]['src'],
               y=df["scalingLargeSourceSink"]['ThroughputInTupsPerSec'],
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

df["scalingLargeSourceSink"]["src"] = "W" + df["scalingLargeSourceSink"][
    "WorkerThreads"].astype(str) + '/S' + df["scalingLargeSourceSink"]["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df["scalingLargeSourceSink"]['src'],
               y=df["scalingLargeSourceSink"]['ThroughputInTasksPerSec'],
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
    go.Scatter(x=df["scalingLargeSourceSink"]['src'],
               y=df["scalingLargeSourceSink"]['ThroughputInMBPerSec'],
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
    go.Scatter(x=df["scalingLargeSourceSink"]['src'],
               y=df["scalingLargeSourceSink"]['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=4, col=7
)
fig.update_xaxes(title_text="Wrk/Src Large Scale", row=4, col=7)

fig.add_trace(
    go.Scatter(x=df["scalingLargeSourceSink"]['src'],
               y=df["scalingLargeSourceSink"]['AvgQueueSize'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="avgqueuesize",
               showlegend=False
               ),
    row=5, col=7
)
fig.update_xaxes(title_text="Wrk/Src Large Scale", row=5, col=7)

fig.update_layout(barmode='overlay')
fig.update_layout(
    title={
        'text': "<b>NebulaStream Performance Numbers -- MemoryMode -- SourceSinkWorkload -- Change: Folly</b><br>"
                "<span style=\"font-size:0.6em\" line-height=1em;margin=-4px>Default Config(GlobalBufferPool=16K, "
                "LocalBufferPool=1024, BufferSize=1MB, Threads=1, Source=1, TupleSize=24Byte, Query::from(input).sink(NullOutputSinkDescriptor::create());)"
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
plotly.offline.plot(fig, filename=folder + "SourceSinkWorkload.html")
