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

df_changingThreadsAndSourceNoProc = pd.read_csv(folder + 'changingThreadsAndSourceNoProc.csv')

df_changingThreadsAndSourceTumblingWindowQuery = pd.read_csv(folder + 'changingThreadsAndSourceTumblingWindowQuery.csv')
df_changingThreadsAndSourceYSBWindowQuery = pd.read_csv(folder + 'changingThreadsAndSourceYSBWindowQuery.csv')
df_changingThreadsAndSourceSlidingWindowQuery = pd.read_csv(folder + 'changingThreadsAndSourceSlidingWindowQuery.csv')

changingThreadsAndSourceTumblingWindowJoinQuery = pd.read_csv(folder + 'changingThreadsAndSourceTumblingWindowJoinQuery.csv')
changingThreadsAndSourceSlidingWindowJoinQuery = pd.read_csv(folder + 'changingThreadsAndSourceSlidingWindowJoinQuery.csv')

import plotly.graph_objects as go
import plotly

# ###############################################################
fig = make_subplots(
    rows=3, cols=6,
    column_widths=[3, 3, 3, 3, 3, 3],
    row_heights=[1, 1, 1],
    shared_xaxes=False,
    subplot_titles=[
        'Baseline NoProc',
        'Tumbling Window Sz=1sec',
        'Sliding Window Sz=1,Sl=0.3',
        'YSB Query Sz=1sec',
        'Tumbling Window Join Sz=1sec',
        'Sliding Window Join Sz=1,Sl=0.3'
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
    row=1, col=1
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt NoProc", row=1, col=1)
fig.update_yaxes(title_text="ThroughputInTupsPerSec", type="log", row=1, col=1)

fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceNoProc['src'], y=df_changingThreadsAndSourceNoProc['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=1
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt NoProc", row=2, col=1)
fig.update_yaxes(title_text="ThroughputInMBPerSec", type="log", row=2, col=1)


df_changingThreadsAndSourceNoProc["src"] = "W" + df_changingThreadsAndSourceNoProc["WorkerThreads"].astype(str) + '/S' + \
                                           df_changingThreadsAndSourceNoProc["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceNoProc['src'], y=df_changingThreadsAndSourceNoProc['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=1
)
fig.update_xaxes(title_text="WrkCnt & SrcCnt NoProc", row=3, col=1)
fig.update_yaxes(title_text="AvgTaskProcessingTime", type="log", row=3, col=1)



############################################# df_changingThreadsAndSourceTumblingWindowQuery #######################################################

df_changingThreadsAndSourceTumblingWindowQuery["src"] = "W" + df_changingThreadsAndSourceTumblingWindowQuery[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceTumblingWindowQuery["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceTumblingWindowQuery['src'],
               y=df_changingThreadsAndSourceTumblingWindowQuery['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=2
)
fig.update_xaxes(title_text="Tumbling Window", row=1, col=2)

fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceTumblingWindowQuery['src'],
               y=df_changingThreadsAndSourceTumblingWindowQuery['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=2
)
fig.update_xaxes(title_text="Tumbling Window", row=3, col=2)

df_changingThreadsAndSourceTumblingWindowQuery["src"] = "W" + df_changingThreadsAndSourceTumblingWindowQuery[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceTumblingWindowQuery["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceTumblingWindowQuery['src'],
               y=df_changingThreadsAndSourceTumblingWindowQuery['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=2
)
fig.update_xaxes(title_text="Tumbling Window", row=3, col=2)

##########
############################################# df_changingThreadsAndSourceSlidingWindowQuery #######################################################

df_changingThreadsAndSourceSlidingWindowQuery["src"] = "W" + df_changingThreadsAndSourceSlidingWindowQuery[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceSlidingWindowQuery["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceSlidingWindowQuery['src'],
               y=df_changingThreadsAndSourceSlidingWindowQuery['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=3
)
fig.update_xaxes(title_text="Sliding Window", row=1, col=3)

fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceSlidingWindowQuery['src'],
               y=df_changingThreadsAndSourceSlidingWindowQuery['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=3
)
fig.update_xaxes(title_text="Sliding Window", row=3, col=3)

df_changingThreadsAndSourceSlidingWindowQuery["src"] = "W" + df_changingThreadsAndSourceSlidingWindowQuery[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceSlidingWindowQuery["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceSlidingWindowQuery['src'],
               y=df_changingThreadsAndSourceSlidingWindowQuery['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=3
)
fig.update_xaxes(title_text="Sliding Window", row=3, col=3)
#######

############################################# df_changingThreadsAndSourceYSBWindowQuery #######################################################

df_changingThreadsAndSourceYSBWindowQuery["src"] = "W" + df_changingThreadsAndSourceYSBWindowQuery[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceYSBWindowQuery["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceYSBWindowQuery['src'],
               y=df_changingThreadsAndSourceYSBWindowQuery['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=4
)
fig.update_xaxes(title_text="YSB Query", row=1, col=4)

fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceYSBWindowQuery['src'],
               y=df_changingThreadsAndSourceYSBWindowQuery['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=4
)
fig.update_xaxes(title_text="YSB Query", row=3, col=4)

df_changingThreadsAndSourceYSBWindowQuery["src"] = "W" + df_changingThreadsAndSourceYSBWindowQuery[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceYSBWindowQuery["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceYSBWindowQuery['src'],
               y=df_changingThreadsAndSourceYSBWindowQuery['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=4
)
fig.update_xaxes(title_text="YSB Query", row=3, col=4)
####

############################################# changingThreadsAndSourceTumblingWindowJoinQuery #######################################################

changingThreadsAndSourceTumblingWindowJoinQuery["src"] = "W" + changingThreadsAndSourceTumblingWindowJoinQuery[
    "WorkerThreads"].astype(str) + '/S' + changingThreadsAndSourceTumblingWindowJoinQuery["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=changingThreadsAndSourceTumblingWindowJoinQuery['src'],
               y=changingThreadsAndSourceTumblingWindowJoinQuery['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=5
)
fig.update_xaxes(title_text="Tumbling Window Join", row=1, col=5)

fig.add_trace(
    go.Scatter(x=changingThreadsAndSourceTumblingWindowJoinQuery['src'],
               y=changingThreadsAndSourceTumblingWindowJoinQuery['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=5
)
fig.update_xaxes(title_text="Tumbling Window Join", row=3, col=5)

changingThreadsAndSourceTumblingWindowJoinQuery["src"] = "W" + changingThreadsAndSourceTumblingWindowJoinQuery[
    "WorkerThreads"].astype(str) + '/S' + changingThreadsAndSourceTumblingWindowJoinQuery["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=changingThreadsAndSourceTumblingWindowJoinQuery['src'],
               y=changingThreadsAndSourceTumblingWindowJoinQuery['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=5
)
fig.update_xaxes(title_text="Tumbling Window Join", row=3, col=5)
####


############################################# changingThreadsAndSourceSlidingWindowJoinQuery #######################################################

changingThreadsAndSourceSlidingWindowJoinQuery["src"] = "W" + changingThreadsAndSourceSlidingWindowJoinQuery[
    "WorkerThreads"].astype(str) + '/S' + changingThreadsAndSourceSlidingWindowJoinQuery["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=changingThreadsAndSourceSlidingWindowJoinQuery['src'],
               y=changingThreadsAndSourceSlidingWindowJoinQuery['ThroughputInTupsPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='red',
                         width=2),
               name="Throughput in Tuples/sec",
               showlegend=False
               ),
    row=1, col=6
)
fig.update_xaxes(title_text="Sliding Window Join", row=1, col=6)

fig.add_trace(
    go.Scatter(x=changingThreadsAndSourceSlidingWindowJoinQuery['src'],
               y=changingThreadsAndSourceSlidingWindowJoinQuery['ThroughputInMBPerSec'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='blue',
                         width=2),
               name="ThroughputInMBPerSec",
               showlegend=False
               ),
    row=2, col=6
)
fig.update_xaxes(title_text="Sliding Window Join", row=3, col=6)

changingThreadsAndSourceSlidingWindowJoinQuery["src"] = "W" + changingThreadsAndSourceSlidingWindowJoinQuery[
    "WorkerThreads"].astype(str) + '/S' + changingThreadsAndSourceSlidingWindowJoinQuery["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=changingThreadsAndSourceSlidingWindowJoinQuery['src'],
               y=changingThreadsAndSourceSlidingWindowJoinQuery['AvgTaskProcessingTime'],
               hoverinfo='x+y',
               mode='markers+lines',
               line=dict(color='green',
                         width=2),
               name="AvgTaskProcessingTime",
               showlegend=False
               ),
    row=3, col=6
)
fig.update_xaxes(title_text="Sliding Window Join", row=3, col=6)
####


fig.update_layout(barmode='overlay')
fig.update_layout(
    title={
        'text': "<b>NebulaStream Performance Numbers -- Different Workloads </b><br>"
                "<span style=\"font-size:0.6em\" line-height=1em;margin=-4px>Default Config(GlobalBufferPool=65536, LocalBufferPool=1024, BufferSize=4MB,  TupleSize=24Byte (3 Attributes))"
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
plotly.offline.plot(fig, filename='DifferentStateFullWorkloads.html')
