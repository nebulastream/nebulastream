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
df_changingThreadsAndSourceSourceSink = pd.read_csv(folder + 'changingThreadsAndSourceBaselineSourceSinkLambdaMode.csv')
df_changingThreadsAndSourceReadOnly = pd.read_csv(folder + 'changingThreadsAndSourceBaselineReadOnlyLambdaMode.csv')
df_changingThreadsAndSourceReadModifyWrite = pd.read_csv(folder + 'changingThreadsAndSourceBaselineReadModifyWriteLambdaMode.csv')

df_changingThreadsAndSourceMedSelectivity = pd.read_csv(folder + 'changingThreadsAndSourceMedSelectivityLambdaMode.csv')
df_changingThreadsAndSourceHighSelectivity = pd.read_csv(folder + 'changingThreadsAndSourceHighSelectivityLambdaMode.csv')
df_changingThreadsAndSourceLowSelectivity = pd.read_csv(folder + 'changingThreadsAndSourceLowSelectivityLambdaMode.csv')

df_changingThreadsAndSourceProjectionOneOut = pd.read_csv(folder + 'changingThreadsAndSourceProjectionOneOutLambdaMode.csv')
df_changingThreadsAndSourceProjectionTwoOut = pd.read_csv(folder + 'changingThreadsAndSourceProjectionTwoOutLambdaMode.csv')

#df_changingThreadsAndSourceMapOneField = pd.read_csv(folder + 'changingThreadsAndSourceMapOneFieldLambdaMode.csv')
df_changingThreadsAndSourceMapOneField = pd.read_csv(folder + 'changingThreadsAndSourceMapTwoFieldLambdaMode.csv')
df_changingThreadsAndSourceMapTwoField = pd.read_csv(folder + 'changingThreadsAndSourceMapTwoFieldLambdaMode.csv')

import plotly.graph_objects as go
import plotly

# ###############################################################
fig = make_subplots(
    rows=3, cols=10,
    column_widths=[3, 3, 3, 3, 3, 3, 3, 3,3,3],
    row_heights=[1, 1, 1],
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

df_changingThreadsAndSourceSourceSink["src"] = "W" + df_changingThreadsAndSourceSourceSink["WorkerThreads"].astype(str) + '/S' + \
                                               df_changingThreadsAndSourceSourceSink["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceSourceSink['src'],
               y=df_changingThreadsAndSourceSourceSink['ThroughputInTupsPerSec'],
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

fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceSourceSink['src'], y=df_changingThreadsAndSourceSourceSink['ThroughputInMBPerSec'],
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


df_changingThreadsAndSourceSourceSink["src"] = "W" + df_changingThreadsAndSourceSourceSink["WorkerThreads"].astype(str) + '/S' + \
                                               df_changingThreadsAndSourceSourceSink["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceSourceSink['src'], y=df_changingThreadsAndSourceSourceSink['AvgTaskProcessingTime'],
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



############################################# df_changingThreadsAndSourceReadOnly #######################################################

df_changingThreadsAndSourceReadOnly["src"] = "W" + df_changingThreadsAndSourceReadOnly["WorkerThreads"].astype(str) + '/S' + \
                                             df_changingThreadsAndSourceReadOnly["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceReadOnly['src'],
               y=df_changingThreadsAndSourceReadOnly['ThroughputInTupsPerSec'],
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
    go.Scatter(x=df_changingThreadsAndSourceReadOnly['src'], y=df_changingThreadsAndSourceReadOnly['ThroughputInMBPerSec'],
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


df_changingThreadsAndSourceReadOnly["src"] = "W" + df_changingThreadsAndSourceReadOnly["WorkerThreads"].astype(str) + '/S' + \
                                             df_changingThreadsAndSourceReadOnly["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceReadOnly['src'], y=df_changingThreadsAndSourceReadOnly['AvgTaskProcessingTime'],
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



############################################# df_changingThreadsAndSourceReadModifyWrite #######################################################

df_changingThreadsAndSourceReadModifyWrite["src"] = "W" + df_changingThreadsAndSourceReadModifyWrite["WorkerThreads"].astype(str) + '/S' + \
                                                    df_changingThreadsAndSourceReadModifyWrite["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceReadModifyWrite['src'],
               y=df_changingThreadsAndSourceReadModifyWrite['ThroughputInTupsPerSec'],
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
    go.Scatter(x=df_changingThreadsAndSourceReadModifyWrite['src'], y=df_changingThreadsAndSourceReadModifyWrite['ThroughputInMBPerSec'],
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


df_changingThreadsAndSourceReadModifyWrite["src"] = "W" + df_changingThreadsAndSourceReadModifyWrite["WorkerThreads"].astype(str) + '/S' + \
                                                    df_changingThreadsAndSourceReadModifyWrite["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceReadModifyWrite['src'], y=df_changingThreadsAndSourceReadModifyWrite['AvgTaskProcessingTime'],
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
    row=1, col=4
)
fig.update_xaxes(title_text="Filter (10%)", row=1, col=4)

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
    row=2, col=4
)
fig.update_xaxes(title_text="Filter (10%)", row=2, col=4)

df_changingThreadsAndSourceLowSelectivity["src"] = "W" + df_changingThreadsAndSourceLowSelectivity[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceLowSelectivity["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceLowSelectivity['src'],
               y=df_changingThreadsAndSourceLowSelectivity['AvgTaskProcessingTime'],
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
    row=1, col=5
)
fig.update_xaxes(title_text="Filter (50%)", row=1, col=5)

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
    row=2, col=5
)
fig.update_xaxes(title_text="Filter (50%)", row=2, col=5)

df_changingThreadsAndSourceMedSelectivity["src"] = "W" + df_changingThreadsAndSourceMedSelectivity[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceMedSelectivity["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceMedSelectivity['src'],
               y=df_changingThreadsAndSourceMedSelectivity['AvgTaskProcessingTime'],
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
    row=1, col=6
)
fig.update_xaxes(title_text="Filter (90%)", row=1, col=6)

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
    row=2, col=6
)
fig.update_xaxes(title_text="Filter (90%)", row=2, col=6)

df_changingThreadsAndSourceHighSelectivity["src"] = "W" + df_changingThreadsAndSourceHighSelectivity[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceHighSelectivity["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceHighSelectivity['src'],
               y=df_changingThreadsAndSourceHighSelectivity['AvgTaskProcessingTime'],
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


############################################# df_changingThreadsAndSourceProjectionOneOut #######################################################

df_changingThreadsAndSourceProjectionOneOut["src"] = "W" + df_changingThreadsAndSourceProjectionOneOut[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceProjectionOneOut["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceProjectionOneOut['src'],
               y=df_changingThreadsAndSourceProjectionOneOut['ThroughputInTupsPerSec'],
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
    go.Scatter(x=df_changingThreadsAndSourceProjectionOneOut['src'],
               y=df_changingThreadsAndSourceProjectionOneOut['ThroughputInMBPerSec'],
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

df_changingThreadsAndSourceProjectionOneOut["src"] = "W" + df_changingThreadsAndSourceProjectionOneOut[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceProjectionOneOut["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceProjectionOneOut['src'],
               y=df_changingThreadsAndSourceProjectionOneOut['AvgTaskProcessingTime'],
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


############################################# df_changingThreadsAndSourceProjectionTwoOut #######################################################

df_changingThreadsAndSourceProjectionTwoOut["src"] = "W" + df_changingThreadsAndSourceProjectionTwoOut[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceProjectionTwoOut["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceProjectionTwoOut['src'],
               y=df_changingThreadsAndSourceProjectionTwoOut['ThroughputInTupsPerSec'],
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
    go.Scatter(x=df_changingThreadsAndSourceProjectionTwoOut['src'],
               y=df_changingThreadsAndSourceProjectionTwoOut['ThroughputInMBPerSec'],
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

df_changingThreadsAndSourceProjectionTwoOut["src"] = "W" + df_changingThreadsAndSourceProjectionTwoOut[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceProjectionTwoOut["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceProjectionTwoOut['src'],
               y=df_changingThreadsAndSourceProjectionTwoOut['AvgTaskProcessingTime'],
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


############################################# df_changingThreadsAndSourceMapOneField #######################################################

df_changingThreadsAndSourceMapOneField["src"] = "W" + df_changingThreadsAndSourceMapOneField[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceMapOneField["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceMapOneField['src'],
               y=df_changingThreadsAndSourceMapOneField['ThroughputInTupsPerSec'],
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
    go.Scatter(x=df_changingThreadsAndSourceMapOneField['src'],
               y=df_changingThreadsAndSourceMapOneField['ThroughputInMBPerSec'],
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

df_changingThreadsAndSourceMapOneField["src"] = "W" + df_changingThreadsAndSourceMapOneField[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceMapOneField["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceMapOneField['src'],
               y=df_changingThreadsAndSourceMapOneField['AvgTaskProcessingTime'],
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


############################################# df_changingThreadsAndSourceMapTwoField #######################################################

df_changingThreadsAndSourceMapTwoField["src"] = "W" + df_changingThreadsAndSourceMapTwoField[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceMapTwoField["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceMapTwoField['src'],
               y=df_changingThreadsAndSourceMapTwoField['ThroughputInTupsPerSec'],
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
    go.Scatter(x=df_changingThreadsAndSourceMapTwoField['src'],
               y=df_changingThreadsAndSourceMapTwoField['ThroughputInMBPerSec'],
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

df_changingThreadsAndSourceMapTwoField["src"] = "W" + df_changingThreadsAndSourceMapTwoField[
    "WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceMapTwoField["SourceCnt"].astype(str)
fig.add_trace(
    go.Scatter(x=df_changingThreadsAndSourceMapTwoField['src'],
               y=df_changingThreadsAndSourceMapTwoField['AvgTaskProcessingTime'],
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
plotly.offline.plot(fig, filename='StateLessQueriesLambdaMode.html')
