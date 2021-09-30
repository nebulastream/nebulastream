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

# set this if you run with DNES_BENCplotResultsForCopyOnly.pyHMARKS_DETAILED_LATENCY_MEASUREMENT
# folder = "./"#in this folder
folder = "./"

df_latencyWrk1 = pd.read_csv(folder + 'latencyW1.log')
df_latencyWrk2 = pd.read_csv(folder + 'latencyW2.log')
df_latencyWrk4 = pd.read_csv(folder + 'latencyW4.log')
df_latencyWrk8 = pd.read_csv(folder + 'latencyW8.log')
df_latencyWrk12 = pd.read_csv(folder + 'latencyW12.log')

import plotly.graph_objects as go
import plotly

# ###############################################################
fig = make_subplots(
    rows=1, cols=5,
    # vertical_spacing=0.05,
    # horizontal_spacing=0.05,
    column_widths=[3, 3, 3, 3, 3],
    row_heights=[2],
    shared_xaxes=False,
    subplot_titles=[
        'Latency TimeSeries -- 1Wrk,NSrc',
        'Latency TimeSeries -- 2Wrk,NSrc',
        'Latency TimeSeries -- 4Wrk,NSrc',
        'Latency TimeSeries -- 8Wrk,NSrc',
        'Latency TimeSeries -- 12Wrk,NSrc',
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

########################################## Latency measurements ##########################################################
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
        row=1, col=1
    )
fig.update_xaxes(title_text="ts (Wrk 1)", row=1, col=1)
fig.update_yaxes(title_text="Latency in MS <br>Legend=Src-X", row=1, col=1)

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
    , row=1, col=1)

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
        row=1, col=2
    )
fig.update_xaxes(title_text="ts (Wrk 2)", row=1, col=2)

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
    , row=1, col=2)

# fig.add_annotation(go.layout.Annotation(
#     x=50,
#     y=df_latencyWrk2['latency'].max() * 1.2,
#     yref='paper',
#     showarrow=True,
#     text='Start'
# )
#     , row=1, col=2)

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
        row=1, col=3
    )
fig.update_xaxes(title_text="ts (Wrk 4)", row=1, col=3)
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
    , row=1, col=3)

# fig.add_annotation(go.layout.Annotation(
#     x=50,
#     y=df_latencyWrk4['latency'].max() * 1.2,
#     yref='paper',
#     showarrow=True,
#     text='Start'
# )
#     , row=1, col=3)

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
        row=1, col=4
    )
fig.update_xaxes(title_text="ts (Wrk 8)", row=1, col=4)
fig.update_yaxes(row=1, col=4)  # type="log",
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
    , row=1, col=4)

# fig.add_annotation(go.layout.Annotation(
#     x=50,
#     y=df_latencyWrk8['latency'].max() * 1.2,
#     yref='paper',
#     showarrow=True,
#     text='Start'
# )
#     , row=1, col=4)


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
        row=1, col=5
    )
fig.update_xaxes(title_text="ts (Wrk 12)", row=1, col=5)
fig.update_yaxes(row=1, col=5)  # type="log",
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
    , row=1, col=5)

  # fig.add_annotation(go.layout.Annotation(
  #     x=50,
  #     y=df_latencyWrk8['latency'].max() * 1.2,
  #     yref='paper',
  #     showarrow=True,
  #     text='Start'
  # )
  #     , row=1, col=4)

fig.update_layout(barmode='overlay')
fig.update_layout(
    title={
        'text': "<b>NebulaStream Performance Numbers Latency NoProc</b><br>"
                "<span style=\"font-size:0.6em\" line-height=1em;margin=-6px>Default Config(GlobalBufferPool=65536, LocalBufferPool=1024, BufferSize=4MB, TupleSize=24Byte, Query: Only Forward)"
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
plotly.offline.plot(fig, filename='copyOnlyBaselineLatency.html')
