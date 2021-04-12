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

folder = "./"#in this filder
# folder = "/home/zeuchste/Dropbox/nes/o3/"
df_changingBufferSizeOrg = pd.read_csv(folder + 'changingSourceCnt.csv')
df_changingBufferSize2 = pd.read_csv(folder + 'changingSourceCnt2.csv')
#df_changingBufferSizeLong = pd.read_csv(folder + 'changingSourceCntLong.csv')

import plotly.graph_objects as go
import plotly

# ###############################################################
fig = make_subplots(
    rows=3, cols=3,
    column_widths=[0.8, 0.8, 0.8],
    row_heights=[1., 1., 1.],
    shared_xaxes=False,
    subplot_titles=[
        'Buffer Size Org',
        'Buffer Size New',
    ]
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
    8: 'violet',
    9: 'brown',
    10: '',
    11: 'plum',
    12: 'purple'
}

# Get the color-wheel
dictOfNamesSrd = {
    1: 'limegreen',
    2: 'brown',
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
df_changingBufferSizeOrg_pivot = pd.pivot_table(df_changingBufferSizeOrg, values='ThroughputInTupsPerSec',
                                                index=['SourceCnt'],
                                                columns='WorkerThreads',
                                                aggfunc=np.sum)


for i in range(len(df_changingBufferSizeOrg_pivot.columns)):
    col = df_changingBufferSizeOrg_pivot.columns[i]
    fig.add_trace(go.Scatter(x=df_changingBufferSizeOrg_pivot.index, y=df_changingBufferSizeOrg_pivot[col].values,
                             name=str(col),
                             # log_x=True,
                             hoverinfo='x+y',
                             mode='markers+lines',
                             line=dict(shape='linear',
                                       color=dictOfNames[col],
                                       width=1),
                             connectgaps=True,
                             showlegend=True,
                             ),
                  row=1, col=1
                  )
fig.update_xaxes(title_text="BufferSizeInBytes", type="log", row=1, col=1)
fig.update_yaxes(title_text="ThroughputInTupsPerSec", type="log", row=1, col=1)

df_changingBufferSizeOrg_pivot = pd.pivot_table(df_changingBufferSizeOrg, values='ThroughputInMBPerSec',
                                           index=['SourceCnt'],
                                            columns='WorkerThreads',
                                            aggfunc=np.sum)

for i in range(len(df_changingBufferSizeOrg_pivot.columns)):
    col = df_changingBufferSizeOrg_pivot.columns[i]
    fig.add_trace(go.Scatter(x=df_changingBufferSizeOrg_pivot.index, y=df_changingBufferSizeOrg_pivot[col].values,
                             # name=str(col),
                             hoverinfo='x+y',
                             mode='markers+lines',
                             line=dict(shape='linear', color=dictOfNames[col],
                                       width=1),
                             connectgaps=True,
                             showlegend=False,
                             ),
                  row=2, col=1
                  )
fig.update_xaxes(title_text="BufferSizeInBytes", type="log", row=2, col=1)
fig.update_yaxes(title_text="ThroughputInMBPerSec", type="log", row=2, col=1)

df_changingBufferSizeOrg_pivot = pd.pivot_table(df_changingBufferSizeOrg, values='AvgLatencyInMs',
                                           index=['SourceCnt'],
                                            columns='WorkerThreads',
                                            aggfunc=np.sum)

for i in range(len(df_changingBufferSizeOrg_pivot.columns)):
    col = df_changingBufferSizeOrg_pivot.columns[i]
    fig.add_trace(go.Scatter(x=df_changingBufferSizeOrg_pivot.index, y=df_changingBufferSizeOrg_pivot[col].values,
                             # name=str(col),
                             hoverinfo='x+y',
                             mode='markers+lines',
                             line=dict(shape='linear', color=dictOfNames[col],
                                       width=1),
                             connectgaps=True,
                             showlegend=False,
                             ),
                  row=3, col=1
                  )
fig.update_xaxes(title_text="BufferSizeInBytes", type="log", row=3, col=1)
fig.update_yaxes(title_text="AvgLatencyInMs", type="log", row=3, col=1)


# ########################################## df_changingBufferSize ##########################################################
df_changingBufferSize2_pivot = pd.pivot_table(df_changingBufferSize2, values='ThroughputInTupsPerSec',
                                                   index=['SourceCnt'],
                                            columns='WorkerThreads',
                                            aggfunc=np.sum)

for i in range(len(df_changingBufferSize2_pivot.columns)):
    col = df_changingBufferSize2_pivot.columns[i]
    fig.add_trace(go.Scatter(x=df_changingBufferSize2_pivot.index, y=df_changingBufferSize2_pivot[col].values,
                             name=str(col),
                             # log_x=True,
                             hoverinfo='x+y',
                             mode='markers+lines',
                             line=dict(shape='linear',
                                       color=dictOfNames[col],
                                       width=1),
                             connectgaps=True,
                             showlegend=False,
                             ),
                  row=1, col=2
                  )
fig.update_xaxes(title_text="BufferSizeInBytes", type="log", row=1, col=2)
fig.update_yaxes(title_text="ThroughputInTupsPerSec", type="log", row=1, col=2)

df_changingBufferSize2_pivot = pd.pivot_table(df_changingBufferSize2, values='ThroughputInMBPerSec',
                                                 index=['SourceCnt'],
                                            columns='WorkerThreads',
                                            aggfunc=np.sum)
for i in range(len(df_changingBufferSize2_pivot.columns)):
    col = df_changingBufferSize2_pivot.columns[i]
    fig.add_trace(go.Scatter(x=df_changingBufferSize2_pivot.index, y=df_changingBufferSize2_pivot[col].values,
                             # name=str(col),
                             hoverinfo='x+y',
                             mode='markers+lines',
                             line=dict(shape='linear', color=dictOfNames[col],
                                       width=1),
                             connectgaps=True,
                             showlegend=False,
                             ),
                  row=2, col=2
                  )
fig.update_xaxes(title_text="BufferSizeInBytes", type="log", row=2, col=2)
fig.update_yaxes(title_text="ThroughputInMBPerSec", type="log", row=2, col=2)

df_changingBufferSize2_pivot = pd.pivot_table(df_changingBufferSize2, values='AvgLatencyInMs',
                                                   index=['SourceCnt'],
                                            columns='WorkerThreads',
                                            aggfunc=np.sum)
for i in range(len(df_changingBufferSize2_pivot.columns)):
    col = df_changingBufferSize2_pivot.columns[i]
    fig.add_trace(go.Scatter(x=df_changingBufferSize2_pivot.index, y=df_changingBufferSize2_pivot[col].values,
                             # name=str(col),
                             hoverinfo='x+y',
                             mode='markers+lines',
                             line=dict(shape='linear', color=dictOfNames[col],
                                       width=1),
                             connectgaps=True,
                             showlegend=False,
                             ),
                  row=3, col=2
                  )
fig.update_xaxes(title_text="BufferSizeInBytes", type="log", row=3, col=2)
fig.update_yaxes(title_text="AvgLatencyInMs", type="log", row=3, col=2)

#
# # ########################################## df_changingBufferSize ##########################################################
# df_changingBufferSizeLong_pivot = pd.pivot_table(df_changingBufferSizeLong, values='ThroughputInTupsPerSec',
#                                                  index=['SourceCnt'],
#                                             columns='WorkerThreads',
#                                             aggfunc=np.sum)
#
# for i in range(len(df_changingBufferSizeLong_pivot.columns)):
#     col = df_changingBufferSizeLong_pivot.columns[i]
#     fig.add_trace(go.Scatter(x=df_changingBufferSizeLong_pivot.index, y=df_changingBufferSizeLong_pivot[col].values,
#                              name=str(col),
#                              # log_x=True,
#                              hoverinfo='x+y',
#                              mode='markers+lines',
#                              line=dict(shape='linear',
#                                        color=dictOfNames[col],
#                                        width=1),
#                              connectgaps=True,
#                              showlegend=False,
#                              ),
#                   row=1, col=3
#                   )
# fig.update_xaxes(title_text="BufferSizeInBytes", type="log", row=1, col=3)
# fig.update_yaxes(title_text="ThroughputInTupsPerSec", type="log", row=1, col=3)
#
# df_changingBufferSizeLong_pivot = pd.pivot_table(df_changingBufferSizeLong, values='ThroughputInMBPerSec',
#                                                 index=['SourceCnt'],
#                                             columns='WorkerThreads',
#                                             aggfunc=np.sum)
# for i in range(len(df_changingBufferSizeLong_pivot.columns)):
#     col = df_changingBufferSizeLong_pivot.columns[i]
#     fig.add_trace(go.Scatter(x=df_changingBufferSizeLong_pivot.index, y=df_changingBufferSizeLong_pivot[col].values,
#                              # name=str(col),
#                              hoverinfo='x+y',
#                              mode='markers+lines',
#                              line=dict(shape='linear', color=dictOfNames[col],
#                                        width=1),
#                              connectgaps=True,
#                              showlegend=False,
#                              ),
#                   row=2, col=3
#                   )
# fig.update_xaxes(title_text="BufferSizeInBytes", type="log", row=2, col=3)
# fig.update_yaxes(title_text="ThroughputInMBPerSec", type="log", row=2, col=3)
#
# df_changingBufferSizeLong_pivot = pd.pivot_table(df_changingBufferSizeLong, values='AvgLatencyInMs',
#                                                index=['SourceCnt'],
#                                             columns='WorkerThreads',
#                                             aggfunc=np.sum)
# for i in range(len(df_changingBufferSizeLong_pivot.columns)):
#     col = df_changingBufferSizeLong_pivot.columns[i]
#     fig.add_trace(go.Scatter(x=df_changingBufferSizeLong_pivot.index, y=df_changingBufferSizeLong_pivot[col].values,
#                              # name=str(col),
#                              hoverinfo='x+y',
#                              mode='markers+lines',
#                              line=dict(shape='linear', color=dictOfNames[col],
#                                        width=1),
#                              connectgaps=True,
#                              showlegend=False,
#                              ),
#                   row=3, col=3
#                   )
# fig.update_xaxes(title_text="BufferSizeInBytes", type="log", row=3, col=3)
# fig.update_yaxes(title_text="AvgLatencyInMs", type="log", row=3, col=3)

####################################################################################################
fig.update_layout(barmode='overlay')
fig.update_layout(
    title={
        'text': "<b>NebulaStream Performance Numbers </b><br>"
                "<span style=\"font-size:0.6em\">Default Config(GlobalBufferPool=65536, LocalBufferPool=1024, BufferSize=4MB, Threads=1, Source=1, TupleSize=24Byte, Query: Only Forward)"
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
# fig.update_layout(legend_title_text='Worker <br> Sources')

fig.update_layout(legend=dict(
    orientation="h",
    yanchor="bottom",
    y=1.06,
    xanchor="right",
    x=1,
    font=dict(
        family="Courier New, monospace",
        size=20,
        color="RebeccaPurple"
    )
))

###############################################################

plotly.offline.plot(fig, filename='results.html')
