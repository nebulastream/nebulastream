import plotly.graph_objs as go
import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
import plotly

# df_changingBufferSize = pd.read_csv('./changingBufferSize.csv')
# df_changingGlobalBufferCnt = pd.read_csv('./changingGlobalBufferCnt.csv')
# df_changingLocalBufferSize = pd.read_csv('./changingLocalBufferSize.csv')
# df_changingSourceCnt = pd.read_csv('./changingSourceCnt.csv')
# df_changingThreadsAndSource = pd.read_csv('./changingThreadsAndSource.csv')
# df_changingThreadsCnt = pd.read_csv('./changingThreadsWithFixSource.csv')
# df_changingThreadsAndSourceOnlyFwdData = pd.read_csv('./changingThreadsAndSourceOnlyFwdData.csv')
df_test = pd.read_csv('./test.csv')
# df_test = pd.read_csv('./output.csv')

#
# import plotly.express as px
# df = px.data.tips()
# print(df.values)
# # fig = px.scatter(df, x="total_bill", y="tip", color="smoker", facet_col="sex")
# fig3 = px.bar(df_test, x=df_test["BufferSizeInBytes"], y=df_test["ThroughputInTupsPerSec"]
#                   , color=dict(
#         color=df_test["WorkerThreads"]
#     )
#                   )

#
# fig3 = px.line(df_test, x="BufferSizeInBytes", y="ThroughputInTupsPerSec",
#              # color='WorkerThreads',
#                facet_col='benchmarkName',
#                # facet_row='Latency',
#              height=400)
#
#
# fig3.show()
# fig4 = px.line(df_test, x="BufferSizeInBytes", y="Latency",
#                color='WorkerThreads',
#                facet_col='benchmarkName',
#                # facet_row='benchmarkName',
#                height=400)
# #
# # fig = make_subplots(rows=2, cols=2, shared_xaxes=True, shared_yaxes=True)
# # # trace = getattr(px, 'line')(df_test["BufferSizeInBytes"])["data"][0]
# # fig.add_trace(fig3["data"][0], row=1, col=1)
# # fig.add_trace(fig3["data"][1], row=2, col=1)
# #
# # fig.show()
# fig3.add_trace(fig4.data[0], row=1, col=1)
# fig3.show()
# exit()
# trace1 = fig3['data'][0]
# trace2 = fig3['data'][0]
#
# exit()
# def facetting_data(df, n_cols=3, to_plot='box'):
#     numeric_cols = df.select_dtypes('number').columns
#     n_rows = -(-len(numeric_cols) // n_cols)  # math.ceil in a fast way, without import
#     row_pos, col_pos = 1, 0
#     fig = make_subplots(rows=n_rows, cols=n_cols)
#
#     for col in numeric_cols:
#         # trace extracted from the fig
#         trace = getattr(px, to_plot)(df[col])["data"][0]
#         # auto selecting a position of the grid
#         if col_pos == n_cols: row_pos += 1
#         col_pos = col_pos + 1 if (col_pos < n_cols) else 1
#         # adding trace to the grid
#         fig.add_trace(trace, row=row_pos, col=col_pos)
#     return fig
#
# fig = facetting_data(df_test, to_plot='violin')
# # final tweaks
# fig.update_layout(width=1000, height=800, title='Violin per feature', title_x=0.5)
# fig.show()


# fig3.append_trace(fig4.data[0], row=2, col=2)
# layout = go.Layout()
# fig = go.Figure(data=[trace1,trace2], layout=layout)
# fig.add_traces([fig3.data[0], fig4.data[0]])

# plotly.offline.plot(fig3, filename='results.html')

#
# fig = go.Figure()
# fig.add_traces([fig3.data[0], fig4.data[0]])
#
# fig.layout.update(fig3.layout)
# fig.layout.update(fig4.layout)
#
# fig.show()


# figfull.append_trace(fig3, row=1, col=1)

# fig3.show()
# exit()
#
#
# fig = px.line(df_test, y="ThroughputInTupsPerSec", x="BufferSizeInBytes", color="WorkerThreads",
#               line_shape="spline", render_mode="svg",
#               color_discrete_sequence=px.colors.qualitative.G10,
#               title="Built-in G10 color sequence")
#
#
# trace1 = fig3['data'][0]
# figfull = make_subplots(rows=2, cols=2, shared_xaxes=True, shared_yaxes=True)
# figfull.append_trace(trace1, row=1, col=1)
# figfull.append_trace(trace1, row=1, col=1)


# figfull.add_trace(go.Scatter(x=df_test["BufferSizeInBytes"],
#                          y=df_test["ThroughputInTupsPerSec"],
#                          mode='lines+markers',
#                         color=""
#                          # line=dict(width=2, color="WorkerThreads"),
#                          # marker=dict(color=["WorkerThreads"]),
#                          # showlegend=False
#                          ),
#               row=1, col=2
#               )
#
# fig.update_layout(width=1000, height=800, title='% of variation in Happiness Score explained by each feature',
#                   title_x=0.5)


from plotly.graph_objs.scatter import Line

# trace11 = go.Scatter(x = df_test["BufferSizeInBytes"], y = df_test["ThroughputInTupsPerSec"], color=('markers'))
# trace11 = go.Bar(x=df_test['BufferSizeInBytes'], y=df_test['ThroughputInTupsPerSec'], color="WorkerThreads",
#                  # mode='lines',
#                  # mode="markers",
#                  showlegend=True,
#                  # marker=dict(
#                  #    color=df_test['WorkerThreads']
#                  # )
#                  )
# from plotly.subplots import make_subplots
#
# fig = make_subplots(rows=1, cols=2)
# # fig.append_trace(trace11, row=1, col=1)
# # # fig.append_trace(trace12, row=1, col=2)
# # # fig.append_trace(trace21, row=1, col=2)
# # # fig.append_trace(trace22, row=1, col=2)
# # fig.show()
#
# fig.add_trace(
#     go.Scatter(df_test, y="ThroughputInTupsPerSec", x="BufferSizeInBytes",
#                line_shape="spline", render_mode="svg",
#                color_discrete_sequence=px.colors.qualitative.G10,
#                title="Built-in G10 color sequence"
#                ),
#     row=1, col=1
#     # )
# )

# fig.add_trace(go.Scatter(x=df_test["BufferSizeInBytes"].index,
#                          y=df_test["ThroughputInTupsPerSec"],
#                          mode='WorkerThreads'
#                          # line=dict(width=2, color="WorkerThreads"),
#                          # marker=dict(color=["WorkerThreads"]),
#                          # showlegend=False
#                          ),
#               row=1, col=1
#               )
# fig.update_xaxes(title_text="BufferSizeInBytes", row=1, col=1)
# fig.update_yaxes(title_text="Throughput in Tuples/sec", row=1, col=1)
# fig = go.Figure(data=data, layout=layout)
import pandas as pd
import numpy as np
# #
# # # figfull.show()
# # # sample dataframe to match OPs structure
# df = pd.DataFrame({'Date' : [pd.Timestamp('20130102'), pd.Timestamp('20130102'),
#                              pd.Timestamp('20130103'), pd.Timestamp('20130103'),
#                              pd.Timestamp('20130103'), pd.Timestamp('20130103'),
#                              pd.Timestamp('20130103'), pd.Timestamp('20130103'),
#                              pd.Timestamp('20130104'), pd.Timestamp('20130104'),
#                              pd.Timestamp('20130105'),pd.Timestamp('20130106')],
#                    'ID' : [1, 3, 1, 2, 1 , 3,3,4,4,4,1,2],
#                    'Category' : pd.Categorical(["A","B","C","B","B","A",
#                                                 "A","A","B","C","B","A"  ])})
# # data munging to get OPs desired plot
# print(df)
# df = pd.pivot_table(df, values='ID', index=['Date'],columns='Category', aggfunc=np.sum)
# # df2 = df_test.pivot_table(df, values='ID', index=['Date'],columns='Category', aggfunc=np.sum)
# print(df)
# # ploty
# fig = go.Figure()# df = pd.DataFrame({'Date' : [pd.Timestamp('20130102'), pd.Timestamp('20130102'),
# #                              pd.Timestamp('20130103'), pd.Timestamp('20130103'),
# #                              pd.Timestamp('20130103'), pd.Timestamp('20130103'),
# #                              pd.Timestamp('20130103'), pd.Timestamp('20130103'),
# #                              pd.Timestamp('20130104'), pd.Timestamp('20130104'),
# #                              pd.Timestamp('20130105'),pd.Timestamp('20130106')],
#                    'ID' : [1, 3, 1, 2, 1 , 3,3,4,4,4,1,2],
#                    'Category' : pd.Categorical(["A","B","C","B","B","A",
#                                                 "A","A","B","C","B","A"  ])})
# # data munging to get OPs desired plot
# df = pd.pivot_table(df, values='ID', index=['Date'],columns='Category', aggfunc=np.sum)
# df2 = df_test.pivot_table(df, values='ID', index=['Date'],columns='Category', aggfunc=np.sum)
#
# # ploty
# fig = go.Figure()
# for col in df.columns:
#     fig.add_trace(go.Scatter(x=df.index, y=df[col].values,
#                              name = col,
#                              mode = 'markers+lines',
#                              line=dict(shape='linear'),
#                              connectgaps=True
#                              )
#                   )
# fig.show()
# # exit(0)
# for col in df.columns:
#     fig.add_trace(go.Scatter(x=df.index, y=df[col].values,
#                              name = col,
#                              mode = 'markers+lines',
#                              line=dict(shape='linear'),
#                              connectgaps=True
#                              )
#                   )
# fig.show()
# exit(0)
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

print(df_test)
df2 = pd.pivot_table(df_test, values='ThroughputInTupsPerSec', index=['BufferSizeInBytes'], columns='WorkerThreads',
                     aggfunc=np.sum)
# df2.reset_index().to_dict('rows')
print(df2)
#
# ########################################## df_changingBufferSize ##########################################################
# ploty

# go.Scatter(x=df_test['BufferSizeInBytes'].index, y=df_test['ThroughputInTupsPerSec'].values,
#            marker=dict(color=df_test['WorkerThreads']),
#            hoverinfo='x+y',
#            mode='markers+lines',WorkerThreads
#            # mode='markers+lines',
#            line=dict(color='red',
#                      width=1),
#            name="Throughput in Tuples/sec",
#            # legendgroup='group1',
#            showlegend=True
#            ),

for col in df2.columns:
    fig.add_trace(go.Scatter(x=df2.index, y=df2[col].values,
                             name=col,
                             mode='markers+lines',
                             line=dict(shape='linear'),
                             connectgaps=True
                             )
                  )
# go.Scatter(
#     name='Measurement 2',
#     x=df['Time'],
#     y=df['Velocity'],
#     mode='markers+lines',
#     marker=dict(color='blue', size=2),
#     showlegend=True
# ),
# fig.update_xaxes(title_text="BufferSizeInBytes", row=1, col=1)
# fig.update_yaxes(title_text="Throughput in Tuples/sec", row=1, col=1)
#
# fig.add_trace(
#     go.Scatter(x=df_changingBufferSize['BufferSizeInBytes'], y=df_changingBufferSize['ThroughputInMBPerSec'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='green',
#                          width=1),
#                name="Throughput in MB/s",
#                legendgroup='group2',
#                showlegend=False
#                ),
#     row=2, col=1
# )
# fig.update_xaxes(title_text="BufferSizeInBytes", row=2, col=1)
# fig.update_yaxes(title_text="ThroughputInMBPerSec", row=2, col=1)
#
# fig.add_trace(
#     go.Scatter(x=df_changingBufferSize['BufferSizeInBytes'], y=df_changingBufferSize['AvgLatencyInMs'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='blue',
#                          width=1),
#                name="Average Latency",
#                legendgroup='group3',
#                showlegend=False
#                ),
#     row=3, col=1
# )
# fig.update_xaxes(title_text="BufferSizeInBytes", row=3, col=1)
# fig.update_yaxes(title_text="AvgLatencyInMs", row=3, col=1)
#
# ############################################# df_changingGlobalBufferCnt #######################################################
# fig.add_trace(
#     go.Scatter(x=df_changingGlobalBufferCnt['NumberOfBuffersInGlobalBufferManager'], y=df_changingBufferSize['ThroughputInTupsPerSec'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='red',
#                          width=1),
#                name="Throughput in Tuples/sec",
#                legendgroup='group1',
#                showlegend=False
#                ),
#     row=1, col=2
# )
# fig.update_xaxes(title_text="NumberOfBuffersInGlobalBufferManager", row=1, col=2)
# fig.update_yaxes(title_text="Throughput in Tuples/sec", row=1, col=2)
#
# fig.add_trace(
#     go.Scatter(x=df_changingGlobalBufferCnt['NumberOfBuffersInGlobalBufferManager'], y=df_changingBufferSize['ThroughputInMBPerSec'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='green',
#                          width=1),
#                name="Throughput in MB/s",
#                legendgroup='group2',
#                showlegend=False
#                ),
#     row=2, col=2
# )
# fig.update_xaxes(title_text="NumberOfBuffersInGlobalBufferManager", row=2, col=2)
#
# fig.add_trace(
#     go.Scatter(x=df_changingGlobalBufferCnt['NumberOfBuffersInGlobalBufferManager'], y=df_changingBufferSize['AvgLatencyInMs'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='blue',
#                          width=1),
#                name="Average Latency",
#                legendgroup='group3',
#                showlegend=False
#                ),
#     row=3, col=2
# )
# fig.update_xaxes(title_text="NumberOfBuffersInGlobalBufferManager", row=3, col=2)
#
# ####################################################################################################
#
# ############################################# df_changingLocalBufferSize #######################################################
# fig.add_trace(
#     go.Scatter(x=df_changingLocalBufferSize['NumberOfBuffersInSourceLocalBufferPool'], y=df_changingBufferSize['ThroughputInTupsPerSec'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='red',
#                          width=1),
#                name="Throughput in Tuples/sec",
#                legendgroup='group1',
#                showlegend=False
#                ),
#     row=1, col=3
# )
# fig.update_xaxes(title_text="NumberOfBufferInLocalBufferPool", row=1, col=3)
#
# fig.add_trace(
#     go.Scatter(x=df_changingLocalBufferSize['NumberOfBuffersInSourceLocalBufferPool'], y=df_changingBufferSize['ThroughputInMBPerSec'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='green',
#                          width=1),
#                name="Throughput in MB/s",
#                legendgroup='group2',
#                showlegend=False
#                ),
#     row=2, col=3
# )
# fig.update_xaxes(title_text="NumberOfBufferInLocalBufferPool", row=2, col=3)
#
# fig.add_trace(
#     go.Scatter(x=df_changingLocalBufferSize['NumberOfBuffersInSourceLocalBufferPool'], y=df_changingBufferSize['AvgLatencyInMs'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='blue',
#                          width=1),
#                name="Average Latency",
#                legendgroup='group3',
#                showlegend=False
#                ),
#     row=3, col=3
# )
# fig.update_xaxes(title_text="NumberOfBufferInLocalBufferPool", row=3, col=3)
#
# ####################################################################################################
#
# ############################################# df_changingSourceCnt #######################################################
# fig.add_trace(
#     go.Scatter(x=df_changingSourceCnt['SourceCnt'], y=df_changingBufferSize['ThroughputInTupsPerSec'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='red',
#                          width=1),
#                name="Throughput in Tuples/sec",
#                legendgroup='group1',
#                showlegend=False
#                ),
#     row=1, col=4
# )
# fig.update_xaxes(title_text="SourceCnt", row=1, col=4)
#
# fig.add_trace(
#     go.Scatter(x=df_changingSourceCnt['SourceCnt'], y=df_changingBufferSize['ThroughputInMBPerSec'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='green',
#                          width=1),
#                name="Throughput in MB/s",
#                legendgroup='group2',
#                showlegend=False
#                ),
#     row=2, col=4
# )
# fig.update_xaxes(title_text="SourceCnt", row=2, col=4)
#
# fig.add_trace(
#     go.Scatter(x=df_changingSourceCnt['SourceCnt'], y=df_changingBufferSize['AvgLatencyInMs'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='blue',
#                          width=1),
#                name="Average Latency",
#                legendgroup='group3',
#                showlegend=False
#                ),
#     row=3, col=4
# )
# fig.update_xaxes(title_text="SourceCnt", row=3, col=4)
#
# ####################################################################################################
#
# ########################################## df_changingThreadsCnt ##########################################################
# fig.add_trace(
#     go.Scatter(x=df_changingThreadsCnt['WorkerThreads'], y=df_changingThreadsCnt['ThroughputInTupsPerSec'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='red',
#                          width=1),
#                name="Throughput in Tuples/sec",
#                legendgroup='group1',
#                showlegend=False
#                ),
#     row=1, col=5
# )
# fig.update_xaxes(title_text="WorkerThreads", row=1, col=5)
#
# fig.add_trace(
#     go.Scatter(x=df_changingThreadsCnt['WorkerThreads'], y=df_changingThreadsCnt['ThroughputInMBPerSec'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='green',
#                          width=1),
#                name="Throughput in MB/s",
#                legendgroup='group2',
#                showlegend=False
#                ),
#     row=2, col=5
# )
# fig.update_xaxes(title_text="WorkerThreads", row=2, col=5)
#
# fig.add_trace(
#     go.Scatter(x=df_changingThreadsCnt['WorkerThreads'], y=df_changingThreadsCnt['AvgLatencyInMs'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='blue',
#                          width=1),
#                name="Average Latency",
#                legendgroup='group3',
#                showlegend=False
#                ),
#     row=3, col=5
# )
# fig.update_xaxes(title_text="WorkerThreads", row=3, col=5)
#
#
# ############################################# df_changingThreadsAndSource #######################################################
#
# df_changingThreadsAndSource["src"] = "W" + df_changingThreadsAndSource["WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSource["SourceCnt"].astype(str)
# fig.add_trace(
#     go.Scatter(x=df_changingThreadsAndSource['src'], y=df_changingThreadsAndSource['ThroughputInTupsPerSec'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='red',
#                          width=1),
#                name="Throughput in Tuples/sec",
#                legendgroup='group1',
#                showlegend=False
#                ),
#     row=1, col=6
# )
# fig.update_xaxes(title_text="Worker Threads & SourceCnt", row=1, col=6)
#
# fig.add_trace(
#     go.Scatter(x=df_changingThreadsAndSource['src'], y=df_changingThreadsAndSource['ThroughputInMBPerSec'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='green',
#                          width=1),
#                name="ThroughputInMBPerSec",
#                legendgroup='group2',
#                showlegend=False
#                ),
#     row=2, col=6
# )
# fig.update_xaxes(title_text="Worker Threads & SourceCnt", row=2, col=6)
#
# df_changingThreadsAndSource["src"] = "W" + df_changingThreadsAndSource["WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSource["SourceCnt"].astype(str)
# fig.add_trace(
#     go.Scatter(x=df_changingThreadsAndSource['src'], y=df_changingThreadsAndSource['AvgLatencyInMs'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='blue',
#                          width=1),
#                name="AvgLatencyInMs",
#                legendgroup='group3',
#                showlegend=False
#                ),
#     row=3, col=6
# )
# fig.update_xaxes(title_text="Worker Threads & SourceCnt", row=3, col=6)
#
#
# ############################################# df_changingThreadsAndSourceOnlyFwdData #######################################################
#
# df_changingThreadsAndSourceOnlyFwdData["src"] = "W" + df_changingThreadsAndSourceOnlyFwdData["WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceOnlyFwdData["SourceCnt"].astype(str)
# fig.add_trace(
#     go.Scatter(x=df_changingThreadsAndSourceOnlyFwdData['src'], y=df_changingThreadsAndSourceOnlyFwdData['ThroughputInTupsPerSec'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='red',
#                          width=1),
#                name="Throughput in Tuples/sec",
#                legendgroup='group1',
#                showlegend=False
#                ),
#     row=1, col=7
# )
# fig.update_xaxes(title_text="Worker Threads & SourceCnt", row=1, col=7)
#
# fig.add_trace(
#     go.Scatter(x=df_changingThreadsAndSourceOnlyFwdData['src'], y=df_changingThreadsAndSourceOnlyFwdData['ThroughputInMBPerSec'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='green',
#                          width=1),
#                name="ThroughputInMBPerSec",
#                legendgroup='group2',
#                showlegend=False
#                ),
#     row=2, col=7
# )
# fig.update_xaxes(title_text="Worker Threads & SourceCnt", row=2, col=7)
#
# df_changingThreadsAndSourceOnlyFwdData["src"] = "W" + df_changingThreadsAndSourceOnlyFwdData["WorkerThreads"].astype(str) + '/S' + df_changingThreadsAndSourceOnlyFwdData["SourceCnt"].astype(str)
# fig.add_trace(
#     go.Scatter(x=df_changingThreadsAndSourceOnlyFwdData['src'], y=df_changingThreadsAndSourceOnlyFwdData['AvgLatencyInMs'],
#                hoverinfo='x+y',
#                mode='markers+lines',
#                line=dict(color='blue',
#                          width=1),
#                name="AvgLatencyInMs",
#                legendgroup='group3',
#                showlegend=False
#                ),
#     row=3, col=7
# )
# fig.update_xaxes(title_text="Worker Threads & SourceCnt", row=3, col=7)
#
# ####################################################################################################
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
