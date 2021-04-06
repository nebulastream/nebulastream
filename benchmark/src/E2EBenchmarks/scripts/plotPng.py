import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psutil
# import plotly.io as pio
# pio.renderers.default = "png"

df = pd.read_csv('changingBufferSize.csv')

# fig = px.line(df, x="BufferSizeInBytes", y="ThroughputInTupsPerSec", title='changingBufferSize -- throughput', mode='lines+markers')
# fig.show()
#
# fig = go.Figure(go.Scatter(x = df['BufferSizeInBytes'], y = df['ThroughputInTupsPerSec'],
#                            name='changingBufferSize -- throughput'))

fig = px.scatter(df, x="BufferSizeInBytes", y="ThroughputInTupsPerSec", title="changingBufferSize")
fig.data[0].update(mode='markers+lines')
#
# fig.update_layout(title='',
#                   plot_bgcolor='rgb(230, 230,230)',
#                   showlegend=True,
#                   ='markers+lines')

# fig.show()
fig.write_image("res.png")
import plotly
#
#
# def figures_to_html(figs, filename="dashboard.html"):
#     dashboard = open(filename, 'w')
#     dashboard.write("<html><head></head><body>" + "\n")
#     for fig in figs:
#         inner_html = fig.to_html().split('<body>')[1].split('</body>')[0]
#         dashboard.write(inner_html)
#     dashboard.write("</body></html>" + "\n")
#

# Example figures
import plotly.express as px
from plotly.offline import plot
from plotly.subplots import make_subplots
import plotly.graph_objects as go

#
# # fig1 = px.line(gapminder, x="year", y="lifeExp", title='Life expectancy in Canada')
# # gapminder = px.data.gapminder().query("continent=='Oceania'")
#
# fig1 = px.scatter(df, x="BufferSizeInBytes", y="ThroughputInTupsPerSec", title="changingBufferSize")
# fig1.data[0].update(mode='markers+lines')
# # fig2 = px.line(gapminder, x="year", y="lifeExp", color='country')
# # gapminder = px.data.gapminder().query("continent != 'Asia'")
# # fig3 = px.line(gapminder, x="year", y="lifeExp", color="continent",
# #                line_group="country", hover_name="country")
#
# figures_to_html([fig1, fig1, fig1])
