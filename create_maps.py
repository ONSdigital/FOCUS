import json
import random
from bokeh.models import HoverTool
from bokeh.plotting import figure, show, output_file, ColumnDataSource

with open('LAs.geojson') as base_map:
    map_data = json.load(base_map)

rnd_values = []
LA_names = []
LA_xs = []
LA_ys = []
colors = ["#F1EEF6", "#D4B9DA", "#C994C7", "#DF65B0", "#DD1C77", "#980043"]

for feature in map_data['features']:
    print(feature['properties']['name'])
    LA_names.append(str(feature['properties']['name']))

    sub_LA_xs = []
    sub_LA_ys = []

    for coord in feature['geometry']['coordinates'][0][0]:

        sub_LA_xs.append((coord[0]))
        sub_LA_ys.append((coord[1]))

    LA_xs.append(sub_LA_xs)
    LA_ys.append(sub_LA_ys)


for item in LA_names:
    rnd_values.append((random.randint(0, 15)))


LA_colors = [colors[int(rate/3)] for rate in rnd_values]

source = ColumnDataSource(data=dict(
    x=LA_xs,
    y=LA_ys,
    color=LA_colors,
    name=LA_names,
    rate=rnd_values,
))

TOOLS = "pan,wheel_zoom,box_zoom,reset,hover,save"

p = figure(title="example", tools=TOOLS)

p.patches('x', 'y', source=source,
          fill_color='color', fill_alpha=0.7,
          line_color="white", line_width=0.5)

hover = p.select_one(HoverTool)
hover.point_policy = "follow_mouse"
hover.tooltips = [
    ("Name", "@name"),
    ("Unemployment rate)", "@rnd_values%"),
    ("(Long, Lat)", "($x, $y)"),
]

output_file("test.html", title="test LA example")

show(p)


