"""module for creating colour coded maps"""

import json
import csv
import os
from bokeh.models import HoverTool
from bokeh.plotting import figure, show, output_file, ColumnDataSource
import math
import numpy as np


# need a better way of setting color levels
def set_colour_level(max_val, min_val, rate):

    steps = int((max_val - min_val)/4)
    i = 1

    while True:
        if rate < min_val + (i*steps):
            break
        elif math.isnan(rate):
            i = 0
            break
        i += 1
    return i


# JSON file is the map data, data_file is the data to be attached to the map. A common key is needed to link them.
def create_choropleth(json_file, data_file):

    with open(json_file) as base_map:
        map_data = json.load(base_map)

    with open(os.path.join(os.getcwd(), "outputs", data_file), 'r') as f:

        reader = csv.reader(f)
        next(reader)
        my_results = list(reader)
        my_results_dict = {rows[0]: rows[1] for rows in my_results}

    results = []
    district_names = []
    district_xs = []
    district_ys = []
    colors = ["#CCCCCC", "#D4B9DA", "#C994C7", "#DF65B0", "#DD1C77", "#980043"]
    name_key = 'LAD11NM'

    for feature in map_data['features']:

        district_names.append(str(feature['properties'][name_key]))
        sub_xs = []
        sub_ys = []


        if str(feature['properties'][name_key]) in my_results_dict:
            results.append(float(my_results_dict[str(feature['properties'][name_key])])*100)
        else:
            results.append(float('nan'))

        if feature['geometry']['type'] == 'Polygon':
            temp_list = feature['geometry']['coordinates'][0]
        else:
            temp_list = feature['geometry']['coordinates'][0][0]
            #temp_list2 = feature['geometry']['coordinates'][1][0]

            #for row in temp_list2:
                #temp_list.append(row)

        for coord in temp_list:
            try:
                sub_xs.append((coord[0]))
                sub_ys.append((coord[1]))
            except IOError as e:
                print(e)

        district_xs.append(sub_xs)
        district_ys.append(sub_ys)

    district_colors = [colors[set_colour_level(np.nanmax(results), np.nanmin(results), rate)] for rate in results]

    source = ColumnDataSource(data=dict(
        x=district_xs,
        y=district_ys,
        color=district_colors,
        name=district_names,
        rate=results,
    ))

    tools = "pan,wheel_zoom,box_zoom,reset,hover,save"

    p = figure(title="Response rates by LA", tools=tools)

    p.patches('x', 'y', source=source,
              fill_color='color', fill_alpha=0.7,
              line_color="grey", line_width=0.3)

    hover = p.select_one(HoverTool)
    hover.point_policy = "follow_mouse"
    hover.tooltips = [
        ("Name", "@name"),
        ("Return rate)", "@rate%"),
    ]

    output_file("Return rate by LA.html", title="Return rate by LA")

    show(p)


