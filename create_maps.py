"""module for creating colour coded maps"""

import json
import csv
import os
from bokeh.models import HoverTool, GeoJSONDataSource
from bokeh.plotting import figure, show, output_file, ColumnDataSource
import math
import numpy as np


def set_colour_level(rate):

    i = 0

    if math.isnan(rate) is True:
        i = 0
    elif rate < 80:
        i = 1
    elif rate < 85:
        i = 2
    elif rate < 90:
        i = 3
    elif rate < 95:
        i = 4
    elif rate <= 100:
        i = 5

    return i


def create_choropleth(json_file, shade_data_file, sup_data_file):


    # read in geojson map file
    with open(json_file) as base_map:
        map_data = json.load(base_map)

    # read in data that will define shading
    with open(os.path.join(os.getcwd(), shade_data_file), 'r') as shade_data:

        reader = csv.reader(shade_data)
        next(reader)
        my_results = list(reader)
        my_shade_dict = {rows[0]: rows[1] for rows in my_results}

    # read in some supplementary info
    with open(os.path.join(os.getcwd(), sup_data_file), 'r') as supp_data:

        reader = csv.reader(supp_data)
        next(reader)
        my_results = list(reader)
        my_supp_dict = {rows[0]: rows[1:] for rows in my_results}

    results = []
    households1 = []
    households2 = []
    households3 = []
    households4 = []
    households5 = []
    areas = []
    district_names = []
    district_xs = []
    district_ys = []
    colors = ["#CCCCCC", "#980043", "#DD1C77", "#DF65B0",  "#C994C7", "#D4B9DA"]

    name_key = 'LAD11NM'  # 'LSOA11NM' # 'LAD11NM'
    ID_key = 'LAD11CD'  # 'LSOA11CD' # 'LAD11CD'

    for feature in map_data['features']:

        if feature['geometry']['type'] == 'Polygon':

            district_names.append(str(feature['properties'][name_key]))

            sub_xs = []
            sub_ys = []

            if str(feature['properties'][ID_key]) in my_shade_dict:
                results.append(float(my_shade_dict[str(feature['properties'][ID_key])])*100)
                households1.append(int(my_supp_dict[str(feature['properties'][ID_key])][1])*100)
                households2.append(int(my_supp_dict[str(feature['properties'][ID_key])][2])*100)
                households3.append(int(my_supp_dict[str(feature['properties'][ID_key])][3])*100)
                households4.append(int(my_supp_dict[str(feature['properties'][ID_key])][4])*100)
                households5.append(int(my_supp_dict[str(feature['properties'][ID_key])][5])*100)
                areas.append(my_supp_dict[str(feature['properties'][ID_key])][6])

            else:
                results.append(float('nan'))
                households1.append(float('nan'))
                households2.append(float('nan'))
                households3.append(float('nan'))
                households4.append(float('nan'))
                households5.append(float('nan'))
                areas.append(float('nan'))

            temp_list = feature['geometry']['coordinates'][0]

            for coord in temp_list:

                sub_xs.append((coord[0]))
                sub_ys.append((coord[1]))

            district_xs.append(sub_xs)
            district_ys.append(sub_ys)

        else:  # likely to be a multipolygon so need to draw each sub list separately

            for sub_list in feature['geometry']['coordinates']:

                district_names.append(str(feature['properties'][name_key]))

                sub_xs = []
                sub_ys = []

                if str(feature['properties'][ID_key]) in my_shade_dict:
                    results.append(float(my_shade_dict[str(feature['properties'][ID_key])])*100)
                    households1.append(int(my_supp_dict[str(feature['properties'][ID_key])][1])*100)
                    households2.append(int(my_supp_dict[str(feature['properties'][ID_key])][2])*100)
                    households3.append(int(my_supp_dict[str(feature['properties'][ID_key])][3])*100)
                    households4.append(int(my_supp_dict[str(feature['properties'][ID_key])][4])*100)
                    households5.append(int(my_supp_dict[str(feature['properties'][ID_key])][5])*100)
                    areas.append(my_supp_dict[str(feature['properties'][ID_key])][6])
                else:
                    results.append(float('nan'))
                    households1.append(float('nan'))
                    households2.append(float('nan'))
                    households3.append(float('nan'))
                    households4.append(float('nan'))
                    households5.append(float('nan'))
                    areas.append(float('nan'))

                for row in sub_list[0]:

                    sub_xs.append((row[0]))
                    sub_ys.append((row[1]))

                district_xs.append(sub_xs)
                district_ys.append(sub_ys)

    district_colors = [colors[set_colour_level(rate)] for rate in results]
    #district_colors = [colors[1] for rate in results]

    source = ColumnDataSource(data=dict(
        x=district_xs,
        y=district_ys,
        color=district_colors,
        name=district_names,
        rate=results,
        households1=households1,
        households2=households2,
        households3=households3,
        households4=households4,
        households5=households5,
        area=areas,
    ))

    tools = "pan,wheel_zoom,box_zoom,reset,hover,save"

    p = figure(width=900, height=900, title="Response rates by LA", tools=tools)

    p.patches('x', 'y', source=source,
              fill_color='color', fill_alpha=0.7,
              line_color="grey", line_width=0.3)

    hover = p.select_one(HoverTool)
    hover.point_policy = "follow_mouse"
    hover.tooltips = [
        ("Name", "@name"),
        ("Return rate", "@rate%"),
        ("htc 1", "@households1"),
        ("htc 2", "@households2"),
        ("htc 3", "@households3"),
        ("htc 4", "@households4"),
        ("htc 5", "@households5"),
        ("Area", "@area"),
    ]

    output_file("Return rate by LA.html", title="Return rate by LA")

    show(p)



