"""module for creating colour coded maps. The JSO?N file is the map. The shade file defines the
colors that will be seen. Sup data is extra information that will be displayed in the hover tool"""

import json
import csv
import os
from bokeh.models import HoverTool
from bokeh.plotting import figure, show, output_file, ColumnDataSource, save
from bokeh.plotting import reset_output
import math
import numpy as np


def set_colour_level(rate, min_shade, max_shade, dynamic_shading, reversed):

    if dynamic_shading:

        shade_range = max_shade - min_shade
        step = math.ceil(shade_range/5)
        min_value = int(min_shade)

        i = 0

        if math.isnan(rate) is True:
            i = 0
        elif rate < min_value + step:
            i = 1
        elif rate < min_value + step*2:
            i = 2
        elif rate < min_value + step*3:
            i = 3
        elif rate < min_value + step*4:
            i = 4
        elif rate <= min_value + step*5:
            i = 5

        if reversed and i != 0:
            return 6-i
        else:
            return i

    else:

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

        if reversed and i != 0:
            return 6-i
        else:
            return i


def create_choropleth(json_file, shade_data_file, sup_data_file, output_type, dynamic_shading, reversed):

    reset_output()

    # read in geojson map file
    with open(json_file) as base_map:
        map_data = json.load(base_map)

    # read in data that will define shading
    with open(os.path.join(os.getcwd(), shade_data_file), 'r') as shade_data:

        reader = csv.reader(shade_data)
        next(reader)
        my_results = list(reader)
        my_shade_dict = {rows[0]: rows[1] for rows in my_results}

    # read in some supplementary info - must have district as row[0]?
    with open(os.path.join(os.getcwd(), sup_data_file), 'r') as supp_data:

        reader = csv.reader(supp_data)
        next(reader)
        my_results = list(reader)
        my_supp_dict = {rows[0]: rows[1:] for rows in my_results}

    suffix = '.html'
    output_filename = os.path.join(output_type + suffix)

    shade = []
    supplementary = []
    district_names = []
    district_xs = []
    district_ys = []

    colors = ["#CCCCCC", "#980043", "#DD1C77", "#DF65B0",  "#C994C7", "#D4B9DA"]

    name_key = 'LAD11NM'  # 'LSOA11NM' # 'LAD11NM'
    id_key = 'LAD11CD'  # 'LSOA11CD' # 'LAD11CD'

    for feature in map_data['features']:

        if feature['geometry']['type'] == 'Polygon':

            district_names.append(str(feature['properties'][name_key]))

            sub_xs = []
            sub_ys = []

            if str(feature['properties'][id_key]) in my_shade_dict:
                print(my_shade_dict[str(feature['properties'][id_key])])
                shade.append(float(my_shade_dict[str(feature['properties'][id_key])])*100)
                supplementary.append(my_supp_dict[str(feature['properties'][id_key])][:])

            else:
                shade.append(float('nan'))
                supplementary.append(float('nan'))

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

                if str(feature['properties'][id_key]) in my_shade_dict:
                    print(my_shade_dict[str(feature['properties'][id_key])])
                    shade.append(float(my_shade_dict[str(feature['properties'][id_key])])*100)
                    supplementary.append(my_supp_dict[str(feature['properties'][id_key])][:])
                else:
                    shade.append(float('nan'))
                    supplementary.append(float('nan'))

                for row in sub_list[0]:

                    sub_xs.append((row[0]))
                    sub_ys.append((row[1]))

                district_xs.append(sub_xs)
                district_ys.append(sub_ys)

    min_shade = np.nanmin(shade)
    max_shade = np.nanmax(shade)

    district_colors = [colors[set_colour_level(rate, min_shade, max_shade, dynamic_shading, reversed)]
                       for rate in shade]

    source = ColumnDataSource(data=dict(
        x=district_xs,
        y=district_ys,
        color=district_colors,
        name=district_names,
        rate=shade,
        supp=supplementary,
    ))

    tools = "pan,wheel_zoom,box_zoom,reset,hover,save"

    title = output_type + " by LA"

    p = figure(width=900, height=900, title=title, tools=tools)

    p.patches('x', 'y', source=source,
              fill_color='color', fill_alpha=0.7,
              line_color="grey", line_width=0.3)

    hover = p.select_one(HoverTool)
    hover.point_policy = "follow_mouse"
    hover.tooltips = [
        ("Name", "@name"),
        (output_type, "@rate%"),
        ("Supp", "@supp"),
    ]

    output_dir = os.path.join("outputs", "charts")

    if os.path.isdir(output_dir) is False:
        os.mkdir(output_dir)

    output_file(os.path.join("outputs", "charts", output_filename), title=title)
    save(p)
    #show(p)



