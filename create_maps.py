"""module for creating colour coded maps. The JSON file is the map. The shade file defines the
colors that will be seen. Sup data is extra information that will be displayed in the hover tool"""

import json
import csv
import os
from bokeh.models import HoverTool
from bokeh.plotting import figure, show, output_file, ColumnDataSource, save, vplot
from bokeh.plotting import reset_output
import math
import numpy as np
import pandas as pd


def set_colour_level(rate, min_shade, max_shade, dynamic_shading, reversed):

    if dynamic_shading:

        shade_range = max_shade - min_shade
        step = math.ceil(shade_range/5)
        min_value = int(min_shade)

        i = 0

        if math.isnan(rate):
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

        if math.isnan(rate):
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


def create_choropleth(output_path, json_file, shade_data_file, output_type, dynamic_shading, reverse):

    reset_output()

    colors = ["#CCCCCC", "#980043", "#DD1C77", "#DF65B0", "#C994C7", "#D4B9DA"]

    # separate shade data file
    my_results = pd.read_csv(shade_data_file)
    base_plot_name = 'shade'

    y = 0
    plot_dict = {}
    for x in range(80, 105, 5):
        temp_df = my_results[(my_results['result'] > y/100) & (my_results['result'] <= x/100)].head()
        plot_dict[str(x)] = dict(zip(temp_df.district, temp_df.result))
        y = x

    # now separate the geojson file by the districts that have an entry?
    # so cycle through create a dict of geoJSON files 1 for each of the lists above - same keys

    # then go through the below adding to same plot different source each time...


    # read in geojson map file
    with open(json_file) as base_map:
        map_data = json.load(base_map)

    # read in data that will define shading
    my_results = pd.read_csv(shade_data_file)
    my_shade_dict = dict(zip(my_results.district, my_results.result))

    suffix = '.html'
    output_filename = os.path.join(output_type + suffix)

    shade = []
    supplementary = []
    district_names = []
    district_xs = []
    district_ys = []

    name_key = 'LAD11NM'  # 'LSOA11NM' # 'LAD11NM'
    id_key = 'LAD11CD'  # 'LSOA11CD' # 'LAD11CD'

    for feature in map_data['features']:

        if feature['geometry']['type'] == 'Polygon':

            district_names.append(str(feature['properties'][name_key]))

            sub_xs = []
            sub_ys = []

            if (str(feature['properties'][id_key]) in my_shade_dict and
                    my_shade_dict[str(feature['properties'][id_key])] != math.nan):

                shade.append(float(my_shade_dict[str(feature['properties'][id_key])])*100)

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

                if (str(feature['properties'][id_key]) in my_shade_dict and
                        my_shade_dict[str(feature['properties'][id_key])] != math.nan):

                    shade.append(float(my_shade_dict[str(feature['properties'][id_key])])*100)

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

    district_colors = [colors[set_colour_level(rate, min_shade, max_shade, dynamic_shading, reverse)]
                       for rate in shade]

    source = ColumnDataSource(data=dict(
        x=district_xs,
        y=district_ys,
        color=district_colors,
        name=district_names,
        rate=shade,
    ))

    tools = "pan,wheel_zoom,box_zoom,reset,hover,save"

    title = output_type + " by LA"

    p = figure(width=900, height=900, title=title, tools=tools)

    p.patches('x', 'y', source=source,
              fill_color='color', fill_alpha=0.7,
              line_color="white", line_width=0.15, legend='95')

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

    output_file(os.path.join(output_path, "charts", output_filename), title=title)
    #save(p)
    show(p)



