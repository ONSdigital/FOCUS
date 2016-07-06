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


def set_colour_level(rate, min_shade, max_shade, dynamic_shading=False, reversed=False):

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


def draw_features(map_features, shade_data):

    colors = ["#CCCCCC", "#980043", "#DD1C77", "#DF65B0", "#C994C7", "#D4B9DA"]

    shade = []
    supplementary = []
    district_names = []
    district_xs = []
    district_ys = []

    name_key = 'LAD11NM'  # 'LSOA11NM' # 'LAD11NM'
    id_key = 'LAD11CD'  # 'LSOA11CD' # 'LAD11CD'

    for feature in map_features:

        if feature['geometry']['type'] == 'Polygon':

            district_names.append(str(feature['properties'][name_key]))

            sub_xs = []
            sub_ys = []

            if (str(feature['properties'][id_key]) in shade_data and
                        shade_data[str(feature['properties'][id_key])] != math.nan):

                shade.append(float(shade_data[str(feature['properties'][id_key])]) * 100)

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

                if (str(feature['properties'][id_key]) in shade_data and
                            shade_data[str(feature['properties'][id_key])] != math.nan):

                    shade.append(float(shade_data[str(feature['properties'][id_key])]) * 100)

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

    district_colors = [colors[set_colour_level(rate, min_shade, max_shade)]
                       for rate in shade]

    source = ColumnDataSource(data=dict(
        x=district_xs,
        y=district_ys,
        color=district_colors,
        name=district_names,
        rate=shade,
    ))

    # so by this point a source file has been generated...

    tools = "pan,wheel_zoom,box_zoom,reset,hover,save"

    title = " by LA"

    p = figure(width=900, height=900, title=title, tools=tools)

    p.patches('x', 'y', source=source,
              fill_color='color', fill_alpha=0.7,
              line_color="white", line_width=0.15, legend='95')

    hover = p.select_one(HoverTool)
    hover.point_policy = "follow_mouse"
    hover.tooltips = [
        ("Name", "@name"),
        ('output', "@rate%"),
        ("Supp", "@supp"),
    ]

    output_dir = os.path.join("outputs", "charts")

    if os.path.isdir(output_dir) is False:
        os.mkdir(output_dir)

    output_file(os.path.join(os.getcwd(), "charts", "test_file"), title=title)
    # save(p)
    show(p)


def create_choropleth(json_file, shade_data_file):

    reset_output()
    # separate shade data file
    my_results = pd.read_csv(shade_data_file)

    y = 0
    plot_dict = {}
    for x in range(80, 105, 5):
        temp_df = my_results[(my_results['result'] > y/100) & (my_results['result'] <= x/100)].head()
        if len(temp_df.index) > 0:
            plot_dict[str(x)] = dict(zip(temp_df.district, temp_df.result))
        y = x

    # now separate the geojson file by the districts that have an entry?
    # so cycle through create a dict of geoJSON files 1 for each of the lists above - same keys
    geojson_dict = {}

    # read in geojson map file
    with open(json_file) as base_map:
        map_data = json.load(base_map)

    # don't need the below to ensure same results...this is just plotting!!
    list_of_plot_dict_keys = sorted(list(plot_dict.keys()), key=int)

    name_key = 'LAD11NM'  # 'LSOA11NM' # 'LAD11NM'
    id_key = 'LAD11CD'  # 'LSOA11CD' # 'LAD11CD'

    for plot in list_of_plot_dict_keys:

        geojson_list = []

        if bool(plot_dict[plot]):

            for feature in map_data['features']:

                if str(feature['properties'][id_key]) in plot_dict[plot]:

                    geojson_list.append(feature)

            geojson_dict[plot] = geojson_list

    # will need to add any regions not found - ie, that there are no results for
    # For each feature dict look for corresponding values in corresponding shade dict and create plot
    for key, value in geojson_dict.items():

        # may need to run the below in this loop direct
        draw_features(value, plot_dict[key])













