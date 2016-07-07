"""module for creating colour coded maps. The JSON file is the map. The shade file defines the
colors that will be seen. Sup data is extra information that will be displayed in the hover tool"""

import json
import os
from bokeh.models import HoverTool
from bokeh.plotting import figure, show, output_file, ColumnDataSource, save, vplot
from bokeh.plotting import reset_output
import math
import numpy as np
import pandas as pd
import helper as h


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

        if math.isnan(rate) or rate == 0:
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


def define_features(map_features, shade_data, key, x_dict, y_dict, color_dict, name_dict, rate_dict, source_dict):

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

    x_dict[key] = district_xs
    y_dict[key] = district_ys
    color_dict[key] = district_colors
    name_dict[key] = district_names
    rate_dict[key] = shade

    source_dict[key] = ColumnDataSource(data=dict(
        x=district_xs,
        y=district_ys,
        color=district_colors,
        name=district_names,
        rate=shade,
    ))


def create_choropleth(json_file, shade_data_file):

    reset_output()

    # separate data file used to define shade
    results_data = pd.read_csv(shade_data_file)
    results_filename = h.path_leaf(shade_data_file).split(".")[0]

    y = 0
    plot_dict = {}  # dict used to store each plots shade data.
    for x in range(80, 105, 5):
        temp_df = results_data[(results_data['result'] > y/100) & (results_data['result'] <= x/100)]
        if len(temp_df.index) > 0:
            plot_dict[str(x)] = dict(zip(temp_df.district, temp_df.result))
        y = x

    # separate JSON file to match the shade separation

    geojson_dict = {}  # dict used to store each plots geo data
    delete_list = []
    with open(json_file) as base_map:
        map_data = json.load(base_map)

    id_key = 'LAD11CD'  # 'LSOA11CD', 'LAD11CD'

    for key, value in plot_dict.items():

        geojson_list = []

        for feature in map_data['features']:

            if str(feature['properties'][id_key]) in value:

                geojson_list.append(feature)
                # but also remove the feature from the map_data[features] file
                delete_list.append(str(feature['properties'][id_key]))

        geojson_dict[key] = geojson_list

    # if any features have no defined output add them but assign them a zero value.

    map_data['features'] = [feature for feature in map_data['features']
                            if feature['properties'][id_key] not in delete_list]

    if bool(map_data['features']):

        plot_dict['0'] = dict((feature['properties'][id_key], 0) for feature in map_data['features'])
        geojson_dict['0'] = [feature for feature in map_data['features']]

    # define the dictionaries that will store the generated plot data
    source_dict = {}
    x_dict = {}
    y_dict = {}
    color_dict = {}
    name_dict = {}
    rate_dict = {}

    for key, value in geojson_dict.items():

        define_features(value, plot_dict[key], key, x_dict, y_dict, color_dict, name_dict, rate_dict, source_dict)

    tools = "pan,wheel_zoom,box_zoom,reset,hover,save"

    title = results_filename + " by LA"

    p = figure(width=900, height=900, title=title, tools=tools)

    # draw each patch

    for key in sorted(source_dict.keys(), key=int, reverse=True):

        p.patches('x', 'y', source=source_dict[key],
                  fill_color='color', fill_alpha=0.7,
                  line_color="white", line_width=0.15, legend=str(key))

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

    suffix = '.html'
    output_filename = os.path.join('test' + suffix)

    output_file_path = os.path.join(output_dir, output_filename)

    output_file(output_file_path, title=title)
    # save(p)
    show(p)

















