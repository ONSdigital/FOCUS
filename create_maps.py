"""module for creating colour coded maps. The JSON file is the map. The shade file defines the
colors that will be seen. Sup data is extra information that will be displayed in the hover tool"""

import json
import os
import math
import numpy as np
import pandas as pd
import helper as h
from bokeh.models import HoverTool
from bokeh.plotting import figure, show, output_file, ColumnDataSource, save, reset_output
import seaborn as sns


def select_palette(shade_no, palette_colour, reverse=False):
    missing = ["#d8dcd6"]
    pal = sns.dark_palette(palette_colour, shade_no, reverse=reverse, input='xkcd')
    return missing + list(pal.as_hex())


def set_dynamic_step(min_shade, max_shade):

    steps = [1, 2, 5, 10]

    shade_range = h.roundup_nearest_ten(max_shade) - h.rounddown_nearest_ten(min_shade)
    # default to step that is a whole number in multiple of 1,2 5 or 10, max of 10 shades?
    for step in steps:
        if shade_range / step <= 10:
            return step


def set_colour_level(rate, min_shade, max_shade, step):

    i = 1
    if math.isnan(rate) or rate == 0:
        return 0
    else:
        for x in range(min_shade, max_shade+step, step):
            if rate <= x:
                return i

            else:
                i += 1


def define_features(map_features, shade_data, key, source_dict, min_range, max_range, step, colours, dynamic):

    shade = []
    district_names = []
    district_code = []
    district_xs = []
    district_ys = []


    name_key = 'LAD11NM'  # 'LSOA11NM' # 'LAD11NM'
    id_key = 'LAD11CD'  # 'LSOA11CD' # 'LAD11CD'

    for feature in map_features:

        if feature['geometry']['type'] == 'Polygon':

            district_names.append(str(feature['properties'][name_key]))
            district_code.append(str(feature['properties'][id_key]))

            sub_xs = []
            sub_ys = []

            if (str(feature['properties'][id_key]) in shade_data and
                    shade_data[str(feature['properties'][id_key])] != math.nan):

                shade.append(float(shade_data[str(feature['properties'][id_key])]) * 100)

            else:
                shade.append(float('nan'))

            temp_list = feature['geometry']['coordinates'][0]

            for coord in temp_list:
                sub_xs.append((coord[0]))  # try list comprehension instead...quicker?
                sub_ys.append((coord[1]))

            district_xs.append(sub_xs)
            district_ys.append(sub_ys)

        else:  # likely to be a multipolygon so need to draw each sub list separately

            for sub_list in feature['geometry']['coordinates']:

                district_names.append(str(feature['properties'][name_key]))
                district_code.append(str(feature['properties'][id_key]))

                sub_xs = []
                sub_ys = []

                if (str(feature['properties'][id_key]) in shade_data and
                        shade_data[str(feature['properties'][id_key])] != math.nan):

                    shade.append(float(shade_data[str(feature['properties'][id_key])]) * 100)

                else:
                    shade.append(float('nan'))

                for row in sub_list[0]:
                    sub_xs.append((row[0]))
                    sub_ys.append((row[1]))

                district_xs.append(sub_xs)
                district_ys.append(sub_ys)

    district_colors = [colours[set_colour_level(rate, min_range, max_range, step)]
                       for rate in shade]

    source_dict[key] = ColumnDataSource(data=dict(
        x=district_xs,
        y=district_ys,
        color=district_colors,
        name=district_names,
        code=district_code,
        rate=shade,
    ))


def create_choropleth(output_path, json_file, shade_data_file, palette_colour, output_type, step, min_range, max_range,
                      reverse, dynamic=True):

    reset_output()

    # separate data file used to define shade
    results_data = pd.read_csv(shade_data_file)

    # calculate the maximum number of shades to show in final output
    if dynamic:
        min_range = h.rounddown_nearest_ten(np.nanmin(list(results_data.result*100)))
        max_range = h.roundup_nearest_ten(np.nanmax(list(results_data.result*100)))
        step = set_dynamic_step(min_range, max_range)

    # check for a whole number in user defined values - return an error if not
    shade_no = int(((max_range+step)-min_range)/step)

    plot_dict = {}  # dict used to store each plots data - one for each shade to display.

    lower_limit = 0
    for upper_limit in range(min_range, max_range+step, step):
        temp_df = results_data[(results_data['result'] > lower_limit/100) & (results_data['result'] <= upper_limit/100)]
        if len(temp_df.index) > 0:
            plot_dict[str(upper_limit)] = dict(zip(temp_df.district, temp_df.result))
        lower_limit = upper_limit

    # separate geojson file to match the plots above
    geojson_dict = {}  # dict used to store each plots geo data
    delete_list = []  # districts to delete once all with a colour are assigned

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

    # add a corresponding plot for the shade for those 0 values
    if bool(map_data['features']):

        plot_dict['0'] = dict((feature['properties'][id_key], 0) for feature in map_data['features'])
        geojson_dict['0'] = [feature for feature in map_data['features']]

    # create the colour palette to use
    colours = select_palette(shade_no, palette_colour, reverse)

    source_dict = {}  # a dict that will store all the columndatasources
    for key, value in geojson_dict.items():

        define_features(value, plot_dict[key], key, source_dict, min_range, max_range, step, colours, dynamic)

    tools = "pan,wheel_zoom,box_zoom,reset,hover,save"

    title = output_type + " by LA"

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
        (output_type, "@rate%"),
        ("Code", "@code"),
    ]

    output_dir = os.path.join(output_path, "charts")

    if os.path.isdir(output_dir) is False:
        os.mkdir(output_dir)

    suffix = '.html'
    output_filename = os.path.join(output_type + suffix)

    output_file_path = os.path.join(output_dir, output_filename)

    output_file(output_file_path, title=title, mode='inline')
    # save(p)
    show(p)

















