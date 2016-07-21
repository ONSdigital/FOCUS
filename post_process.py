"""module used to aggregate raw csv files into master datalists ready for analysis of further output"""
import pandas as pd
import os
import glob
import create_maps
import create_graphs
import numpy as np
from collections import defaultdict


def aggregate(output_path, output_type):

    folder_list = glob.glob(output_path + '/*/')
    data_lists = {}

    for folder in folder_list:
        folder_name = folder.split(os.path.sep)[-2]
        if folder_name in output_type:

            folder_name = folder.split(os.path.sep)[-2]
            data_lists[folder_name] = []

            glob_folder = os.path.join('outputs', folder_name, '*.csv')
            file_list = glob.glob(glob_folder)

            # for each file add to a list in the top level dictionary
            for file in file_list:

                data_lists[file.split(os.path.sep)[1]].append(pd.read_csv(file, header=0))

    return data_lists


def create_map(output_path, data_lists, geojson, palette_colour='heather', data_numerator="Returned",
               data_denominator="hh_count", response_type="all", step=5, min_range=80, max_range=100,
               reverse=False, dynamic=False):

    numerator_list = []
    denominator_list = []

    if response_type == 'all':
        type_filter = [0, 1]
    elif response_type == 'paper':
        type_filter = [0]
    else:
        type_filter = [1]

    for df in data_lists[data_numerator]:

        int_df = df.loc[df['digital'].isin(type_filter)]
        int_df = pd.DataFrame({'numerator_result': int_df.groupby(['district', 'rep']).size()}).reset_index()
        numerator_list.append(pd.DataFrame(int_df.groupby(['district']).mean()['numerator_result']))

    if data_denominator == 'hh_count':
        for df in data_lists['hh_count']:

            denominator_list.append(pd.DataFrame({'denominator_result': (df.groupby(['district']).mean()['hh_count'])}))

    else:
        for df in data_lists[data_denominator]:
            int_df = df.loc[df['digital'].isin(type_filter)]
            int_df = pd.DataFrame({'denominator_result': int_df.groupby(['district', 'rep']).size()}).reset_index()
            denominator_list.append(pd.DataFrame(int_df.groupby(['district']).mean()['denominator_result']))

    index = 1
    for item in numerator_list:
        returns = pd.DataFrame((item.join(denominator_list[index-1])))
        returns = returns[['numerator_result']].div(getattr(returns, 'denominator_result'), axis=0)
        returns.rename(columns={'numerator_result': 'result'}, inplace=True)
        output_dir = os.path.join(output_path, "csv")
        if os.path.isdir(output_dir) is False:
            os.mkdir(output_dir)
        plot_data = os.path.join(output_dir, "run " + str(index) + " " + data_numerator + " " + response_type + ".csv")
        returns.to_csv(plot_data)
        create_maps.create_choropleth(output_path, geojson, plot_data, palette_colour,
                                      "run " + str(index) + " " + data_numerator + " " + response_type + " " + "returns",
                                      step, min_range, max_range, reverse, dynamic)
        index += 1


def create_bar_chart(output_path, data_lists, data_numerator="Returned", data_denominator="hh_record"):

    numerator_list = []
    total_hh = defaultdict(list)

    for df in data_lists[data_denominator]:

        for index, row in df.iterrows():
            total_hh[row[2]].append(row[2])

        for key, value in total_hh.items():
            total_hh[key] = len(total_hh[key])

    for df in data_lists[data_numerator]:

        int_df = pd.DataFrame({'numerator_result': df.groupby(['rep', 'hh_type', 'digital']).size()}).reset_index()
        out_df = pd.DataFrame(int_df.groupby(['hh_type', 'digital']).mean()['numerator_result']).reset_index()

        # at this point go through the rows adding relavent h count...
        for index, row in out_df.iterrows():
            out_df.ix[index, 'total_hh'] = total_hh[row[0]]

        out_df.rename(columns={'numerator_result': 'result'}, inplace=True)
        out_df['digital'].replace(bool(True), 'digital', inplace=True)
        out_df['digital'].replace(bool(False), 'paper', inplace=True)
        out_df['perc_res'] = out_df[['result']].div(getattr(out_df, 'total_hh'), axis=0)
        numerator_list.append(out_df)

    create_graphs.bar_response(numerator_list, output_path)


def create_story(output_path, data_lists, geojson, sim_end=1400, run=0, palette_colour='muted purple',
                 response_type="story", step=5, min_range=0, max_range=100, reverse=False, dynamic=False):


    # take the required gap...and filter files by that...then pass to crearte maps above to create totals?
    output_dir = os.path.join(output_path, "return stories")

    if os.path.isdir(output_dir) is False:
        os.mkdir(output_dir)

    input_data = data_lists['Return'][run]

    input_hh_data = data_lists['Total_hh'][run]
    input_hh_data.columns = ['rep', 'district', 'hh_count']
    input_hh_data = pd.DataFrame(input_hh_data.groupby(['district']).mean()['hh_count'])

    input_data.columns = ['rep', 'district', 'digital', 'hh_type', 'time']
    split_range = np.arange(0, sim_end, 24)

    for item in split_range:

        response_type = str(item) + 'story'

        int_df = input_data[input_data.time < item + 24]
        int_df = pd.DataFrame({'result': int_df.groupby(['district', 'rep']).size()}).reset_index()
        int_df = pd.DataFrame(int_df.groupby(['district']).mean()['result'])

        returns = pd.DataFrame(int_df.join(input_hh_data))

        returns = returns[['result']].div(returns.hh_count, axis=0)
        print(returns)

        plot_data = os.path.join(output_dir, str(item) + " story.csv")
        returns.to_csv(plot_data)

        #create_maps.create_choropleth(output_path, geojson, plot_data, palette_colour, response_type,
        #                              step, min_range, max_range, reverse, dynamic)





