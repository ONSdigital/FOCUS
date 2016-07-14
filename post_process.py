"""module used to aggregate raw csv files into master datalists ready for analysis of further output"""
import pandas as pd
import os
import glob
import create_maps
import numpy as np


def aggregate(output_path):

    folder_list = glob.glob(output_path + '/*/')
    data_lists = {}

    for folder in folder_list:

        folder_name = folder.split(os.path.sep)[-2]
        data_lists[folder_name] = []

        glob_folder = os.path.join('outputs', folder_name, '*.csv')
        file_list = glob.glob(glob_folder)

        # for each file add to a list in the top level dictionary
        for file in file_list:
            # add the raw file to dataLists ready for
            data_lists[file.split(os.path.sep)[1]].append(pd.read_csv(file, header=-1))

    return data_lists


def create_response_map(output_path, data_lists, geojson, palette_colour='heather', ret_type="Return", response_type="all",
                        step=5, min_range=80, max_range=100, reverse=False, dynamic=False):

    # create overall response rate by district or just for paper/digital
    return_list = []
    if response_type == 'all':
        type_filter = [0, 1]
    elif response_type == 'paper':
        type_filter = [0]
    else:
        type_filter = [1]

    for df in data_lists[ret_type]:

        df.columns = ['rep', 'district', 'digital', 'hh_type', 'time']
        int_df = df.loc[df['digital'].isin(type_filter)]
        int_df = pd.DataFrame({'result': int_df.groupby(['district', 'rep']).size()}).reset_index()
        return_list.append(pd.DataFrame(int_df.groupby(['district']).mean()['result']))

    district_size_list = []

    # collate total hh outputs
    for df in data_lists['Total_hh']:
        df.columns = ['rep', 'district', 'hh_count']
        district_size_list.append(pd.DataFrame(df.groupby(['district']).mean()['hh_count']))

    index = 1
    for item in return_list:
        returns = pd.DataFrame((item.join(district_size_list[index-1])))
        returns = returns[['result']].div(returns.hh_count, axis=0)
        output_dir = os.path.join(output_path, "csv")
        if os.path.isdir(output_dir) is False:
            os.mkdir(output_dir)
        plot_data = os.path.join(output_dir, "run " + str(index) + " " + ret_type + " " + response_type + ".csv")
        returns.to_csv(plot_data)
        create_maps.create_choropleth(output_path, geojson, plot_data, palette_colour,
                                      "run " + str(index) + " " + ret_type + " " + response_type + " " + "returns",
                                      step, min_range, max_range, reverse, dynamic)
        index += 1


def create_visit_map(output_path, data_lists, geojson, palette_colour, visit_type="Visit_contact",
                     step=10, min_range=20, max_range=100, reverse=False):

    # create some output
    visit_list = []

    # count the number of visits per district
    for df in data_lists["Visit"]:

        df.columns = ['rep', 'district', 'hh_id', 'hh_type', 'time', 'id']
        int_df = pd.DataFrame({'Visits': df.groupby(['rep', 'district']).size()}).reset_index()
        visit_list.append(pd.DataFrame(int_df.groupby(['district']).mean()['Visits']))

    visit_outcome_list = []

    # count the number of visits that had the input outcome
    for df in data_lists[visit_type]:
        df.columns = ['rep', 'district', 'digital', 'hh_type', 'time', 'hh_id']
        int_df = pd.DataFrame({'result': df.groupby(['rep', 'district']).size()}).reset_index()
        visit_outcome_list.append(pd.DataFrame(int_df.groupby(['district']).mean()['result']))

    index = 1
    for item in visit_outcome_list:
        visits_done = pd.DataFrame((visit_list[index-1].join(item)))
        visits_done = visits_done[['result']].div(visits_done.Visits, axis=0)
        # output the percentage of visits that have the outcome as per the input.
        output_dir = os.path.join("outputs", "csv")
        if os.path.isdir(output_dir) is False:
            os.mkdir(output_dir)
        plot_data = os.path.join(output_dir, "run " + str(index) + " " + visit_type + ".csv")
        visits_done.to_csv(plot_data)
        create_maps.create_choropleth(output_path, geojson, plot_data, palette_colour,
                                      "run " + str(index) + " " + visit_type,  step, min_range, max_range, reverse)
        index += 1


def create_story(output_path, data_lists, geojson, sim_end=1400, run=0, palette_colour='muted purple',
                 response_type="story", step=5, min_range=0, max_range=100, reverse=False, dynamic=False):

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





