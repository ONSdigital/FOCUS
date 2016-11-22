"""module used to aggregate raw csv files into master datalists ready for analysis of further output"""
import pandas as pd
import os
import glob
import create_maps
import create_graphs
import numpy as np
from collections import defaultdict


# converts csv output files to a dictionary of dictionaries of Pandas Dataframes for each output type and run
def csv_to_pandas(output_path, output_type):
    # output_type is a list of the types of output to include (e.g. responses, visits)

    folder_list = glob.glob(output_path + '/*/')  # create list of folders at output path
    data_dict = defaultdict(list)  # create a dict ready for the dataframes

    for folder in folder_list:
        folder_name = folder.split(os.path.sep)[-2]
        if folder_name in output_type:

            glob_folder = os.path.join('outputs', folder_name, '*.csv')
            file_list = glob.glob(glob_folder)  # get a list of all files (sim runs) in the folder

            data_dict[folder_name] = defaultdict(list)

            # for each file (sim run) add to dictionary for that type of output
            for file in file_list:
                file_name = file.split(os.path.sep)[-1][0]
                data_dict[folder_name][file_name] = pd.read_csv(file, header=0)  # add each run to dict

    return data_dict











# deprecation function used to create interactive maps using the bokeh package
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

    for key, value in data_lists[data_numerator].items():

        int_df = value.loc[value['digital'].isin(type_filter)]
        int_df = pd.DataFrame({'numerator_result': int_df.groupby(['district', 'rep']).size()}).reset_index()
        numerator_list.append(pd.DataFrame(int_df.groupby(['district']).mean()['numerator_result']))

    if data_denominator == 'hh_count':

        for key, value in data_lists['hh_count'].items():

            int_df = pd.DataFrame({'denominator_result': (value.groupby(['district', 'rep']).sum()['hh_count'])}).reset_index()
            denominator_list.append(pd.DataFrame(int_df.groupby(['district']).mean()['denominator_result']))

    else:
        for key, value in data_lists[data_denominator].items():
            int_df = value.loc[value['digital'].isin(type_filter)]
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


def create_line_chart(output_path, data_lists,  data_numerator="Returned", data_denominator="hh_record"):

    # list of df of raw data...
    plot_list = []
    total_hh = defaultdict(list)

    for df in data_lists[data_denominator]:

        # below returns the total number of each class of HH
        for index, row in df.iterrows():
            total_hh[row[2]].append(row[2])

        for key, value in total_hh.items():
            total_hh[key] = len(total_hh[key])

    for df in data_lists[data_numerator]:

        int_df = pd.DataFrame({'numerator_result': df.groupby(['rep', 'hh_type', 'digital']).size()}).reset_index()
        out_df = pd.DataFrame(int_df.groupby(['hh_type', 'digital']).mean()['numerator_result']).reset_index()

        # at this point go through the rows adding relavent h count...
        for index, row in out_df.iterrows():
            out_df.ix[index, 'total_hh'] = total_hh[row['hh_type']]

        out_df.rename(columns={'numerator_result': 'result'}, inplace=True)
        out_df['digital'].replace(bool(True), 'digital', inplace=True)
        out_df['digital'].replace(bool(False), 'paper', inplace=True)

        # then divide
        out_df['perc_res'] = out_df[['result']].div(getattr(out_df, 'total_hh'), axis=0)

        # now have % returns total...but need over time...more post processing required...
        # so for each period get total and return result to lsit... then plot

        plot_list.append(out_df)

        simple_split(plot_list, "returned")

    create_graphs.line_response(plot_list, output_path)


def create_bar_chart(output_path, data_lists, data_numerator="Returned", data_denominator="hh_record"):

    plot_list = []
    total_hh = defaultdict(list)

    for df in data_lists[data_denominator]:

        # below returns the total number of each class of HH
        for index, row in df.iterrows():
            total_hh[row[2]].append(row[2])

        for key, value in total_hh.items():
            total_hh[key] = len(total_hh[key])

    for df in data_lists[data_numerator]:

        int_df = pd.DataFrame({'numerator_result': df.groupby(['rep', 'hh_type', 'digital']).size()}).reset_index()
        out_df = pd.DataFrame(int_df.groupby(['hh_type', 'digital']).mean()['numerator_result']).reset_index()

        # at this point go through the rows adding relavent h count...
        for index, row in out_df.iterrows():
            out_df.ix[index, 'total_hh'] = total_hh[row['hh_type']]

        out_df.rename(columns={'numerator_result': 'result'}, inplace=True)
        out_df['digital'].replace(bool(True), 'digital', inplace=True)
        out_df['digital'].replace(bool(False), 'paper', inplace=True)
        out_df['perc_res'] = out_df[['result']].div(getattr(out_df, 'total_hh'), axis=0)
        plot_list.append(out_df)

    create_graphs.bar_response(plot_list, output_path)


# check and think if this is the best and correct way to handle these files
# must be able to summarise each runs output and store appropriately
# just do simple average counts to start
# for all output types
# then add in proportions

# function to take data_lists (a list of Pandas dataframes) with a time dimension and produce aggregated values
def simple_split(data_lists, start=0, end=1440, step=360, cumulative=True, runs=()):

    split_runs = defaultdict(list)
    index = 0

    for key, value in data_lists.items():
        if index in runs or len(runs) == 0:
            split_runs[key] = split_single(value, start, end, step, cumulative)
            index += 1

    return split_runs

# function to take a dataframe (with a time dimension) and produce aggregated values
def split_single(df, start, end, step, cumulative=True):

    split_dict = defaultdict(list)
    split_range = np.arange(start, end, step)

    for split in split_range:
        if cumulative:
            int_df = df.loc[df.time <= split + step]
        else:
            int_df = df.loc[(df["time"] > split) & (df["time"] <= split + step)]

        split_dict[split+step] = int_df

    return split_dict


def divide_all(data_numerator, data_denominator, runs=()):

    divide_runs = defaultdict(list)

    for key, value in data_numerator.items():
        divide_runs[key] = divide_single(data_numerator[key], data_denominator)

    return divide_runs


def divide_single(data_numerator, data_denominator):

    int_num_df = pd.DataFrame({'numerator_result': data_numerator.groupby(['rep', 'hh_type']).size()}).reset_index()
    out_num_df = pd.DataFrame(int_num_df.groupby(['hh_type']).mean()['numerator_result']).reset_index()

    # add item to divide by do div and keep only new...
    int_den_df = pd.DataFrame({'denominator_result': data_denominator.groupby(['rep', 'hh_type']).size()}).reset_index()
    out_den_df = pd.DataFrame(int_den_df.groupby(['hh_type']).mean()['denominator_result']).reset_index()

    returns = pd.merge(out_num_df, out_den_df, on='hh_type')

    returns['returns'] = returns[['numerator_result']].div(getattr(returns, 'denominator_result'), axis=0)
    returns = returns[['hh_type', 'returns']]

    return returns


def divide_single_count(data_numerator, data_denominator):

    int_num_df = pd.DataFrame({'numerator_result': data_numerator.groupby(['rep', 'district']).size()}).reset_index()
    out_num_df = pd.DataFrame(int_num_df.groupby(['district']).mean()['numerator_result']).reset_index()

    out_den_df = pd.DataFrame(data_denominator.groupby(['district']).mean()['hh_count']).reset_index()

    returns = pd.merge(out_num_df, out_den_df, on='district')

    returns['returns'] = returns[['numerator_result']].div(getattr(returns, 'hh_count'), axis=0)
    returns = returns[['returns']]

    return returns


def add_hh_count(data_lists):

    hh_count = defaultdict(list)

    for df in data_lists['hh_count']:
        hh_count['hh_count'].append(df)

    return hh_count








