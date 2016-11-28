"""module used to aggregate raw csv files into master datalists ready for analysis of further output"""
import pandas as pd
import os
import glob
import create_maps
import create_graphs
import numpy as np
from collections import defaultdict


def csv_to_pandas(output_path, output_type):
    """converts csv output files to a dictionary of dictionaries of Pandas data frames for each output type and run.
    output_type is a list of the types of output to include (e.g. responses, visits). to refer to the finished data
    dict use: data_dict['type of output']['run']. Data frame will be every repliciation of that output type for that
     run"""

    folder_list = glob.glob(output_path + '/*/')  # create list of folders at output path
    data_dict = defaultdict(list)  # create a dict ready for the dataframes

    for folder in folder_list:
        folder_name = folder.split(os.path.sep)[-2]
        if folder_name in output_type:

            glob_folder = os.path.join('outputs', folder_name, '*.csv')
            file_list = glob.glob(glob_folder)  # get a list of all files(sim runs) in the folder

            data_dict[folder_name] = defaultdict(list)

            # for each file (sim run) add to dictionary for that type of output
            for file in file_list:
                file_name = file.split(os.path.sep)[-1][0]
                data_dict[folder_name][file_name] = pd.read_csv(file, header=0)  # add each run to dict

    return data_dict


def cumulative_sum(df, start, end, step, geog):
    """takes a data frame and creates cumulative totals for specified geography"""

    # create bins and group names
    bin_values = np.arange(start, end + step, step)
    group_names = np.arange(start, end, step)

    print(df)

    # add a new column containing the categories the entry belongs too
    df['categories'] = pd.cut(df['time'], bin_values, labels=group_names)

    print(df)

    # group by each combination of district and category and count the number of each category
    cat_sum = df.groupby([geog, 'categories'])['categories'].size()
    # calculate and the cum sum of the totals
    cat_sum = cat_sum.groupby(level=[0]).cumsum().reset_index(name='cum_sum')
    print(cat_sum)

    # now get them in the right output format for visualisation


    return cat_sum


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








