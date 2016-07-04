"""module used to aggregate raw csv files into master datalists ready for analysis of further output"""
import pandas as pd
import os
import glob
import create_maps


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


def create_response_map(output_path, data_lists, geojson):

    # create some default output in ths case overall response rate by district
    return_list = []

    for df in data_lists['Return']:

        df.columns = ['rep', 'district', 'hh_id', 'hh_type', 'time']
        int_df = pd.DataFrame({'Returns': df.groupby(['district', 'rep']).size()}).reset_index()
        return_list.append(pd.DataFrame(int_df.groupby(['district']).mean()['Returns']))

    district_size_list = []

    # collate total hh outputs
    for df in data_lists['Total_hh']:
        df.columns = ['rep', 'district', 'hh_count']
        district_size_list.append(pd.DataFrame(df.groupby(['district']).mean()['hh_count']))

    # selects only the first data frame
    index = 1
    for item in return_list:
        returns = pd.DataFrame((item.join(district_size_list[index-1])))
        returns = returns[['Returns']].div(returns.hh_count, axis=0)
        output_dir = os.path.join("outputs", "csv")
        if os.path.isdir(output_dir) is False:
            os.mkdir(output_dir)
        file_name = os.path.join(output_dir, "run " + str(index) + " returns.csv")
        returns.to_csv(file_name)
        create_maps.create_choropleth(geojson, file_name, "inputs/LA_hh.csv", "run " + str(index) + " return rates",
                                      False, False)
        index += 1


def create_visit_map(output_path, data_lists, geojson, visit_type="Visit_success"):

    # create some output
    visit_list = []

    # count the number of visits per district
    for df in data_lists["Visit"]:

        df.columns = ['rep', 'district', 'hh_id', 'hh_type', 'time']
        int_df = pd.DataFrame({'Visits': df.groupby(['rep', 'district', 'hh_type']).size()}).reset_index()
        visit_list.append(pd.DataFrame(int_df.groupby(['district']).sum()['Visits']))

    visit_outcome_list = []

    # count the number of visits that had the input outcome
    for df in data_lists[visit_type]:
        df.columns = ['rep', 'district', 'hh_id', 'hh_type', 'time']
        int_df = pd.DataFrame({'outcome': df.groupby(['rep', 'district']).size()}).reset_index()
        visit_outcome_list.append(pd.DataFrame(int_df.groupby(['district']).mean()['outcome']))

    index = 1
    for item in visit_outcome_list:
        visits_done = pd.DataFrame((visit_list[index-1].join(item)))
        visits_done = visits_done[['outcome']].div(visits_done.Visits, axis=0)
        # output the percentage of visits that have the outcome as per the input.
        output_dir = os.path.join("outputs", "csv")
        if os.path.isdir(output_dir) is False:
            os.mkdir(output_dir)
        file_name = os.path.join(output_dir, "run " + str(index) + " " + visit_type + ".csv")
        visits_done.to_csv(file_name)
        create_maps.create_choropleth(geojson, file_name, "inputs/LA_hh.csv",
                                      "run " + str(index) + " " + visit_type,
                                      True, False)
        index += 1





