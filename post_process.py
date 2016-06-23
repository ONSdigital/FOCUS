"""module used to aggregate raw csv files into master datalists ready for analysis of further output"""
import pandas as pd
import os
import glob
import create_maps


def aggregate(output_path, data_lists):

    folder_list = glob.glob("outputs/*/")

    for folder in folder_list:

        folder_name = folder.split(os.path.sep)[1]
        data_lists[folder_name] = []

        glob_folder = os.path.join('outputs', folder_name, '*.csv')
        file_list = glob.glob(glob_folder)

        # for each file add to a list in the top level dictionary
        for file in file_list:
            # add the raw file to dataLists ready for
            data_lists[file.split(os.path.sep)[1]].append(pd.read_csv(file, header=-1))


def create_response_map(output_path, data_lists, geojson):

    # create some default output
    responded_list = []

    for df in data_lists['Responded']:

        df.columns = ['rep', 'district', 'hh_id', 'hh_type', 'time']
        int_df = pd.DataFrame({'Returns': df.groupby(['rep', 'district', 'hh_type']).size()}).reset_index()
        responded_list.append(pd.DataFrame(int_df.groupby(['district']).sum()['Returns']))

    district_size_list = []

    # collate total hh outputs
    for df in data_lists['Total_hh']:
        df.columns = ['rep', 'district', 'hh_count']
        district_size_list.append(pd.DataFrame(df.groupby(['district']).mean()['hh_count']))

    perc_ret = pd.DataFrame((responded_list[0].join(district_size_list[0])))
    perc_ret = perc_ret[['Returns']].div(perc_ret.hh_count, axis=0)
    perc_ret.to_csv(os.path.join(output_path, "perc_ret.csv"))

    create_maps.create_choropleth(geojson, "outputs/perc_ret.csv", "inputs/LA_hh.csv")


def create_visit_map(output_path, data_lists, geojson):

    # create some output
    visit_list = []

    for df in data_lists['Visit']:

        df.columns = ['rep', 'district', 'hh_id', 'hh_type', 'time']
        int_df = pd.DataFrame({'Visits': df.groupby(['rep', 'district', 'hh_type']).size()}).reset_index()
        visit_list.append(pd.DataFrame(int_df.groupby(['district']).sum()['Visits']))

    district_size_list = []

    # collate total hh outputs
    for df in data_lists['Total_hh']:
        df.columns = ['rep', 'district', 'hh_count']
        district_size_list.append(pd.DataFrame(df.groupby(['district']).mean()['hh_count']))

    visits_done = pd.DataFrame((visit_list[0].join(district_size_list[0])))
    visits_done = visits_done[['Visits']].div(visits_done.hh_count, axis=0)
    visits_done.to_csv(os.path.join(output_path, "visits.csv"))

    create_maps.create_choropleth(geojson, "outputs/visits.csv", "inputs/LA_hh.csv")





