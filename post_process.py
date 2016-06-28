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


def create_response_map(geojson, output_path, data_lists ):

    # create some default output
    return_list = []

    for df in data_lists['Return']:

        df.columns = ['rep', 'district', 'hh_id', 'hh_type', 'time']
        int_df = pd.DataFrame({'Returns': df.groupby(['rep', 'district', 'hh_type']).size()}).reset_index()
        return_list.append(pd.DataFrame(int_df.groupby(['district']).sum()['Returns']))

    district_size_list = []

    # collate total hh outputs
    for df in data_lists['Total_hh']:
        df.columns = ['rep', 'district', 'hh_count']
        district_size_list.append(pd.DataFrame(df.groupby(['district']).mean()['hh_count']))

    returns = pd.DataFrame((return_list[0].join(district_size_list[0])))
    returns = returns[['Returns']].div(returns.hh_count, axis=0)
    returns.to_csv(os.path.join(output_path, "returns.csv"))

    create_maps.create_choropleth(geojson, "outputs/returns.csv", "inputs/LA_hh.csv", "Return rates", False)


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
        # check below works correct with multiple reps - may need average of sum by below groupings
        int_df = pd.DataFrame({'outcome': df.groupby(['rep', 'district', 'hh_type']).size()}).reset_index()
        visit_outcome_list.append(pd.DataFrame(int_df.groupby(['district']).sum()['outcome']))

    visits_done = pd.DataFrame((visit_list[0].join(visit_outcome_list[0])))
    visits_done = visits_done[['outcome']].div(visits_done.Visits, axis=0)
    # output the percentage of visits that have the outcome as per the input.
    visits_done.to_csv(os.path.join(output_path, "visits.csv"))

    # get headers of created scv file
    # and supp file?

    create_maps.create_choropleth(geojson, "outputs/visits.csv", "inputs/LA_hh.csv", visit_type, True)





