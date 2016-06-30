import glob
import os
import post_process

output_path = os.path.join(os.getcwd(), 'outputs')

data_lists = {}

folder_list = glob.glob("outputs/*/")

for folder in folder_list:

    folder_name = folder.split(os.path.sep)[1]
    data_lists[folder_name] = []

    glob_folder = os.path.join('outputs', folder_name, '*.csv')
    file_list = glob.glob(glob_folder)

    post_process.aggregate(output_path, data_lists)



post_process.create_response_map(output_path, data_lists, 'inputs/geog_E+W_LAs.geojson')