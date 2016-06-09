"""Main control file"""
import sys
import json
import datetime
import random
import simpy
import initialisev2
import os
import csv
import shutil
import pandas as pd
import glob
from itertools import groupby

# set required flags
create_new_config = False

# delete all old output files from default location - but not generated JSON files
if os.path.isdir('outputs/') is True:
    dirs = [x[0] for x in os.walk('outputs/')]
    for d in dirs:
        if d != 'outputs/':
            shutil.rmtree(str(d))

# read in input configuration file - use a default if nothing is selected
input_path = input('Enter input file path or press enter to use defaults: ')
if len(input_path) < 1:
    file_name = 'single multi district.JSON'
    input_path = os.path.join(os.getcwd(), file_name)

# loads the selected config file
try:
    with open(input_path) as data_file:
        input_data = json.load(data_file)  # dict of the whole file

# if something goes wrong exit with error
except IOError as e:
    print(e)
    sys.exit()

# ask for output destination
output_path = input('Enter output path or press enter to use default: ')
if len(output_path) < 1:
    outputs = 'outputs'
    output_path = os.path.join(os.getcwd(), outputs)

try:
    # create if does not exist
    if os.path.isdir(output_path) is False:
        os.makedirs(output_path)

# if something goes wrong exit with error
except IOError as e:
    print(e)
    sys.exit()


# create list of runs in config file
list_of_runs = sorted(list(input_data.keys()), key=int)  # returns top level of config file

# cycle through the runs
for run in list_of_runs:

    # pull out length of sim for current run
    sim_start = datetime.datetime.strptime(input_data[run]['start_date'], '%Y, %m, %d, %H, %M, %S')
    sim_end = datetime.datetime.strptime(input_data[run]['end_date'], '%Y, %m, %d, %H, %M, %S')
    sim_hours = (sim_end - sim_start).total_seconds()/3600

    # number of replications to run
    replications = input_data[run]['replications']

    # run each replication
    for rep in range(replications):

        output_data = []  # a list to store namedtuples that record each event in the simulation

        # set a random seed based on current date and current rep unless seed exists
        if str(rep) not in input_data[run]['replication seeds']:
            now = datetime.datetime.now()
            seed_date = datetime.datetime(2012, 4, 12, 19, 00, 00)
            seed = abs(now - seed_date).total_seconds() + int(run)

        else:
            seed = input_data[run]['replication seeds'][str(rep)]

        rnd = random.Random()
        rnd.seed(seed)
        # and write to dict as record of each reps seed
        if str(rep) not in input_data[run]['replication seeds']:
            input_data[run]['replication seeds'][rep] = seed
            create_new_config = True

        # define simpy env for current rep
        env = simpy.Environment()

        # initialise replication
        current_rep = initialisev2.Rep(env, input_data[run], output_data, rnd, run, sim_hours, rep + 1, seed)

        # and run it
        env.run(until=sim_hours)

        # then create the csv output files for the current replication
        output_data.sort(key=lambda x: type(x).__name__)

        for k, g in groupby(output_data, lambda x: type(x).__name__):
            if os.path.isdir('outputs/{}'.format(k) + '/') is False:
                os.mkdir('outputs/{}'.format(k) + '/')
            with open('outputs/{}'.format(k) + '/' + str(run) + '.csv', 'a', newline='') as f_output:  # use a for append
                csv_output = csv.writer(f_output)
                rows = list(g)
                # uncomment below to add headers based on named tuples
                # csv_output.writerow(list(rows[0]._fields))
                for row in rows:
                    csv_output.writerow(list(row))

# then, if required, dump JSON config file with seeds to the output folder
if create_new_config is True:
    output_JSON_name = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + '.JSON'
    with open(os.path.join(output_path, output_JSON_name), 'w') as outfile:
        json.dump(input_data, outfile)

# get list of all directories
dirList = glob.glob("outputs/*/")
dataLists = {}

# for each directory
for raw_directory_path in dirList:
    # get the files
    folder = raw_directory_path.split("/")[1]
    dataLists[folder] = []
    fileList = glob.glob('outputs/' + folder + '/*.csv')
    # for each file add to a list in the top level dictionary
    for raw_file_path in fileList:
        print(raw_file_path)
        # add the raw file to dataLists ready for further processing
        dataLists[raw_file_path.split("/")[1]].append(pd.read_csv(raw_file_path, header=-1))

# create some default output
default_key = 'Responded'

Responded_list = []

for df in dataLists[default_key]:

    df.columns = ['rep', 'district', 'hh_id', 'hh_type', 'time']
    # print(df.sort(['rep', 'district'], ascending=[1, 0]))
    print(df)

    int_df = pd.DataFrame({'count': df.groupby(['rep', 'district', 'hh_type']).size()}).reset_index()

    print(int_df)
    Responded_list.append(pd.DataFrame(int_df.groupby(['district', 'hh_type']).mean()['count']))

    print(Responded_list[len(Responded_list)-1])





























