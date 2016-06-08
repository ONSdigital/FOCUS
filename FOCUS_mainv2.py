import sys
import json
import datetime
import random
import simpy
import initialisev2
import os

# read in configuration file - use a default if nothing is selected
input_path = input('Enter input file path or press enter to use defaults: ')
if len(input_path) < 1:
    file_name = 'single multi district.JSON'
    input_path = os.path.join(os.getcwd(), file_name)

# loads the selected config file
try:
    with open(input_path) as data_file:
        input_data = json.load(data_file)  # dict of the whole file

# if something goes wrong exit
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

except IOError as e:
    print(e)
    sys.exit()

# create high level loop
list_of_runs = sorted(list(input_data.keys()), key=int)  # returns top level of config file

# cycle through the runs - could pass runs to separate processes?
for run in list_of_runs:

    # pull out length of sim for current run
    sim_start = datetime.datetime.strptime(input_data[run]['start_date'], '%Y, %m, %d, %H, %M, %S')
    sim_end = datetime.datetime.strptime(input_data[run]['end_date'], '%Y, %m, %d, %H, %M, %S')
    sim_hours = (sim_end - sim_start).total_seconds()/3600

    # number of replications to run
    replications = input_data[run]['replications']

    # run each replication
    for rep in range(replications):

        # set a random seed based on current date and current run
        now = datetime.datetime.now()
        seed_date = datetime.datetime(2012, 4, 12, 19, 00, 00)
        seed = abs(now - seed_date).total_seconds() + int(run)
        rnd = random.Random()
        rnd.seed(seed)
        # and write to dict as record of each reps seed
        input_data[run]['replication seeds'][rep] = seed

        # define simpy env for rep
        env = simpy.Environment()

        # and initialise rep
        current_run = initialisev2.Rep(env, input_data[run], rnd, rep + 1, seed)

        env.run(until=sim_hours)
        rep += 1

        # and then dump JSON config file with seeds to the output folder for record keeping
        output_JSON_name = str(now.strftime("%Y-%m-%d %H:%M")) + '.JSON'
        with open(os.path.join(output_path, output_JSON_name), 'w') as outfile:
            json.dump(input_data, outfile)

