import sys
import json
import datetime
import random
import simpy
import initialisev2

file_name = input('Enter input file name: ')
if len(file_name) < 1:
    file_name = 'single multi district.JSON'

# loads the selected config file
try:
    with open(file_name) as data_file:
        input_data = json.load(data_file)  # dict of the whole file

except IOError as e:
    print(e)
    sys.exit()

list_of_runs = sorted(list(input_data.keys()), key=int)  # returns top level of config file

for run in list_of_runs:

    # pull out inputs common across current run
    sim_start = datetime.datetime.strptime(input_data[run]['start_date'], '%Y, %m, %d, %H, %M, %S')
    sim_end = datetime.datetime.strptime(input_data[run]['end_date'], '%Y, %m, %d, %H, %M, %S')
    sim_hours = (sim_end - sim_start).total_seconds()/3600

    replications = input_data[run]['replications']

    # run for each replication
    for rep in range(replications):

        # set a random seed
        now = datetime.datetime.now()
        seed_date = datetime.datetime(2012, 4, 12, 19, 00, 00)
        seed = abs(now - seed_date).total_seconds() + int(run)
        rnd = random.Random()
        rnd.seed(seed)
        # and write to JSON file dict as record of each reps seed
        input_data[run]['replication seeds'][rep] = seed

        # define simpy env for run
        env = simpy.Environment()

        # and initialise
        current_run = initialisev2.Run(env, input_data[run], rnd, run, rep + 1, seed)

        env.run(until=sim_hours)
        rep += 1

        # and then dump JSON config file to output file location





