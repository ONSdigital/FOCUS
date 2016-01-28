"""Main file for control of FOCUS"""
import random
import initialize
import csv
from itertools import groupby
import simpy
import datetime
from collections import namedtuple
import json

replication = namedtuple('Replication', ['type', 'run', 'rep', 'time', 'seed'])

letter_data_file = 'in_letter_data.csv'  # details the effects the various types of letters have
letter_input = 'in_letter_phases.csv'  # details when each type of letter will be sent and to whom
enu_shifts = 'in_enu_shifts.csv'  # when enumerators are available
adviser_shifts = 'in_adviser_shifts.csv'
adviser_chat_shifts = 'in_adviser_chat_shifts.csv'

# allow user to select desired configuration file
file_name = input('Enter file name: ')
if len(file_name) < 1:
    file_name = 'data_alt.JSON'

# loads the selected config file
with open(file_name) as data_file:
    input_data = json.load(data_file)  # dict of the whole file

list_of_runs = sorted(list(input_data.keys()), key=int)  # returns top level of config file - iterate through this list to do all the runs


for run in list_of_runs:

    try:

        output_data = []  # for output...

        sim_start = datetime.datetime.strptime(input_data[run]['start_date'], '%Y, %m, %d, %H, %M, %S')
        sim_end = datetime.datetime.strptime(input_data[run]['end_date'], '%Y, %m, %d, %H, %M, %S')
        sim_days = (sim_end - sim_start).days

        replications = input_data[run]['replications']

        reps = 0
       # print(input_data[run]['description'])

        for reps in range(replications):
            #print('Run: ', run, ' replication: ', reps + 1)
            now = datetime.datetime.now()
            seed_date = datetime.datetime(2012, 4, 12, 19, 00, 00)
            seed = abs(now - seed_date).total_seconds() + int(run)
            rnd = random.Random()
            rnd.seed(seed)
            env = simpy.Environment()
            output_data.append(replication('start', int(run), reps + 1, now, seed))
            current_run = initialize.Run(env, input_data[run], output_data, sim_start, sim_days, enu_shifts, letter_input,
                                         letter_data_file, adviser_shifts, adviser_chat_shifts, rnd, run, reps + 1)

            env.run(until=sim_days*24)

            now = datetime.datetime.now()
            output_data.append(replication('end', int(run), reps + 1, now, seed))
            reps += 1

        output_data.sort(key=lambda x: type(x).__name__)

        for k, g in groupby(output_data, lambda x: type(x).__name__):
            with open('outputs/{}.csv'.format(k), 'w', newline='') as f_output:  # use a for append
                csv_output = csv.writer(f_output)
                rows = list(g)
                csv_output.writerow(list(rows[0]._fields))
                for row in rows:
                    csv_output.writerow(list(row))
    except:
        pass


