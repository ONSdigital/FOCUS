"""Main control file"""
import json
import datetime as dt
import random
import simpy
import initialise
import os
import shutil
import post_process as pp
import copy
from collections import defaultdict
from itertools import repeat
from multiprocessing import cpu_count, Pool, freeze_support, Lock
import helper as hp
import pandas as pd
import output_options as oo
import glob
import gzip

import matplotlib.pyplot as plt
plt.style.use('ggplot')

l = Lock()


def start_run(run_input, seeds, max_districts, out_path):

    max_output_file_size = 100000000
    start_date = dt.date(*map(int, run_input['start_date'].split(',')))
    end_date = dt.date(*map(int, run_input['end_date'].split(',')))
    sim_hours = (end_date - start_date).total_seconds()/3600
    census_date = dt.date(*map(int, run_input['census_date'].split(',')))
    census_day = (census_date - start_date).days

    # write key dates/info to a csv for later use in post processing
    temp_list = [{'start_date': start_date,
                  'end_date': end_date,
                  'census_date': census_date,
                  'census_day': census_day,
                  'sim_hours': sim_hours}]

    # create dataframe to hold summary data
    days = int(sim_hours / 24)+1
    day_cols = []
    for day in range(0, days):
        day_cols.append(day)

    # generate list of codes for reference from raw inputs
    la_list = hp.generate_list(os.path.join(os.getcwd(), 'raw_inputs', 'la lookup.csv'), 0)
    lsoa_list = hp.generate_list(os.path.join(os.getcwd(), 'raw_inputs', 'lsoa lookup.csv'), 0)
    district_list = [str(district) for district in range(1, max_districts+1)]
    dig_list = ['0', '1']
    hh_type_list = [str(hh_type) for hh_type in range(1, 16)]

    passive_summary = {'la': dict((la_list[i], [0]*days) for i in range(0, len(la_list))),
                       'digital': dict((dig_list[i], [0]*days) for i in range(0, len(dig_list))),
                       'hh_type': dict((hh_type_list[i], [0]*days) for i in range(0, len(hh_type_list))),
                       'district_name': dict((district_list[i], [0]*days) for i in range(0, len(district_list)))}

    active_summary = {'la': dict((la_list[i], [0]*days) for i in range(0, len(la_list))),
                      'digital': dict((dig_list[i], [0] * days) for i in range(0, len(dig_list))),
                      'hh_type': dict((hh_type_list[i], [0] * days) for i in range(0, len(hh_type_list))),
                      'district_name': dict((district_list[i], [0]*days) for i in range(0, len(district_list)))}

    active_totals = {'lsoa': dict((lsoa_list[i], 0) for i in range(0, len(lsoa_list))),
                     'la': dict((la_list[i], 0) for i in range(0, len(la_list))),
                     'district_name': dict((district_list[i], 0) for i in range(0, len(district_list)))}

    passive_totals = {'lsoa': dict((lsoa_list[i], 0) for i in range(0, len(lsoa_list))),
                      'la': dict((la_list[i], 0) for i in range(0, len(la_list))),
                      'district_name': dict((district_list[i], 0) for i in range(0, len(district_list)))}

    # possible extra summary outputs are visits over time...

    l.acquire()
    if oo.record_key_info:
        if not os.path.isdir(os.path.join(out_path, 'key info')):
            os.mkdir(os.path.join(out_path, 'key info'))

        if not os.path.isfile(os.path.join(out_path, 'key dates', run_input['run_id'] + ".csv")):
            pd.DataFrame(temp_list).to_csv(os.path.join(out_path, 'key info', run_input['run_id'] + ".csv"))
        else:
            pd.DataFrame(temp_list).to_csv(os.path.join(out_path, 'key info', run_input['run_id'] + ".csv"), mode='a',
                                           header=False)
    l.release()

    output_data = defaultdict(list)

    rnd = random.Random()
    rnd.seed(str(seeds))

    # define simpy env for current rep
    env = simpy.Environment()

    # initialise replication
    initialise.Rep(env,
                   run_input,
                   output_data,
                   passive_summary,
                   active_summary,
                   active_totals,
                   passive_totals,
                   rnd,
                   sim_hours,
                   start_date,
                   census_day,
                   out_path,
                   max_output_file_size)

    # and run it
    env.run(until=sim_hours)

    # write the output to csv files
    hp.write_output(output_data, out_path, run_input['run_id'])

    # write summary data to csv to defined folder in main outputs
    summary_path = os.path.join(out_path, 'summary')

    c_run = run_input['run_id']
    c_rep = run_input['rep_id']

    hp.output_summary(summary_path, passive_summary, 'passive_summary', c_run, c_rep)
    hp.output_summary(summary_path, passive_totals, 'passive_totals', c_run, c_rep)
    hp.output_summary(summary_path, active_summary, 'active_summary', c_run, c_rep)
    hp.output_summary(summary_path, active_totals, 'active_totals', c_run, c_rep)


def produce_default_output(current_path):
    """Produces default charts and outputs if turned on. If not leaves the raw data untouched."""

    # line chart of overall responses over time
    pp.produce_rep_results(current_path)
    default_path = os.path.join(current_path, 'summary', 'active_summary', 'digital')
    pp.plot_summary(default_path, reps=False, cumulative=True, individual=False)

    # pyramid chart showing comparison of two strategies on LSOA return rates
    pandas_data = pp.csv_to_pandas(current_path, ['hh_record'])
    input_left = pd.read_csv(os.path.join(current_path, 'summary', 'passive_totals', 'lsoa', 'average.csv'), index_col=0)
    name_left = 'Passive'
    input_right = pd.read_csv(os.path.join(current_path, 'summary', 'active_totals', 'lsoa', 'average.csv'), index_col=0)
    name_right = 'Active'

    pp.sum_pyramid(pandas_data['hh_record'], input_left, input_right, name_left, name_right, bin_size=2)


if __name__ == '__main__':

    create_new_config = False
    produce_default = True
    multiple_processors = True  # set to false to debug
    delete_old = False
    freeze_support()

    # delete all old output files but not the main directory.
    if delete_old:
        if os.path.isdir('outputs/'):
            dirs = os.listdir(os.path.join(os.getcwd(), 'outputs'))
            for d in dirs:
                if d != 'outputs/':
                    shutil.rmtree(os.path.join(os.getcwd(), 'outputs', d))

    first_cwd = os.getcwd()
    input_path = os.path.join(os.getcwd(), 'inputs')
    # get list of files to run here rather than loading them in
    glob_folder = os.path.join(input_path, '*.JSON')
    simulation_list = glob.glob(glob_folder)  # get a list of all files(sim runs) in the folder

    outputs = 'outputs'
    output_path = os.path.join(os.getcwd(), outputs)

    sims = len(simulation_list)
    sim_counter = 1
    # for each sim file in the input folder
    for sim in simulation_list:

        os.chdir(first_cwd)  # reset to overall working directory each time
        sim_name = os.path.basename(sim)[:-5]  # get filename without the .JSON extension
        sub_path_name = sim_name + " " + str(dt.datetime.now().strftime("%Y""-""%m""-""%d %H.%M.%S"))
        output_JSON_name = sim_name + " " + str(dt.datetime.now().strftime("%Y""-""%m""-""%d %H.%M.%S")) + '.JSON'
        current_output_path = os.path.join(output_path, sub_path_name)
        if not os.path.isdir(current_output_path):
            os.makedirs(current_output_path)

        with open(sim) as data_file:
            input_data = json.load(data_file)

        # get list of all districts in simulation
        list_of_districts = sorted(list(input_data.keys()), key=int)
        districts = max([int(district) for district in list_of_districts])
        reps = input_data['1']['replications']  # extract replications
        max_runs = reps * districts

        # define a list to be used to map all run/replication combinations to available processors
        run_list = []
        seed_dict = {}
        seed_list = []
        dict_all = defaultdict()

        st = dt.datetime.now()
        print('Simulations started at: ', st)
        counter = 0

        # place, with random seeds, a copy of the run/rep into the run list
        for district in list_of_districts:
            input_data[district]['run_id'] = district
            seed_dict[str(district)] = {}
            for rep in range(1, input_data[district]['replications'] + 1):

                if str(rep) not in input_data[district]['replication seeds']:
                    now = dt.datetime.now()
                    seed_date = dt.datetime(2012, 4, 12, 19, 00, 00)
                    seed = abs(now - seed_date).total_seconds() + int(district) + rep
                    seed_dict[str(input_data[district]['run_id'])][str(rep)] = seed
                    if not str(input_data[district]['run_id']) in dict_all:
                        dict_all[str(input_data[district]['run_id'])] = {}
                    dict_all[str(input_data[district]['run_id'])][str(rep)] = seed
                    create_new_config = True

                else:
                    seed = input_data[district]['replication seeds'][str(rep)]

                seed_list.append(seed)

                input_data[district]['rep_id'] = rep
                run_list.append(copy.deepcopy(input_data[district]))
                counter += 1

                # if run list len is chunk size run them...
                if len(run_list) == cpu_count()*20 or (district == str(districts) and rep == reps):

                    # different run methods - use single processor for debugging
                    if multiple_processors:
                        pool = Pool(cpu_count())
                        pool.starmap(start_run,
                                     zip(run_list, seed_list, repeat(districts), repeat(current_output_path)))
                        pool.close()
                        pool.join()
                    else:
                        for i in range(len(run_list)):
                            start_run(run_list[i], seed_list[i], districts, current_output_path)

                    # calculate finish time for current simulation and print progress
                    time_now = dt.datetime.now()
                    time_left = ((time_now - st).seconds / (counter / max_runs)) - (time_now - st).seconds
                    finish_time = time_now + dt.timedelta(seconds=time_left)

                    print(pp.roundup((counter / max_runs) * 100, 1), "percent of current simulation complete. "
                                                                     "Projected finish time is:", finish_time)
                    run_list = []
                    seed_dict[str(district)] = {}
                    seed_list = []

        # add back in the ability to output a record, with seeds, as to what has been run...
        # dict all contains the required seeds

        if create_new_config:

            json_file_path = os.path.join(output_path, output_JSON_name)

            list_of_seed_runs = sorted(list(dict_all.keys()), key=int)
            # first assign the seeds...
            for run in list_of_seed_runs:
                input_data[run]['replication seeds'] = dict_all[run]
                # delete ids
                del input_data[run]['run_id']
                del input_data[run]['rep_id']

            with open(json_file_path, 'w') as outfile:
                json.dump(input_data, outfile)

            with open(json_file_path, 'rb') as f_in:
                with gzip.open(json_file_path + '.gz', 'w') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # delete orig JSON file
            os.remove(json_file_path)

            """to read a gzip file
            with gzip.open('file.txt.gz', 'rb') as f:
                file_content = f.read()
            """

        print('Simulation complete at time: ', dt.datetime.now())

        if produce_default:
            print("Starting post processing.")
            produce_default_output(current_output_path)

        cet = dt.datetime.now()
        print(sim_counter, 'of', sims, 'complete at time', cet)
        sim_counter += 1

    # overall end time
    et = dt.datetime.now()
    print('All simulations complete at time: ', et)
