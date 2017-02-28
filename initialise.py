"""rep class is the instance that contains all that is required for current rep of simulation"""
import district
import hq
import helper as h
import simpy
import sys
from collections import namedtuple, defaultdict

warnings = namedtuple('Warnings', ['rep', 'warning', 'detail'])


class Rep(object):
    """contains the methods and data for an individual replication"""

    def __init__(self, env, input_data, output_data, rnd, sim_hours, start_date, census_day, out_path,
                 max_output_file_size):

        # values passed to the class
        self.env = env
        self.input_data = input_data
        self.output_data = output_data
        self.rnd = rnd
        self.sim_hours = sim_hours
        self.start_date = start_date
        self.census_day = census_day
        self.output_path = out_path
        self.max_output_file_size = max_output_file_size

        # variables created within the class - belonging to it
        self.run = self.input_data['run id']
        self.reps = self.input_data['rep id']
        self.start_day = start_date.weekday()

        self.districts = []  # list containing each instance of the district class
        self.ad_avail = []  # list of all the available advisers

        self.total_ad_instances = 0
        self.total_responses = 0
        self.total_hh = 0  # total number of hh across all districts
        self.total_co = 0  # total number of co across all districts

        # methods to run on initiation
        ad_inputs = self.input_data['advisers']
        self.total_ad_instances = sum([ad_inputs[adviser]["number"] for adviser in ad_inputs])
        if self.total_ad_instances > 0:
            self.adviser_store = simpy.FilterStore(self.env, capacity=self.total_ad_instances)

        # create common resources
        # self.create_advisers(self.input_data['advisers'], "")  # Call centre
        self.adviser_types = defaultdict(dict)
        if self.total_ad_instances > 0:
            self.create_advisers()
            self.add_to_store()

        self.create_districts()  # initialises districts

    def create_advisers(self):

        id_num = 0
        list_of_adviser_types = sorted(list(self.input_data['advisers'].keys()))
        for adviser_type in list_of_adviser_types:

            # get data for current type
            adviser_input_data = self.input_data['advisers'][adviser_type]

            try:

                if int(adviser_input_data['number']) > 0:

                    for i in range(int(adviser_input_data['number'])):
                        id_num += 1
                        self.ad_avail.append(hq.Adviser(self,
                                                        id_num,
                                                        adviser_input_data,
                                                        adviser_type))

            except KeyError:
                print("Error when creating advisers in run: ", self.run, " replication: ", self.reps)
                sys.exit()

    # add advisers to store
    def add_to_store(self):

        for adviser in self.ad_avail:
            self.adviser_types[adviser.type]['start_time'] = adviser.start_sim_time
            self.adviser_types[adviser.type]['end_time'] = adviser.end_sim_time
            self.adviser_types[adviser.type]['availability'] = adviser.set_avail_sch

            self.adviser_store.put(adviser)

    def create_districts(self):

        co_number = 0

        list_of_districts = sorted(list(self.input_data['districts'].keys()))

        for distr in list_of_districts:

            print(distr)

            # checks size of output file and writes to file if too large
            if (h.dict_size(self.output_data)) >= self.max_output_file_size:
                h.write_output(self.output_data, self.output_path, self.reps)

            self.districts.append(district.District(self,
                                                    distr))

            try:
                co_number += self.input_data['districts'][distr]["census officer"]["standard"]["number"]
            except KeyError as e:
                warning_detail = "no CO for run, ", self.run, " in create districts"
                self.output_data['Warnings'].append(warnings(self.reps,
                                                             e,
                                                             warning_detail))
                co_number = 0

        print("number of CO: ", co_number)
