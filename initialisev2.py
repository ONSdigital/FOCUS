"""rep class is the instance that contains all that is required for current rep of simulation"""
import district
import simpy
import censusv2
import datetime
import helper as h


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
        # self.ad_avail = []  # list of all the available advisers

        # self.total_ad_instances = 0
        self.total_returns = 0
        self.total_hh = 0  # total number of hh across all districts
        self.total_co = 0  # total number of co across all districts

        # methods to run on initiation
        # self.total_ad_instances = self.input_data['advisers']['number']
        # if self.total_ad_instances > 0:
        #     self.adviser_store = simpy.FilterStore(self.env, capacity=self.total_ad_instances)

        # create common resources
        # self.create_advisers(self.input_data['advisers'], "")  # Call centre staff

        self.create_districts()  # initialises districts

    def create_advisers(self, input_data, input_key):

        # create the advisers - both web and phone
        id_ad_num = 0
        for i in range(int(input_data["number"])):

            self.ad_avail.append(censusv2.Adviser(self, id_ad_num, input_data))

            id_ad_num += 1

    def create_districts(self):

        co_number = 0

        list_of_districts = sorted(list(self.input_data['districts'].keys()))

        for distr in list_of_districts:

            # checks size of output file and writes to file if too large
            if (h.dict_size(self.output_data)) >= self.max_output_file_size:
                h.write_output(self.output_data, self.output_path, self.reps)

            self.districts.append(district.District(self,
                                                    distr))

            co_number += self.input_data['districts'][distr]["census officer"]["standard"]["number"]

        #print("number of CO: ", co_number)
