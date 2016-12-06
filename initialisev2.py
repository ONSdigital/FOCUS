"""rep class is the instance that contains all that is required for current rep of simulation"""
import district
import simpy
import censusv2
import datetime
import helper as h


class Rep(object):
    """contains the methods and data for an individual replication"""

    def __init__(self, env, input_data, output_data, rnd, run, sim_hours, reps, out_path):

        # values passed to the class
        self.env = env
        self.input_data = input_data
        self.output_data = output_data
        self.rnd = rnd
        self.run = run
        self.sim_hours = sim_hours
        self.start_date = datetime.datetime.strptime(self.input_data['start_date'], '%Y, %m, %d, %H, %M, %S')
        self.end_date = datetime.datetime.strptime(self.input_data['end_date'], '%Y, %m, %d, %H, %M, %S')
        self.reps = reps
        self.output_path = out_path

        # variables created within the class - belonging to it
        self.total_ad_instances = 0
        self.total_web_chat_instances = 0
        self.total_returns = 0
        self.districts = []  # list containing each instance of the district class
        self.ad_avail = []  # list of all the available advisers
        self.total_hh = 0  # used to store total number of hh created across all districts
        self.total_co = 0

        # methods to run on start
        self.total_ad_instances = self.input_data['advisers']['number']
        if self.total_ad_instances > 0:
            self.adviser_store = simpy.FilterStore(self.env, capacity=self.total_ad_instances)

        # create common resources
        self.create_advisers(self.input_data['advisers'], "")  # Call centre staff

        # create districts
        self.create_districts()

    def create_advisers(self, input_data, input_key):

        # create the advisers - both web and phone
        id_ad_num = 0
        for i in range(int(input_data["number"])):

            self.ad_avail.append(censusv2.Adviser(self, id_ad_num, input_data))

            id_ad_num += 1

    def create_districts(self):

        CO_number = 0

        list_of_districts = sorted(list(self.input_data['districts'].keys()))

        for distr in list_of_districts:

            # checks size of output and writes to file if too large
            if (h.dict_size(self.output_data)) > 10000000:
                h.write_output(self.output_data, self.output_path, self.reps)

            self.districts.append(district.District(self,
                                                    self.rnd,
                                                    self.env,
                                                    distr,
                                                    self.input_data['districts'][distr],
                                                    self.output_data))

            CO_number += self.input_data['districts'][distr]["census officer"]["standard"]["number"]

        print("number of CO: ", CO_number)
