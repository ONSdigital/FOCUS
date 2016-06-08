"""rep class is the instance that contains all that is required for current rep of simulation"""
import district
import simpy
import censusv2


class Rep(object):
    """contains the methods and data for an individual replication"""

    def __init__(self, env, input_data, rnd, reps, seed):

        # values fed into class
        self.env = env
        self.input_data = input_data
        self.rnd = rnd
        self.reps = reps
        self.seed = seed

        # variables created within the class
        self.total_ad_instances = 0
        self.total_web_chat_instances = 0
        self.districts = []  # list containing each instance of the district class
        self.ad_avail = []  # list of all teh available advisers

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

            print('adviser created')
            self.ad_avail.append(censusv2.Adviser(self))

            id_ad_num += 1

    def create_districts(self):

        for distr in self.input_data['districts']:

            print(distr)

            self.districts.append(district.District(self, self.rnd, self.env, distr, self.input_data['districts'][distr]))
