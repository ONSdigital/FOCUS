import csv
import simpy
from simpy.util import start_delayed
import census
import household
import datetime
import math


class Run(object):
    """contains the methods and data for an individual run"""

    def __init__(self, env, input_data, output_data,  letter_input, letter_data_file, rnd, run, reps, seed):

        self.env = env
        self.input_data = input_data
        self.output_data = output_data
        self.start_date = datetime.datetime.strptime(input_data['start_date'], '%Y, %m, %d, %H, %M, %S').date()
        self.end_date = datetime.datetime.strptime(input_data['end_date'], '%Y, %m, %d, %H, %M, %S').date()
        self.sim_days = (self.end_date - self.start_date).days
        self.letter_input = letter_input
        self.letter_data_file = letter_data_file
        self.rnd = rnd
        self.run = run
        self.reps = reps
        self.seed = seed

        self.initial_hh_sep = 0  # initial average hh separation - does not change once set
        self.travel_time = 0  # changes based on number of households who have responded
        self.total_responses = 0  # simply tally of total responses
        self.fu_start = 0  # variable created ready to store time of FU
        self.total_enu_instances = 0
        self.total_ad_instances = 0
        self.total_ad_chat_instances = 0

        self.district = []  # will contain the households that are due to be visited
        self.copy_district = []  # the copy does not change - effectively it represents the address register
        self.hh_count = {'htc1': 0, 'htc2': 0, 'htc3': 0, 'htc4': 0, 'htc5': 0}
        self.enu_avail = []  # default list use to hold enu instances when not working
        self.enu_working = []  # list to store instances of working enumerators
        self.ad_storage_list = []
        self.ad_working = []
        self.ad_chat_storage_list = []
        self.ad_chat_working = []
        self.letters = []  # contains details of the letter phases, when, who etc
        self.incomplete = []  # a list containing the households who submitted incomplete responses

        self.hh_numbers_list = []
        self.visit_list = []

        self.letter_data_dict = {}  # reference for effect of letters

        self.FU_on = True
        self.call_FU_on = False
        self.send_letters = str2bool(self.input_data['letters_on'])

        self.load_letter_data()  # from here run the methods to set the initial conditions
        self.load_letter_phases()  # creates the list of letter phases
        self.create_households(self.input_data['households'])
        self.calc_fu_start(self.input_data['households'])
        self.start_fu_visits()  # places events in event list to enable coordinator to update visits list
        self.create_enumerators(self.input_data["collectors"], self.start_date)
        self.create_advisers(self.input_data["advisers"], "")
        if self.total_ad_instances > 0:
            self.adviser_store = simpy.FilterStore(self.env, capacity=self.total_ad_instances)
        if self.total_ad_chat_instances > 0:
            self.adviser_chat_store = simpy.FilterStore(self.env, capacity=self.total_ad_chat_instances)
        if self.send_letters is True:
            self.start_letters()  # places events in event list to send letters
        self.resp_day(self.sim_days*24)

    """load letter data from passed file into dictionary for later reference"""
    def load_letter_data(self):
        # load the file with the letter phases info into the letters list
        with open(self.letter_data_file, mode="r") as infile:
            reader = csv.DictReader(infile)
            for rowdict in reader:
                temp_key = rowdict['letter_type']
                rowdict.pop('letter_type')
                self.letter_data_dict[temp_key] = rowdict

    """load letter phases from passed file into letters list"""
    def load_letter_phases(self):
        # load the file with the letter phases info into the letters list
        with open(self.letter_input, mode="r") as infile:
            reader = csv.reader(infile)
            next(reader, None)
            for row in reader:
                self.letters.append(row)

    """creates the households and calculates the initial hh separation"""
    def create_households(self, input_dict):

        id_num = 0

        for hh_type, value in input_dict.items():

            for i in range(input_dict[hh_type]['number']):

                self.district.append(household.Household(self, self.env, hh_type, id_num, input_dict[hh_type]))
                id_num += 1

        # then define for the run the initial distance between houses
        hh_area = self.input_data['district_area'] / len(self.district)
        self.initial_hh_sep = 2*(math.sqrt(hh_area/math.pi))

    """set in hours when fu starts based on input dates"""
    def calc_fu_start(self, input_data):

        # create the advisers

        for key, value in input_data.items():
            if isinstance(value, dict):

                self.calc_fu_start(value)
            elif key == "FU_start_time":
                if self.fu_start == 0 or value < self.fu_start:
                    self.fu_start = value

    """starts the FU visits -  or rather starts the coordinator who makes the visit list"""
    def start_fu_visits(self):
        start_delayed(self.env, census.fu_startup(self, self.env, self.district, self.output_data), self.fu_start)

    """create instances of an enumerator class that will be available between set times from a set date
    until the sim ends"""

    def create_enumerators(self, input_data, input_key):

        id_num = 0
        for key, value in input_data.items():
            if isinstance(value, dict):

                self.create_enumerators(value, key)
            elif key == "number":
                #print(int(input_data["number"]))
                for i in range(int(input_data["number"])):

                    self.enu_avail.append(census.alt_Enumerator(self,
                                                                id_num,
                                                                input_data['start_time'],
                                                                input_data['end_time'],
                                                                input_data['start_date'],
                                                                input_data['end_date'],
                                                                input_key,
                                                                input_data['travel_speed'],
                                                                self.input_data['households'],  # households to visit
                                                                self.FU_on))
                    id_num += 1
                    self.total_enu_instances = id_num

        return input_key

    def create_advisers(self, input_data, input_key):

        # create the advisers
        id_ad_num = 0
        id_ad_chat_num = 0
        for key, value in input_data.items():
            if isinstance(value, dict):

                self.create_advisers(value, key)
            elif key == "number" and ("telephone" in input_key) == True:
                for i in range(int(input_data["number"])):

                    self.ad_storage_list.append(census.Adviser(self,
                                                               id_ad_num,
                                                               input_data["start_time"],
                                                               input_data["end_time"],
                                                               input_data["start_date"],
                                                               input_data["end_date"],
                                                               input_key,
                                                               input_data["FU_on"]))
                    id_ad_num += 1
                    self.total_ad_instances = id_ad_num

            elif key == "number" and ("web" in input_key) == True:
                for i in range(int(input_data["number"])):

                    self.ad_chat_storage_list.append(census.AdviserChat(self,
                                                                        id_ad_chat_num,
                                                                        input_data["start_time"],
                                                                        input_data["end_time"],
                                                                        input_data["start_date"],
                                                                        input_data["end_date"],
                                                                        input_key))
                    id_ad_chat_num += 1
                    self.total_ad_chat_instances = id_ad_chat_num

        return input_key

    """scheduling of events to start the posting of letters at defined times"""
    def start_letters(self):

        if len(self.letters) > 0 and self.send_letters is True:
            for row in self.letters:
                start_delayed(self.env, census.letter_startup(self, self.env, self.district, self.output_data,
                                                              self.sim_days, str2bool(row[1]), row[2], row[3]), int(row[0]))

    """Prints the percentage of responses received by group at past delay"""
    def resp_day(self, delay):

        start_delayed(self.env, census.print_resp(self), delay - 24)

"""converts string inputs to a bool"""
def str2bool(value):
    return str(value).lower() in ("True", "true", "1")





