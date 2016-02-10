import csv
import simpy
from simpy.util import start_delayed
import census
import household
import datetime
import math


class Run(object):
    """contains the methods and data for an individual run"""

    def __init__(self, env, input_data, output_data, sim_start, sim_days, enu_shifts, letter_input, letter_data_file,
                 adviser_shifts, adviser_chat_shifts, rnd, run, reps, seed):

        self.env = env
        self.input_data = input_data
        self.output_data = output_data
        self.sim_start = sim_start
        self.sim_days = sim_days
        self.enu_shifts = enu_shifts
        self.letter_input = letter_input
        self.letter_data_file = letter_data_file
        self.adviser_shifts = adviser_shifts
        self.adviser_chat_shifts = adviser_chat_shifts
        self.rnd = rnd
        self.run = run
        self.reps = reps
        self.seed = seed

        self.initial_hh_sep = 0  # initial average hh separation - does not change once set
        self.travel_time = 0  # changes based on number of households who have responded
        self.total_responses = 0  # simply tally of total responses
        self.fu_start = 0  # variable created ready to store time of FU
        self.total_enu_instances = 0

        self.district = []  # will contain the households that are due to be visited
        self.copy_district = []  # the copy does not change - effectively it represents the address register
        self.hh_count = {'htc1': 0, 'htc2': 0, 'htc3': 0, 'htc4': 0, 'htc5': 0}
        self.enu_avail = []  # default list use to hold enu instances when not working
        self.enu_working = []  # list to store instances of working enumerators
        self.ad_storage_list = []
        self.ad_chat_storage_list = []
        self.letters = []  # contains details of the letter phases, when, who etc
        self.incomplete = []  # a list containing the households who submitted incomplete responses

        self.hh_numbers_list = []
        self.visit_list = []

        self.enu_dict = {}   # reference dict for enu schedule
        self.adviser_dict = {}  # reference dict for adviser schedule
        self.adviser_chat_dict ={}
        self.letter_data_dict = {}  # reference for effect of letters

        self.FU_on = True
        self.call_FU_on = False
        self.send_letters = str2bool(self.input_data['letters_on'])

        self.load_adviser_data(self.adviser_dict)
        self.load_adviser_data(self.adviser_chat_dict)
        self.adviser_cap = max(int(d['capacity']) for d in self.adviser_dict.values())   # max possible advisers for run
        self.adviser_store = simpy.FilterStore(self.env, capacity=self.adviser_cap)  # don't need to define cap but good idea
        self.adviser_chat_cap = max(int(d['capacity']) for d in self.adviser_chat_dict.values())   # max possible advisers for run
        self.adviser_chat_store = simpy.Store(self.env, capacity=self.adviser_chat_cap) # don't need to define cap but good idea
        self.load_schedule(self.enu_shifts, self.enu_dict, 'Date')
        self.load_letter_data()  # from here run the methods to set the initial conditions
        self.load_letter_phases()  # creates the list of letter phases
        self.create_households(self.input_data['households'])
        self.calc_fu_start()  # must run before enumerators are created
        self.start_fu_visits()  # places events in event list to enable coordinator to update visits list
        #self.create_enu_instances(self.enu_dict, self.sim_start)
        self.create_alt_enu_instances(self.input_data["collectors"], self.sim_start)
        #self.create_adviser_instances()
        self.alt_create_adviser_instances(self.input_data["advisers"], self.sim_start)
        self.start_letters()  # places events in event list to send letters
        #self.resp_day(10*24)  # simple outputs by day
        #self.resp_day(self.fu_start)  # simple outputs by day
        #self.resp_day(3*24)
        #self.resp_day(9*24)
        self.resp_day(29*24)

    def load_file_to_dict(self, input_filename, input_dict, main_key, sub_key):
        # loads csv file into a dictionary
        with open(input_filename, 'r') as input_file:
            reader = csv.DictReader(input_file)
            for row in reader:
                input_dict[row[main_key]] = row[sub_key]

    """load adviser data from passed file into dictionary for later reference"""
    def load_adviser_data(self, input_dict):
        with open(self.adviser_shifts, mode="r") as infile:
            reader = csv.DictReader(infile)
            for rowdict in reader:
                temp_key = rowdict['date']
                rowdict.pop('date')
                input_dict[temp_key] = rowdict

    """loads passed schedule into a dictionary"""
    def load_schedule(self, input_filename, input_dict, variable):
        # loads shift patterns from csv into nested dictionary

        with open(input_filename, mode="r") as input_file:
            reader = csv.DictReader(input_file)
            for rowdict in reader:
                temp_key = rowdict[variable]
                rowdict.pop(variable)
                input_dict[temp_key] = rowdict

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
    def calc_fu_start(self):

        fu_start_date = datetime.datetime.strptime(min(self.enu_dict), '%Y-%m-%d').date()
        self.fu_start = (fu_start_date - self.sim_start.date()).days * 24

    """starts the FU visits -  or rather starts the coordinator who makes the visit list"""
    def start_fu_visits(self):
        start_delayed(self.env, census.fu_startup(self, self.env, self.district, self.output_data), self.fu_start)

    """create instances of an enumerator class that will be available between set times from a set date
    until the sim ends"""
    def create_enu_instances(self, input_dict, input_date):

        id_num = 0
        for key, value in input_dict.items():
            if isinstance(value, dict):
                # date = key
                self.create_enu_instances(value, key)
            else:
                for i in range(int(value)):
                    start_time = int(key.split('-')[0])
                    end_time = int(key.split('-')[1])
                    self.enu_avail.append(census.Enumerator(self, id_num, start_time, end_time, input_date,
                                                            self.input_data['households'], self.FU_on))
                    id_num += 1
                    self.total_enu_instances = id_num

        return input_date

    def create_alt_enu_instances(self, input_data, input_key):

        id_num = 0
        for key, value in input_data.items():
            if isinstance(value, dict):

                self.create_alt_enu_instances(value, key)
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

    """schedule processes that updates how many instances of the adviser classes exists for the current day"""
    def create_adviser_instances(self):
        # create all the advisers here
        # then as part of the initiation start a process that adds them to the store at the right time
        # and then removes them at the right time
        for i in range(self.adviser_cap):
            self.ad_storage_list.append(census.Adviser(self, i, self.call_FU_on))  # set to true to do FU_calls

        for i in range(self.adviser_chat_cap):
            self.ad_chat_storage_list.append(census.AdviserChat(self, i))

        # schedule update processes here - updates to the number of advisers in the store
        for day in range(self.sim_days):
            delay = day*24
            if delay == 0:
                self.env.process(census.pop_advisers(self, self.adviser_dict, self.ad_storage_list, self.adviser_store))
                self.env.process(census.pop_advisers(self, self.adviser_chat_dict, self.ad_chat_storage_list,
                                                     self.adviser_chat_store))
            else:
                start_delayed(self.env, census.pop_advisers(self, self.adviser_dict, self.ad_storage_list,
                                                            self.adviser_store), delay)
                start_delayed(self.env, census.pop_advisers(self, self.adviser_chat_dict,
                                                            self.ad_chat_storage_list, self.adviser_chat_store),
                              delay)

    def alt_create_adviser_instances(self, input_data, input_key):

        # create the advisers
        id_num = 0
        for key, value in input_data.items():
            if isinstance(value, dict):

                self.alt_create_adviser_instances(value, key)
            elif key == "number":
                for i in range(int(input_data["number"])):

                    self.ad_storage_list.append(census.Adviser(self,
                                                               id_num,
                                                               input_data["start_time"],
                                                               input_data["end_time"],
                                                               input_data["start_date"],
                                                               input_data["end_date"],
                                                               input_key,
                                                               input_data["FU_on"]))
                    id_num += 1

        return input_key

        # create all the advisers here
        # then as part of the initiation start a process that adds them to the store at the right time
        # and then removes them at the right time
         # set to true to do FU_calls
        # schedule update processes here - updates to the number of advisers in the store


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





