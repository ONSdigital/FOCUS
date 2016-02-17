import csv
import simpy
from simpy.util import start_delayed
import census
import household
import datetime
import math


class Run(object):
    """contains the methods and data for an individual run"""

    def __init__(self, env, input_data, output_data, rnd, run, reps, seed):

        self.env = env
        self.input_data = input_data
        self.output_data = output_data
        self.start_date = datetime.datetime.strptime(input_data['start_date'], '%Y, %m, %d, %H, %M, %S')
        self.end_date = datetime.datetime.strptime(input_data['end_date'], '%Y, %m, %d, %H, %M, %S')
        self.sim_hours = (self.end_date.date() - self.start_date.date()).total_seconds()/3600
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

        # temp only
        self.letter_sent = 0
        self.letter_effect = 0

        self.district = []  # will contain the households that are due to be visited
        self.copy_district = []  # the copy does not change - effectively it represents the address register

        '# create a dictionary to store a simple count of hh'
        self.hh_count = {}
        self.htc_resp = {}
        self.list_of_hh = sorted(list(self.input_data['households']))  # top level keys only, hh in this case
        for hh in self.list_of_hh:
            self.hh_count[hh] = 0

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

        self.FU_on = True
        self.call_FU_on = False

        # create the instances of the objects in the simulation
        self.create_households(self.input_data['households'])
        self.calc_fu_start(self.input_data['households'])
        self.start_coordinator()  # places events in event list to enable coordinator to update visits list
        self.create_enumerators(self.input_data["collector"], self.start_date)
        self.create_advisers(self.input_data["advisers"], "")

        # create required stores but only if there are resources to put inside them
        if self.total_ad_instances > 0:
            self.adviser_store = simpy.FilterStore(self.env, capacity=self.total_ad_instances)
        if self.total_ad_chat_instances > 0:
            self.adviser_chat_store = simpy.FilterStore(self.env, capacity=self.total_ad_chat_instances)

        # schedule the sending of letters
        self.start_letters(self.input_data["households"])

        # some simple output
        self.resp_day(self.sim_hours)

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
    def start_coordinator(self):
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

                    self.enu_avail.append(census.Enumerator(self,
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
    def start_letters(self, input_dict):

        # input dict is the entire hh section of current run
        list_of_hh = sorted(list(input_dict.keys()))  # top level keys only, hh in this case

        for hh in list_of_hh:

            letter_phases = input_dict[hh]["letter_phases"]  # dict of just the letters for that hh

            list_of_letters = sorted(list(letter_phases.keys()))  # top level keys only
            if len(list_of_letters) != 0:

                for letter in list_of_letters:
                    letter_date = datetime.datetime.strptime(letter_phases[letter]["date"], '%Y, %m, %d, %H, %M, %S')
                    self.letter_sent = (letter_date - self.start_date).total_seconds()/3600

                    # below only applicable if only 1 letter sent- for simple output only - delete in long term

                    self.letter_effect = letter_phases[letter]["effect"]

                    if self.letter_sent != 0:
                        start_delayed(self.env, census.letter_startup(self,
                                                                      self.env,
                                                                      self.district,
                                                                      self.output_data,
                                                                      self.sim_hours,
                                                                      str2bool(letter_phases[letter]["targeted"]),
                                                                      letter,
                                                                      letter_phases[letter]["effect"],
                                                                      hh,
                                                                      letter_phases[letter]["delay"]),
                                      self.letter_sent)
                    else:
                        self.env.process(census.letter_startup(self,
                                                               self.env,
                                                               self.district,
                                                               self.output_data,
                                                               self.sim_hours,
                                                               str2bool(letter_phases[letter]["targeted"]),
                                                               letter,
                                                               letter_phases[letter]["effect"],
                                                               hh,
                                                               letter_phases[letter]["delay"]))

    def resp_day(self, delay):

        start_delayed(self.env, census.print_resp(self), delay - 24)


def str2bool(value):
    return str(value).lower() in ("True", "true", "1")





