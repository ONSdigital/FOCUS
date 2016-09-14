"""A district represents any user defined region (such as an LSOA/LA) that contains a set number of HH
and shares a number of census officers"""
import householdv2
import sys
import censusv2
import math
from helper import returns_to_date
from simpy.util import start_delayed
from collections import namedtuple

hh_count = namedtuple('hh_count', ['rep', 'district', 'hh_count'])
hh_record = namedtuple('hh_record', ['rep', 'district', 'hh_type'])


class District(object):

    def __init__(self, rep, rnd, env, name, input_data, output_data):
        # values fed into class
        self.rep = rep
        self.rnd = rnd
        self.env = env
        self.name = name
        self.input_data = input_data
        self.output_data = output_data

        # belong to the class
        self.households = []  # list of households in the district
        self.district_co = []  # list of CO assigned to the district
        self.reminders = []  # list of reminders to be sent
        self.return_rate = 0
        self.travel_dist = 0

        # processes to run
        self.create_households()
        self.output_data['hh_count'].append(hh_count(self.rep.reps, self.name, len(self.households)))
        self.start_fu()  # process used to commence FU activities for the district
        self.create_co(self.input_data["census officer"])

        self.create_letterphases()

        self.hh_area = self.input_data['district_area'] / len(self.households)
        self.initial_hh_sep = 2*(math.sqrt(self.hh_area/math.pi))

        self.env.process(self.hh_separation())

    # takes current response rate and calculates hh separation based on current response rate.
    def hh_separation(self):

        while True:

            try:
                self.travel_dist = self.initial_hh_sep / (math.sqrt(1 - (returns_to_date(self, "%"))))
            except ZeroDivisionError:
                self.travel_dist = 0

            yield self.env.timeout(24)

    def create_households(self):

        list_of_hh = sorted(list(self.input_data['households'].keys()))
        for hh in list_of_hh:

            for i in range(self.input_data['households'][hh]['number']):

                # create instance of HH class
                self.households.append(householdv2.Household(self.rep,
                                                             self.rnd,
                                                             self.env,
                                                             self,
                                                             self.rep.total_hh,
                                                             hh,
                                                             self.input_data['households'][hh],
                                                             self.output_data))

                self.output_data['hh_record'].append(hh_record(self.rep.reps,
                                                               self.name,
                                                               hh))

                self.rep.total_hh += 1

    def start_fu(self):

        hh_list = sorted(list(self.input_data['households'].keys()))
        delay = min([self.input_data['households'][hh]['FU_start_time'] for hh in hh_list])
        start_delayed(self.env, censusv2.start_fu(self.env, self), delay)

    def create_letterphases(self):

        letter_list = sorted(list(self.input_data['letter_phases']))

        for letter in letter_list:
            letter_data = self.input_data['letter_phases'][letter]
            self.reminders.append(censusv2.LetterPhase(self.env,
                                                       self.rep,
                                                       self,
                                                       letter_data))

    def create_co(self, input_data, input_key=""):

        id_num = 0
        for key, value in input_data.items():
            if isinstance(value, dict):

                self.create_co(value, key)

            else:
                try:
                    if 'number' in input_data:
                        for i in range(int(input_data["number"])):
                            id_num += 1
                            self.district_co.append(censusv2.CensusOfficer(self.rep,
                                                                           self.env,
                                                                           self,
                                                                           input_data,
                                                                           self.rep.total_co))

                            self.rep.total_co += 1

                except IOError as e:
                    print(e)
                    sys.exit()
                break

        return input_key


def least_busy_CO(district):

    min_length = min([len(co.action_plan) for co in district.district_co])

    coord = [co for co in district.district_co if len(co.action_plan) == min_length]

    return coord[0]





