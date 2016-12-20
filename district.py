"""A district represents any user defined region (such as an LSOA/LA) that contains a set number of HH
and shares a number of census officers"""
import householdv2
import sys
import censusv2
import math
from helper import returns_to_date
from simpy.util import start_delayed
from collections import namedtuple
import helper as h

hh_count = namedtuple('hh_count', ['district', 'hh_count'])
hh_record = namedtuple('hh_record', ['district', 'hh_type'])
return_times = namedtuple('Returned', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'time'])
response = namedtuple('Response', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'time'])


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
        # new process to determine time of first interaction - visits, letters
        # simple value for now
        self.first_interaction = 24*10

        self.create_households()
        # need to include totals inc earliers in the below count ****
        if self.rep.reps == 1:
            self.output_data['hh_count'].append(hh_count(self.name, len(self.households)))
        self.start_fu()  # process used to commence FU activities for the district
        # self.create_co(self.input_data["census officer"])

        # self.create_letterphases()

        try:
            self.hh_area = self.input_data['district_area'] / len(self.households)
            self.initial_hh_sep = 2 * (math.sqrt(self.hh_area / math.pi))
        except ZeroDivisionError as e:
            print(e, " in district ", self.name)
            sys.exit()

        self.env.process(self.hh_separation())
        self.env.process(self.start_hh())

    def start_hh(self):
        # all start at the same time at present - in reality not all will receive IAC at same time
        for household in self.households:
            self.env.process(household.action())

        yield self.env.timeout((self.rep.sim_hours) - self.env.now)

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

            # get hh data for current type
            hh_input_data = self.input_data['households'][hh]

            for i in range(hh_input_data['number']):

                # determine initial HH action
                initial_action = self.initial_action(hh_input_data, self.first_interaction, hh)

                if initial_action[0] == 'early':
                    # don't need an instance of a household just directly record a response/return at correct time

                    self.rep.total_returns += 1

                    self.rep.output_data['Returned'].append(return_times(self.rep.reps,
                                                                         self.name,
                                                                         hh_input_data["LA"],
                                                                         hh_input_data["LSOA"],
                                                                         initial_action[1],
                                                                         hh,
                                                                         initial_action[2]))
                else:
                    # create a household instance passing initial state
                    self.households.append(householdv2.Household(self.rep,
                                                                 self.rnd,
                                                                 self.env,
                                                                 self,
                                                                 self.rep.total_hh,
                                                                 hh,
                                                                 hh_input_data,
                                                                 self.output_data,
                                                                 initial_action))

                if self.rep.reps == 1:
                    self.output_data['hh_record'].append(hh_record(self.name,
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

    def initial_action(self, input_data, first_interaction, hh):

        digital = h.set_preference(input_data['paper_prop'],
                                   self.rnd)

        if digital or h.str2bool(input_data['paper_allowed']):
            # use default
            behaviour = 'default'
        else:
            # use alt
            behaviour = 'alt'

        # set values to use
        hh_resp = input_data['behaviours'][behaviour]['response']
        hh_help = input_data['behaviours'][behaviour]['help']

        response_test = self.rnd.uniform(0, 100)  # represents the COA to be taken.
        if response_test <= hh_resp:
            # respond but test when
            return self.early_responder(input_data, digital, first_interaction, hh)
        elif hh_resp < response_test <= hh_help:
            # call for help return when
            return self.help(input_data, digital, first_interaction, hh)
        else:
            # do nothing return 0 time
            return self.do_nothing(input_data, digital, first_interaction, hh)

    def early_responder(self, input_data, digital, first_interaction, hh):

        response_time = h.set_household_response_time(self.rep,
                                                      input_data,
                                                      self.rep.sim_hours)

        if digital and response_time + input_data['delay']['digital'] <= first_interaction:

            self.rep.output_data['Response'].append(response(self.rep.reps,
                                                             self.name,
                                                             input_data["LA"],
                                                             input_data["LSOA"],
                                                             digital,
                                                             hh,
                                                             response_time))

            return ['early', digital, response_time + input_data['delay']['digital']]

        elif not digital and h.str2bool(input_data['paper_allowed']) \
                and response_time + input_data['delay']['paper'] <= first_interaction:

            self.rep.output_data['Response'].append(response(self.rep.reps,
                                                             self.name,
                                                             input_data["LA"],
                                                             input_data["LSOA"],
                                                             digital,
                                                             hh,
                                                             response_time))

            return ['early', digital, response_time + input_data['delay']['paper']]

        else:

            return ['late', digital, response_time]

    def help(self, input_data, digital, first_interaction, hh):

        return ['help', digital, 0]

    def do_nothing(self, input_data, digital, first_interaction, hh):
        return ['do_nothing', digital, 0]


def least_busy_CO(district):

    min_length = min([len(co.action_plan) for co in district.district_co])

    coord = [co for co in district.district_co if len(co.action_plan) == min_length]

    return coord[0]






