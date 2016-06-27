"""A district represents any user defined region (such as an LSOA/LA) that contains a set number of HH
and shares a number of census officers"""
import householdv2
import sys
import censusv2
from simpy.util import start_delayed
from collections import namedtuple

total_hh = namedtuple('Total_hh', ['reps', 'district', 'count'])


class District(object):

    def __init__(self, rep, rnd, env, name, input_data, output_data):
        # values fed into class
        self.rep = rep
        self.rnd = rnd
        self.env = env
        self.name = name
        self.input_data = input_data
        self.output_data = output_data

        self.households = []  # list of households in the district
        self.district_co = []  # list of CO assigned to the district

        self.co_working = []
        self.co_not_working = []

        self.create_households()
        self.output_data['Total_hh'].append(total_hh(self.rep.reps, self.name, len(self.households)))
        self.start_fu()  # process used to commence FU activities for the district
        self.create_co(self.input_data["census officer"], "")

    def create_households(self):

        list_of_hh = sorted(list(self.input_data['households'].keys()))
        # for hh in self.input_data['households']:
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

                self.rep.total_hh += 1

    def start_fu(self):

        hh_list = sorted(list(self.input_data['households'].keys()))
        delay = min([self.input_data['households'][hh]['FU_start_time'] for hh in hh_list])
        start_delayed(self.env, censusv2.start_fu(self.env, self), delay)

    def create_co(self, input_data, input_key):

        id_num = 0
        for key, value in input_data.items():
            if isinstance(value, dict):

                self.create_co(value, key)

            else:
                try:
                    if 'number' in input_data:
                        for i in range(int(input_data["number"])):
                            id_num += 1
                            # print('CO created in ' + str(self.name))
                            # create CO
                            self.district_co.append(censusv2.CensusOfficer(self.rep,
                                                                           self.env,
                                                                           self,
                                                                           input_data))

                except IOError as e:
                    print(e)
                    sys.exit()
                break

        return input_key

