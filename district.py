"""A district represents any user defined region (such as an LSOA/LA) that contains a set number of HH
and shares a number of census officers"""
import householdv2
import sys
import censusv2
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

        # list of households in the district
        self.households = []

        # create the households that will exist in the district
        self.create_households()
        # record total households created for region?
        self.output_data.append(total_hh(self.rep.reps, self.name, int(len(self.households))))

        # create action plans for the district
        # self.action_plan = self.create_action_plans()

        # create Census Officers that will work in the district
        # self.create_co(self.input_data["census officer"], "")

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

    def create_action_plans(self):

        return censusv2.ActionPlan(self.env, self.name, self.households)

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
                            censusv2.CensusOfficer(self.env, self, self.action_plan)

                except IOError as e:
                    print(e)
                    sys.exit()
                break

        return input_key

