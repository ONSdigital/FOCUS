"""A district represents any user defined region (such as an LSOA/LA) that contains a set number of HH
and shares a number of census officers"""
import householdv2
import sys
import censusv2


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

        # create action plans for the district
        self.action_plan = self.create_action_plans()

        # create Census Officers that will work in the district
        self.create_co(self.input_data["census officer"], "")

    def create_households(self):

        for hh in self.input_data['households']:

            for i in range(self.input_data['households'][hh]['number']):

                print(hh)
                # create instance of HH class
                self.households.append(householdv2.Household(self.rep,
                                                             self.rnd,
                                                             self.env,
                                                             hh,
                                                             self.input_data['households'][hh],
                                                             self.output_data))

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
                            print('CO created in ' + str(self.name))
                            # create CO
                            censusv2.CensusOfficer(self.env, self, self.action_plan)

                except IOError as e:
                    print(e)
                    sys.exit()
                break

        return input_key

