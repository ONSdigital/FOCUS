"""file used to store the classes and definitions related to the households represented in the simulation """


class Household(object):

    # Create an instance of the class
    def __init__(self, rnd, env, hh_type, input_data):

        self.rnd = rnd
        self.env = env
        self.hh_type = hh_type
        self.input_data = input_data

        self.resp_level = self.input_data['default_resp']
        self.help_level = self.input_data['default_help']

        self.action()

    def action(self):

        action_test = self.rnd.uniform(0, 100)  # represents the COA to be taken.

        if action_test <= self.resp_level:

            print('respond')

        elif self.resp_level < action_test <= self.help_level:
                # ask for help

            print('call')

        else:

            print('nothing')