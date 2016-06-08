"""file used to store the classes and definitions related to the households represented in the simulation """


class Household(object):

    # Create an instance of the class
    def __init__(self, rep, rnd, env, hh_type, input_data):

        self.rep = rep
        self.rnd = rnd
        self.env = env
        self.hh_type = hh_type
        self.input_data = input_data

        self.resp_level = self.input_data['default_resp']
        self.help_level = self.input_data['default_help']
        self.status = ''

        self.rep.env.process(self.action())

    def action(self):

        action_test = self.rnd.uniform(0, 100)  # represents the COA to be taken.

        if action_test <= self.resp_level:

            print('respond')

        elif self.resp_level < action_test <= self.help_level:

            print('call')
            contact_time = 1  # all call at same time for testing
            self.status = "making contact"
            yield self.env.timeout(contact_time)
            yield self.env.process(self.contact())

        else:

            print('nothing')

    def contact(self):

        print(str(self.hh_type) + "  called at " + str(self.env.now))

        current_ad = yield self.rep.adviser_store.get()
        print(str(current_ad) + " is with " + str(self.hh_type) + " at time " + str(self.env.now))
        yield self.env.timeout(0.5)
        self.rep.adviser_store.put(current_ad)
