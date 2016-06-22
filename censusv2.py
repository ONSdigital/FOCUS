"""Module used to store the classes related to census..."""


# a helper process that creates an instance of a coordinator class and starts it working
def start_fu(env, district):

    StartFU(env, district)
    yield env.timeout(0)


class Adviser(object):
    """Call centre adviser - multitasking"""

    def __init__(self, rep):

        self.rep = rep
        self.rep.env.process(self.add_to_store())

    # method to transfer the adviser to the store ready to be claimed
    def add_to_store(self):

        self.rep.ad_avail.remove(self)
        self.rep.adviser_store.put(self)
        yield self.rep.env.timeout(0)


class StartFU(object):
    """represents the RMT creating the visit lists for the assigned districts"""
    def __init__(self, env, district):

        self.env = env
        self.district = district
        self.households = self.district.households

        self.visit_list = []

        self.env.process(self.arrange_visits())

    def arrange_visits(self):

            self.visit_list = []

            for household in self.households:
                self.visit_list.append(household)
                #print(str(household.hh_type) + ' added to ' + str(self.district) + ' FU list')

            yield self.env.timeout(24)
            self.env.process(self.arrange_visits())


class CensusOfficer(object):
    """represents an individual Census Officer. Each instance can be different"""

    def __init__(self, env, district, action_plan):

        self.env = env
        self.district = district
        self.action_plan = action_plan

        # self.env.process(self.contact())

    def contact(self):

        if len(self.action_plan.visit_list) > 0:
            current_hh = self.action_plan.visit_list.pop(0)
            #print(str(self) + 'Visiting ' + str(current_hh.hh_type) + ' at time ' + str(self.env.now))
            yield self.env.timeout(1)
        else:
            yield self.env.timeout(24)

        self.env.process(self.contact())








