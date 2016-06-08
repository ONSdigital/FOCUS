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


class ActionPlan(object):
    """represents the coordinator for the assigned district"""
    def __init__(self, env, district, households):

        self.env = env
        self.district = district
        self.households = households

        self.visit_list = []

        self.env.process(self.arrange_visits())

    def arrange_visits(self):
        while True:

            self.visit_list = []

            for household in self.households:
                self.visit_list.append(household)
                print(str(household.hh_type) + ' added to ' + str(self.district) + ' FU list')

            yield self.env.timeout(1000)


class CensusOfficer(object):
    """represents an individual Census Officer. Each instance can be different"""

    def __init__(self, env, district, action_plan):

        self.env = env
        self.district = district
        self.action_plan = action_plan

        self.env.process(self.contact())

    def contact(self):

        if len(self.action_plan.visit_list) > 0:
            current_hh = self.action_plan.visit_list.pop(0)
            print(str(self) + 'Visiting ' + str(current_hh.hh_type) + ' at time ' + str(self.env.now))
            yield self.env.timeout(1)





