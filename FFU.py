import math
import datetime as dt
import output_options as oo
import hq
import helper as h
from simpy.util import start_delayed


# a helper process that creates an instance of a StartFU class and starts it working
def start_fu(env, district):

    StartFU(env, district)
    yield env.timeout(0)


class StartFU(object):
    """represents the RMT creating the visit lists for the assigned districts and CO"""
    def __init__(self, env, district):

        self.env = env
        self.district = district
        self.update = self.district.input_data['RMT_update']

        self.visit_list = []

        self.env.process(self.create_visit_lists())

    def create_visit_lists(self):

        if h.responses_to_date(self.district) < self.district.input_data["trigger"]:
            # only create action plans if the target rate has not been reached

            self.visit_list = []

            self.visit_list = [household for household in self.district.households
                               if not household.responded
                               and household.visits < household.input_data['max_visits']
                               and h.time_from_start(self.district.rep, household.input_data['FU_start_date']) <= self.env.now]

            # order by priority
            self.visit_list.sort(key=lambda hh: hh.priority, reverse=False)

            j = 0
            temp_co_list = []
            for co in self.district.district_co:
                if h.date_between(co.rep, co.start_date, self.env.now, co.end_date):
                    temp_co_list.append(co)

            num_of_co = len(temp_co_list)
            action_plan_list = []

            for i in range(num_of_co):
                action_plan = self.visit_list[i::num_of_co]
                action_plan_list.append(action_plan)

            #if co.rep.districts[0].district == "1":
            #    print("time ", self.env.now, " action plans", len(action_plan_list), " co's ", len(temp_co_list))

            for co in temp_co_list:
                co.action_plan = action_plan_list[j]
                j += 1

            yield self.env.timeout(self.update)
            self.env.process(self.create_visit_lists())

        else:
            # returns to date over trigger point so no need to do any more FU...
            for co in self.district.district_co:
                co.action_plan = []

            self.district.district_co = []


class CensusOfficer(object):
    """represents an individual Census Officer. Each instance can be different"""

    def __init__(self, rep,  env, district, input_data, co_id):

        self.rep = rep
        self.env = env
        self.district = district
        self.input_data = input_data
        self.co_id = co_id

        self.rnd = self.rep.rnd
        self.action_plan = []
        self.start_date = dt.datetime.strptime((self.input_data['start_date']), '%Y, %m, %d').date()
        self.end_date = dt.datetime.strptime((self.input_data['end_date']), '%Y, %m, %d').date()
        self.has_pq = h.str2bool(self.input_data['has_pq'])
        self.has_postcard = h.str2bool(self.input_data['has_postcard'])
        self.start_sim_time = h.get_entity_time(self)  # self.co_start_time()  # the sim time the co starts work
        self.end_sim_time = h.get_entity_time(self, "end")  # self.co_end_time()  # the sim time the co ends work

        self.active_time = []  # as above but for active time
        # start work at correct time
        start_delayed(self.env, self.co_working_test(), self.start_sim_time)

    def optimal_time(self, household):
        # determines if the current time is the best time to visit a hh that has asked for a visit

        # get times avail
        d = household.input_data['contact_rate'][str(h.current_day(household))]

        # if current time is not best time don't visit
        if d[h.return_time_key(d, self.env.now)] != d[max(d, key=d.get)]:
            return False
        else:
            # visit
            return True

    def return_next_visit(self, live=False):
        # if live tests if a return has been received since the action plans have been created.

        if self.action_plan:
            household = self.action_plan.pop(0)
        else:
            return None

        if live and household.returned:
            return self.return_next_visit(live)
        elif household.arranged_visit and not self.optimal_time(household):
            # put hh back in list at correct place and then get the next one...unless no other hh to visit
            if self.action_plan:
                print(household.hh_id)
                try:
                    self.action_plan.insert(10, household)
                except ReferenceError:
                    print('error')
            else:
                return household

            return self.return_next_visit(live)
        else:
            return household

    def co_working_test(self):

        if not self.district.district_co and not self.action_plan:
            #  if removed from district no more work to do
            # timeout until end of sim
            end_time = self.rep.sim_hours - self.env.now
            yield self.env.timeout(end_time)

        elif self.working() and self.action_plan:

            # get the first household to visit
            household = self.return_next_visit()

            if not household:
                # if no more visits to do that day finish until next day
                yield self.env.timeout(self.next_available())
            else:
                yield self.env.process(self.fu_visit_contact(household))
                self.env.process(self.co_working_test())

        else:
            yield self.env.timeout(self.next_available())
            self.env.process(self.co_working_test())

    def household_test(self, household, input_type):
        # tests if hh is in or is converted to a return!

        # added to quickly test dropping effectiveness of visits from lfs values - not final!!
        temp_factor = 1
        if input_type == "contact_rate":
            temp_factor = 0.5

        test_value = self.rnd.uniform(0, 100)
        dict_value = household.input_data[input_type][str(h.current_day(self))]

        if test_value <= dict_value[h.return_time_key(dict_value, self.env.now)]*temp_factor:
            return True
        else:
            return False

    def fu_visit_contact(self, household):

        household.priority += 1  # automatically lower the priority of this hh after a visit
        household.visits += 1

        if oo.record_visit_summary:

            for key, value in self.rep.visit_summary.items():
                value[str(getattr(household, key))][math.floor(self.rep.env.now / 24)] += 1

            for key, value in self.rep.visit_totals.items():
                value[str(getattr(household, key))] += 1

        if oo.record_visit:
            self.rep.output_data['Visit'].append(oo.visit_output(self.rep.reps,
                                                                   self.district.district,
                                                                   household.la,
                                                                   household.lsoa,
                                                                   household.digital,
                                                                   household.hh_type,
                                                                   household.hh_id,
                                                                   household.visits,
                                                                   self.env.now))

        if household.responded:
            if oo.record_visit_wasted:
                self.rep.output_data['Visit_wasted'].append(oo.generic_output(self.rep.reps,
                                                                              self.district.district,
                                                                              household.la,
                                                                              household.lsoa,
                                                                              household.digital,
                                                                              household.hh_type,
                                                                              household.hh_id,
                                                                              self.env.now))

        elif household.resp_planned:
            if oo.record_visit_unnecessary:
                self.rep.output_data['Visit_unnecessary'].append(oo.generic_output(self.rep.reps,
                                                                                   self.district.district,
                                                                                   household.la,
                                                                                   household.lsoa,
                                                                                   household.digital,
                                                                                   household.hh_type,
                                                                                   household.hh_id,
                                                                                   self.env.now))

        household_is_in = self.household_test(household, "contact_rate")

        if household_is_in:
            if oo.record_visit_contact:
                self.rep.output_data['Visit_contact'].append(oo.generic_output(self.rep.reps,
                                                                               self.district.district,
                                                                               household.la,
                                                                               household.lsoa,
                                                                               household.digital,
                                                                               household.hh_type,
                                                                               household.hh_id,
                                                                               self.env.now))

            yield self.rep.env.process(self.fu_visit_assist(household))

        elif (not household_is_in and household.visits == household.input_data['max_visits'] and
              h.str2bool(household.input_data['paper_after_max_visits'])):

            household.paper_allowed = True
            hq.schedule_paper_drop(household, 'Visit', 'pq', self.has_pq)

            time_worked = self.input_data["visit_times"]["out_paper"] / 60 +\
                          self.district.travel_dist / self.input_data["travel_speed"]

            if oo.record_time_summary:

                for key, value in self.rep.time_summary.items():
                    value[str(getattr(household, key))][math.floor(self.rep.env.now / 24)] += time_worked

                for key, value in self.rep.time_totals.items():
                    value[str(getattr(household, key))] += time_worked

            yield self.rep.env.timeout(time_worked)

        else:
            # out - add drop off of a note
            if oo.record_visit_out:
                self.rep.output_data['Visit_out'].append(oo.generic_output(self.rep.reps,
                                                                           self.district.district,
                                                                           household.la,
                                                                           household.lsoa,
                                                                           household.digital,
                                                                           household.hh_type,
                                                                           household.hh_id,
                                                                           self.env.now))

            self.env.process(hq.schedule_paper_drop(household, 'Visit', 'postcard', self.has_postcard))

            time_worked = self.input_data["visit_times"]["out"]/60 + self.district.travel_dist / self.input_data["travel_speed"]

            if oo.record_time_summary:

                for key, value in self.rep.time_summary.items():
                    value[str(getattr(household, key))][math.floor(self.rep.env.now / 24)] += time_worked

                for key, value in self.rep.time_totals.items():
                    value[str(getattr(household, key))] += time_worked

            yield self.rep.env.timeout(time_worked)

    def fu_visit_assist(self, household):

        da_test = self.rnd.uniform(0, 100)
        da_effectiveness = self.input_data['da_effectiveness'][household.hh_type]

        time_worked = self.input_data['visit_times']['query']/60

        if oo.record_time_summary:

            for key, value in self.rep.time_summary.items():
                value[str(getattr(household, key))][math.floor(self.rep.env.now / 24)] += time_worked

            for key, value in self.rep.time_totals.items():
                value[str(getattr(household, key))] += time_worked

        yield self.rep.env.timeout(time_worked)

        # if digital, have already responded or can use paper skip straight to the outcome of the visit
        if household.digital or household.return_sent or household.paper_allowed:
            yield self.rep.env.process(self.fu_visit_outcome(household))

        # if not digital have not sent a return then try to persuade them to complete online.
        elif da_test <= da_effectiveness:
            # convert

            time_worked = self.input_data['visit_times']['convert']/60

            if oo.record_time_summary:

                for key, value in self.rep.time_summary.items():
                    value[str(getattr(household, key))][math.floor(self.rep.env.now / 24)] += time_worked

                for key, value in self.rep.time_totals.items():
                    value[str(getattr(household, key))] += time_worked

            yield self.rep.env.timeout(time_worked)

            household.digital = True
            if oo.record_visit_convert:
                self.rep.output_data['Visit_convert'].append(oo.generic_output(self.rep.reps,
                                                                               household.district.district,
                                                                               household.la,
                                                                               household.lsoa,
                                                                               household.digital,
                                                                               household.hh_type,
                                                                               household.hh_id,
                                                                               self.env.now))

            yield self.rep.env.process(self.fu_visit_outcome(household))

        # if not digital, have not sent a return , do not convince to complete online, engaged OR
        # if not digital, have not sent a return , do not convince to complete online, not engaged, max visits reached,
        # then give paper if an option.
        elif household.engaged or (h.responses_to_date(self.district) < self.district.input_data['paper_trigger'] and
                                   household.visits >= 2 and not household.digital and not household.paper_allowed and
                                   h.str2bool(household.input_data['paper_after_max_visits'])):

            # give paper

            household.paper_allowed = True
            self.env.process(hq.schedule_paper_drop(household, 'Visit', 'pq', self.has_pq))

            time_worked = self.input_data['visit_times']['paper'] / 60 + \
                          self.district.travel_dist / self.input_data["travel_speed"]

            if oo.record_time_summary:

                for key, value in self.rep.time_summary.items():
                    value[str(getattr(household, key))][math.floor(self.rep.env.now / 24)] += time_worked

                for key, value in self.rep.time_totals.items():
                    value[str(getattr(household, key))] += time_worked

            yield self.rep.env.timeout(time_worked)

        else:
            # none of the above true so do no more at this visit
            if oo.record_visit_assist:
                self.rep.output_data['Visit_assist'].append(oo.generic_output(self.rep.reps,
                                                                              self.district.district,
                                                                              household.la,
                                                                              household.lsoa,
                                                                              household.digital,
                                                                              household.hh_type,
                                                                              household.hh_id,
                                                                              self.env.now))

            time_worked = self.input_data['visit_times']['paper'] / 60 + \
                          self.district.travel_dist / self.input_data["travel_speed"]

            if oo.record_time_summary:

                for key, value in self.rep.time_summary.items():
                    value[str(getattr(household, key))][math.floor(self.rep.env.now / 24)] += time_worked

                for key, value in self.rep.time_totals.items():
                    value[str(getattr(household, key))] += time_worked

            yield self.rep.env.timeout(time_worked)

    def fu_visit_outcome(self, household):

        household_returns = self.household_test(household, "success_rate")

        if not household.return_sent and household_returns:
            # hh have not responded yet and respond there and then either by paper or digital.
            if oo.record_visit_success:
                self.rep.output_data['Visit_success'].append(oo.visit_output(self.rep.reps,
                                                                               self.district.district,
                                                                               household.la,
                                                                               household.lsoa,
                                                                               household.digital,
                                                                               household.hh_type,
                                                                               household.hh_id,
                                                                               household.visits,
                                                                               self.env.now))

            household.resp_planned = True

            yield self.rep.env.process(household.household_returns(household.calc_delay()))

            time_worked = self.input_data['visit_times']['success'] / 60 + \
                          self.district.travel_dist / self.input_data["travel_speed"]

            if oo.record_time_summary:
                for key, value in self.rep.time_summary.items():
                    value[str(getattr(household, key))][math.floor(self.rep.env.now / 24)] += time_worked

                for key, value in self.rep.time_totals.items():
                    value[str(getattr(household, key))] += time_worked

            yield self.rep.env.timeout(time_worked)

        elif (not household.return_sent and not household_returns and
              h.responses_to_date(self.district) < self.district.input_data['paper_trigger'] and
              household.visits == household.input_data['max_visits'] and
              h.str2bool(household.input_data['paper_after_max_visits']) and
              not household.paper_allowed):
            # hh have not responded but do not respond as a result of the visit.
            # need extra here for when you fail but not at max visits?
            if oo.record_visit_failed:
                self.rep.output_data['Visit_failed'].append(oo.generic_output(self.rep.reps,
                                                                              household.district.district,
                                                                              household.la,
                                                                              household.lsoa,
                                                                              household.digital,
                                                                              household.hh_type,
                                                                              household.hh_id,
                                                                              self.env.now))
            # leave paper in hope they respond?
            household.paper_allowed = True
            hq.schedule_paper_drop(household, 'Visit', 'pq', self.has_pq)

            if oo.record_paper_summary:
                # add to the summary of the amount of paper given

                for key, value in self.rep.paper_summary.items():
                    value[str(getattr(household, key))][0] += 1

                for key, value in self.rep.paper_totals.items():
                    value[str(getattr(household, key))] += 1

            time_worked = self.input_data['visit_times']['failed'] / 60 + \
                          self.district.travel_dist / self.input_data["travel_speed"]

            for key, value in self.rep.time_summary.items():
                value[str(getattr(household, key))][math.floor(self.rep.env.now / 24)] += time_worked

            for key, value in self.rep.time_totals.items():
                value[str(getattr(household, key))] += time_worked

            yield self.rep.env.timeout(time_worked)

        elif not household.return_sent and not household_returns:
            # failed but no max visits so do no more
            if oo.record_visit_failed:
                self.rep.output_data['Visit_failed'].append(oo.generic_output(self.rep.reps,
                                                                              household.district.district,
                                                                              household.la,
                                                                              household.lsoa,
                                                                              household.digital,
                                                                              household.hh_type,
                                                                              household.hh_id,
                                                                              self.env.now))

            time_worked = self.input_data['visit_times']['failed'] / 60 + \
                              self.district.travel_dist / self.input_data["travel_speed"]

            if oo.record_time_summary:

                for key, value in self.rep.time_summary.items():
                    value[str(getattr(household, key))][math.floor(self.rep.env.now / 24)] += time_worked

                for key, value in self.rep.time_totals.items():
                    value[str(getattr(household, key))] += time_worked

            yield self.rep.env.timeout(time_worked)

    def working(self):
        """returns true or false depending on whether or not a CO is available at current date and time"""

        day_of_week = h.current_day(self)

        if self.start_sim_time <= self.env.now < self.end_sim_time and \
                self.input_data['availability'][str(day_of_week)]:

            for i in range(0, len(self.input_data['availability'][str(day_of_week)]), 2):

                start_time = dt.time(*map(int, self.input_data['availability'][str(day_of_week)][i].split(':')))
                end_time = dt.time(*map(int, self.input_data['availability'][str(day_of_week)][i+1].split(':')))

                start_sim_time = h.make_time_decimal(start_time) + math.floor(self.env.now/24)*24
                end_sim_time = h.make_time_decimal(end_time) + math.floor(self.env.now / 24) * 24

                if start_sim_time <= self.env.now < end_sim_time:
                    return True

        return False

    def return_index(self):

        return_index = 0

        for household in self.action_plan:

            if not household.arranged_visit:

                return_index = self.action_plan.index(household) + 1

        return max(return_index, 0)

    def return_dow(self, days_gone_from_start, original_tod, count=0):
        # get next relevant dow based on current sim time

        current_dow = (self.rep.start_date + dt.timedelta(days=days_gone_from_start)).weekday()

        if (not self.input_data['availability'][str(current_dow)] or
           h.str_to_dec(self.input_data['availability'][str(current_dow)][-1]) <= original_tod):
            # empty list
            return self.return_dow(days_gone_from_start + 1, 0, count + 1)
        else:
            # not empty
            return [str(current_dow), count]

    def next_available(self):
        """return the number of hours until the CO is next available or remove from sim if finished"""

        if self.env.now > self.end_sim_time:
            return self.rep.sim_hours - self.env.now

        current_dow = (self.rep.start_date + dt.timedelta(days=math.floor(self.env.now / 24))).weekday()
        dow = self.return_dow(math.floor(self.env.now / 24), self.env.now % 24)
        # if action plan is zero,so no houses to visits always delay to the start of the next valid day
        if not self.action_plan:
            # this actually just jumps it to the end of the day
            return 24 - self.env.now % 24

        elif int(dow[0]) == current_dow:

            return h.str_to_dec(self.input_data['availability'][str(dow[0])][-2]) - self.env.now % 24
        else:

            return ((24 - self.env.now % 24) + (int(dow[1]) - 1) * 24 + h.str_to_dec(self.input_data['availability']
                                                                                     [str(dow[0])][0]))
