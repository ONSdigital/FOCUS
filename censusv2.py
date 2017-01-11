"""Module used to store the classes related to census..."""
import math
import datetime as dt
from collections import namedtuple
import helper as h
import datetime
from simpy.util import start_delayed
import sys


return_times = namedtuple('Returned', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])  # time return received
reminder_sent = namedtuple('Reminder_sent', ['rep', 'Time', 'digital',  'hh_type', 'type', 'hh_id'])
visit = namedtuple('Visit', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'time', 'hh_id', 'co_id'])
visit_contact = namedtuple('Visit_contact', ['rep', 'district', 'LA', 'LSOA',  'digital', 'hh_type', 'time', 'hh_id'])
visit_out = namedtuple('Visit_out', ['rep', 'district', 'LA', 'LSOA', 'digital','hh_type', 'time', 'hh_id'])
visit_wasted = namedtuple('Visit_wasted', ['rep', 'district', 'LA', 'LSOA','digital', 'hh_type', 'time', 'hh_id'])
visit_success = namedtuple('Visit_success', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'time', 'hh_id'])
visit_failed = namedtuple('Visit_failed', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'time', 'hh_id'])
visit_convert = namedtuple('Visit_convert', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'time', 'hh_id'])
visit_paper = namedtuple('Visit_paper', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'time', 'hh_id'])
visit_unnecessary = namedtuple('Visit_unnecessary', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'time', 'hh_id'])
visit_assist = namedtuple('Visit_assist', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'time', 'hh_id'])
post_paper = namedtuple('Post_paper', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'time'])
sent_letter = namedtuple('Sent_letter', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'time', 'hh_id'])
warnings = namedtuple('Warnings', ['rep', 'warning', 'detail'])


# a helper process that creates an instance of a StartFU class and starts it working
def start_fu(env, district):

    StartFU(env, district)
    yield env.timeout(0)


def send_reminder(household, reminder_type):

    household.rep.output_data["Reminder_sent"].append(reminder_sent(household.rep.reps,
                                                                    household.env.now,
                                                                    household.digital,
                                                                    household.hh_type,
                                                                    reminder_type,
                                                                    household.hh_id))

    start_delayed(household.env, household.receive_reminder(reminder_type), 24)
    yield household.env.timeout(0)


def ret_rec(hh, rep):
    # print out every 100000 returns?
    if rep.total_returns % 100000 == 0:
        print(rep.total_returns)

    hh.returned = True
    rep.total_returns += 1

    # check size of output data - if over an amount, size or length write to file?
    rep.output_data['Returned'].append(return_times(rep.reps,
                                                    hh.district.name,
                                                    hh.input_data["LA"],
                                                    hh.input_data["LSOA"],
                                                    hh.digital,
                                                    hh.hh_type,
                                                    hh.hh_id,
                                                    rep.env.now))

    # checks size of output and writes to file if too large
    if (h.dict_size(rep.output_data)) > 1000000:
        h.write_output(rep.output_data, rep.output_path, rep.run)

    yield rep.env.timeout(0)
    # so returned and we know it! remove from simulation??


class Adviser(object):
    """Call centre adviser - multitasking"""

    def __init__(self, rep, id_num, input_data):

        self.rep = rep
        self.id_num = id_num
        self.input_data = input_data

        self.start_date = datetime.datetime.strptime(self.input_data['start_date'], '%Y, %m, %d, %H, %M, %S')
        self.end_date = datetime.datetime.strptime(self.input_data['end_date'], '%Y, %m, %d, %H, %M, %S')

        # start the processes to add and remove from the store...

        self.set_availability()


    def set_availability(self):

        for single_date in h.date_range(self.start_date, self.end_date):
            for time_slot in self.input_data['availability'][str(single_date.weekday())]:

                in_time = h.make_time(time_slot[0][0], time_slot[0][1], time_slot[0][2])
                out_time = h.make_time(time_slot[1][0], time_slot[1][1], time_slot[1][2])
                days_in_hours = int((single_date - self.start_date).days)*24

                start_delayed(self.rep.env, self.add_to_store(), h.make_time_decimal(in_time) + days_in_hours)
                start_delayed(self.rep.env, self.remove_from_store(), h.make_time_decimal(out_time) + days_in_hours)

    # method to transfer the adviser to the store ready to be claimed
    def add_to_store(self):

        self.rep.ad_avail.remove(self)
        self.rep.adviser_store.put(self)
        yield self.rep.env.timeout(0)

    def remove_from_store(self):

        current_ad = yield self.rep.adviser_store.get(lambda item: item.id_num == self.id_num)
        self.rep.ad_avail.append(current_ad)
        yield self.rep.env.timeout(0)


class StartFU(object):
    """represents the RMT creating the visit lists for the assigned districts and CO"""
    def __init__(self, env, district):

        self.env = env
        self.district = district
        self.households = self.district.households
        self.update = self.district.input_data['RMT_update']

        self.visit_list = []

        self.env.process(self.create_visit_lists())

    def create_visit_lists(self):

        if h.returns_to_date(self.district) < self.district.input_data["trigger"]:

            self.visit_list = []

            # determine who needs to be followed up
            for household in self.households:
                if (household.returned is False and household.visits < household.input_data['max_visits'] and
                        household.input_data['FU_start_time'] <= self.env.now):

                    self.visit_list.append(household)

            # order by priority
            self.visit_list.sort(key=lambda hh: hh.priority, reverse=False)

            num_of_co = len(self.district.district_co)

            # split the hh up between the CO with the higher pri hh the top of each list
            for co in self.district.district_co:
                action_plan = self.visit_list[0::num_of_co]
                self.visit_list = [hh for hh in self.visit_list if hh not in action_plan]

                num_of_co -= 1

                co.action_plan = action_plan
            yield self.env.timeout(self.update)
            self.env.process(self.create_visit_lists())


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
        self.has_paper = h.str2bool(self.input_data['has_paper'])
        self.start_sim_time = self.co_start_time()  # the sim time the co starts work
        self.end_sim_time = self.co_end_time()  # the sim time the co ends work

        start_delayed(self.env, self.co_working_test(), self.start_sim_time)

    def co_start_time(self):
        # returns the simpy time as to when the co starts work

        try:
            # check start date has valid availability schedule
            start_date = dt.date(*map(int, self.input_data['start_date'].split(',')))
            # convert start date to simpy time
            start_date_simpy = (start_date - self.rep.start_date).total_seconds() / 3600

            # convert start time of that day to simpy time
            start_time = self.input_data['availability'][str(start_date.weekday())][0]
            start_time_simpy = h.make_time_decimal(dt.time(*map(int, start_time.split(':'))))

            return start_date_simpy + start_time_simpy

        except IndexError as e:
            print(e, "District ", self.district.name, " has no availability schedule set for CO on start day of ",
                  start_date)
            sys.exit()

    def co_end_time(self):
        # returns the simpy time as to when the co stops work

        # convert end date to simpy time
        end_date = dt.date(*map(int, self.input_data['end_date'].split(',')))
        end_date_simpy = (end_date - self.rep.start_date).total_seconds() / 3600

        # convert end time of that day to simpy time
        end_time = self.input_data['availability'][str(end_date.weekday())][-1]
        end_time_simpy = h.make_time_decimal(dt.time(*map(int, end_time.split(':'))))

        return end_date_simpy + end_time_simpy

    def co_working_test(self):

        if h.returns_to_date(self.district) >= self.district.input_data["trigger"]:
            # trigger reached stop collection and remove CO from list of CO's but at end of current day only?
            self.district.district_co.remove(self)

        elif self.working() and len(self.action_plan) > 0:

            # yield self.env.process(self.fu_household_test())
            household = self.action_plan.pop(0)
            yield self.env.process(self.fu_visit_contact(household))
            self.env.process(self.co_working_test())

        else:
            yield self.env.timeout(self.next_available())
            self.env.process(self.co_working_test())

    def fu_household_test(self):
        # this logic will look to see if it is the optimal time to visit a hh that has asked for a visit
        # if not it will add it back to the list and move to the next household
        # if yes it will make the call
        # not used for now

        # take household to visit out but check if a visit has been arranged
        household = self.action_plan.pop(0)
        d = household.input_data['at_home'][str(h.current_day(household))]

        # check arrange time vs current time...and day...
        if household.arranged_visit and h.return_time_key(d, self.env.now) != max(d, key=d.get) and not self.catch_all_arranged():

            self.action_plan.insert(self.return_index(), household)
        else:
            yield self.env.process(self.fu_visit_contact(household))

    def catch_all_arranged(self):

        return all(hh.arranged_visit for hh in self.action_plan)

    def fu_visit_contact(self, household):

        self.rep.output_data['Visit'].append(visit(self.rep.reps,
                                                   self.district.name,
                                                   household.input_data["LA"],
                                                   household.input_data["LSOA"],
                                                   household.digital,
                                                   household.hh_type,
                                                   self.rep.env.now,
                                                   household.hh_id,
                                                   self.co_id))

        if household.resp_planned:
            self.rep.output_data['Visit_unnecessary'].append(visit_unnecessary(self.rep.reps,
                                                                               self.district.name,
                                                                               household.input_data["LA"],
                                                                               household.input_data["LSOA"],
                                                                               household.digital,
                                                                               household.hh_type,
                                                                               self.rep.env.now,
                                                                               household.hh_id))

        household.visits += 1
        household.priority += 1  # automatically lower the priority of this hh after a visit
        contact_test = self.rnd.uniform(0, 100)
        #print(contact_test)
        contact_dict = household.input_data['at_home'][str(h.current_day(self))]

        if contact_test <= contact_dict[h.return_time_key(contact_dict, self.env.now)]:
            #in
            self.rep.output_data['Visit_contact'].append(visit_contact(self.rep.reps,
                                                                       self.district.name,
                                                                       household.input_data["LA"],
                                                                       household.input_data["LSOA"],
                                                                       household.digital,
                                                                       household.hh_type,
                                                                       self.env.now,
                                                                       household.hh_id))

            yield self.rep.env.process(self.fu_visit_assist(household))

        elif (contact_test > contact_dict[h.return_time_key(contact_dict, self.env.now)] and
              household.visits == household.input_data['max_visits'] and
              h.str2bool(household.input_data['paper_after_max_visits'])):

            household.paper_allowed = True
            schedule_paper_drop(self, household, self.has_paper)

            visit_time = self.input_data["visit_times"]["out_paper"]
            yield self.rep.env.timeout((visit_time/60) + self.district.travel_dist/self.input_data["travel_speed"])

        else:
            # out - add drop of of a note
            self.rep.output_data['Visit_out'].append(visit_out(self.rep.reps,
                                                               self.district.name,
                                                               household.input_data["LA"],
                                                               household.input_data["LSOA"],
                                                               household.digital,
                                                               household.hh_type,
                                                               self.env.now,
                                                               household.hh_id))

            self.env.process(send_reminder(household, 'postcard'))

            visit_time = self.input_data["visit_times"]["out"]
            yield self.rep.env.timeout((visit_time / 60) + self.district.travel_dist / self.input_data["travel_speed"])

    def fu_visit_assist(self, household):

        da_test = self.rnd.uniform(0, 100)
        da_effectiveness = self.input_data['da_effectiveness'][household.hh_type]

        yield self.env.timeout(self.input_data['visit_times']['query']/60)

        # if digital or have already responded skip straight to the outcome of the visit
        if household.digital or household.responded:
            yield self.rep.env.process(self.fu_visit_outcome(household))

        # if not digital try to persuade them to complete online.
        elif not household.digital and da_test <= da_effectiveness:
            yield self.env.timeout(self.input_data['visit_times']['convert']/60)

            self.rep.output_data['Visit_convert'].append(visit_convert(self.rep.reps,
                                                                       household.district.name,
                                                                       household.input_data["LA"],
                                                                       household.input_data["LSOA"],
                                                                       household.digital,
                                                                       household.hh_type,
                                                                       self.rep.env.now,
                                                                       household.hh_id))

            household.digital = True
            yield self.rep.env.process(self.fu_visit_outcome(household))

        # if not digital, do not convince to complete online, and trigger and max visits not reached give paper if on.
        elif (not household.digital and da_test > da_effectiveness and
              h.returns_to_date(self.district) < self.district.input_data['paper_trigger'] and
              household.visits == household.input_data['max_visits'] and
              h.str2bool(household.input_data['paper_after_max_visits'])):

            household.paper_allowed = True
            schedule_paper_drop(self, household, self.has_paper)

            visit_time = self.input_data["visit_times"]["paper"]
            yield self.rep.env.timeout((visit_time/60) + self.district.travel_dist/self.input_data["travel_speed"])

        else:
            # or suggest other forms of assistance to be decided...
            # no more - another visit will be scheduled...
            self.rep.output_data['Visit_assist'].append(visit_assist(self.rep.reps,
                                                                     self.district.name,
                                                                     household.input_data["LA"],
                                                                     household.input_data["LSOA"],
                                                                     household.digital,
                                                                     household.hh_type,
                                                                     self.env.now,
                                                                     household.hh_id))
            visit_time = self.input_data["visit_times"]["paper"]
            yield self.rep.env.timeout((visit_time / 60) + self.district.travel_dist / self.input_data["travel_speed"])

    def fu_visit_outcome(self, household):

        outcome_test = self.rnd.uniform(0, 100)
        #print(outcome_test)
        conversion_dict = household.input_data['conversion_rate'][str(h.current_day(self))]

        if household.responded is True:
            self.rep.output_data['Visit_wasted'].append(visit_wasted(self.rep.reps,
                                                                     household.district.name,
                                                                     household.input_data["LA"],
                                                                     household.input_data["LSOA"],
                                                                     household.digital,
                                                                     household.hh_type,
                                                                     self.env.now,
                                                                     household.hh_id))

            visit_time = self.input_data["visit_times"]["wasted"]
            yield self.rep.env.timeout((visit_time/60) + self.district.travel_dist/self.input_data["travel_speed"])

        # hh have not responded yet and respond there and then either by paper or digital.
        elif (not household.responded and
                outcome_test <= conversion_dict[h.return_time_key(conversion_dict, self.env.now)]):

            self.rep.output_data['Visit_success'].append(visit_success(self.rep.reps,
                                                                       household.district.name,
                                                                       household.input_data["LA"],
                                                                       household.input_data["LSOA"],
                                                                       household.digital,
                                                                       household.hh_type,
                                                                       self.env.now,
                                                                       household.hh_id))
            household.resp_planned = True
            visit_time = self.input_data["visit_times"]["success"]
            yield self.rep.env.timeout((visit_time/60) + self.district.travel_dist/self.input_data["travel_speed"])
            self.rep.env.process(household.respond(household.delay))

        # hh have not responded but do not respond as a result of the visit.
        # need extra here fro when you fail but not at max visits...
        elif (not household.responded and
              outcome_test > conversion_dict[h.return_time_key(conversion_dict, self.env.now)] and
              h.returns_to_date(self.district) < self.district.input_data['paper_trigger'] and
              household.visits == household.input_data['max_visits'] and
              h.str2bool(household.input_data['paper_after_max_visits'])):

            self.rep.output_data['Visit_failed'].append(visit_failed(self.rep.reps,
                                                                     household.district.name,
                                                                     household.input_data["LA"],
                                                                     household.input_data["LSOA"],
                                                                     household.digital,
                                                                     household.hh_type,
                                                                     self.env.now,
                                                                     household.hh_id))
            # leave paper in hope they respond?
            household.paper_allowed = True
            schedule_paper_drop(self, household, self.has_paper)

            visit_time = self.input_data["visit_times"]["failed"]
            yield self.rep.env.timeout((visit_time / 60) + self.district.travel_dist/self.input_data["travel_speed"])

    def working(self):
        """returns true or false depending on whether or not a CO is available at current date and time"""

        day_of_week = self.rep.start_day + math.floor(self.env.now / 24) % 7

        if self.start_sim_time <= self.env.now < self.end_sim_time and self.input_data['availability'][str(day_of_week)]:

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
                #print(return_index)

        return max(return_index, 0)

    def return_dow(self, days_gone_from_start, original_tod, count=0):
        # get next relavent dow based on current sim time
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

        current_dow = (self.rep.start_date + dt.timedelta(days=math.floor(self.env.now / 24))).weekday()
        dow = self.return_dow(math.floor(self.env.now / 24), self.env.now % 24)
        # if action plan is zero,so o houses to visits always delay to the start of the next valid day
        if not self.action_plan:
            # this actually just jumps it to the end of the day
            return 24 - self.env.now % 24

        elif int(dow[0]) == current_dow:

            return h.str_to_dec(self.input_data['availability'][str(dow[0])][-2]) - self.env.now % 24
        else:

            return ((24 - self.env.now % 24) + (int(dow[1]) - 1) * 24 + h.str_to_dec(self.input_data['availability']
                                                                                     [str(dow[0])][0]))


class LetterPhase(object):

    def __init__(self, env, rep, district, input_data):
        self.env = env
        self.rep = rep
        self.district = district
        self.input_data = input_data
        # add process to decide who to send letters too...but with a delay

        start_delayed(self.env, self.fu_letter(), self.input_data["send_time"])

    def fu_letter(self):

        # send a letter if conditions met
        #print("targeted ", self.input_data["targeted"])
        for household in self.district.households:

            #print("all hh ", "id ",  household.hh_id, " type ", household.hh_type, " responded " , household.responded)
            if (h.str2bool(self.input_data["targeted"]) and household.hh_type in self.input_data["targets"] and
                not household.responded) or \
                    (not h.str2bool(self.input_data["targeted"]) and household.hh_type in self.input_data["targets"]):

                #print("letter", household.hh_id)

                # send a letter
                self.env.process(self.co_send_letter(household,
                                                     self.input_data["effect"],
                                                     self.input_data["postal_delay"],
                                                     self.input_data["pq"]))

        yield self.env.timeout(0)

    def co_send_letter(self, household, effect, delay, pq):

        self.rep.output_data['Sent_letter'].append(sent_letter(self.rep.reps,
                                                               household.district.name,
                                                               household.input_data["LA"],
                                                               household.input_data["LSOA"],
                                                               household.digital,
                                                               household.hh_type,
                                                               self.rep.env.now,
                                                               household.hh_id))

        #print("letter sent to hh ", household.hh_id)

        yield self.env.timeout(delay)
        self.env.process(household.receive_letter(effect, pq))


def schedule_paper_drop(obj, household, has_paper=False):

    if has_paper:

        obj.rep.output_data['Visit_paper'].append(visit_paper(obj.rep.reps,
                                                              household.district.name,
                                                              household.input_data["LA"],
                                                              household.input_data["LSOA"],
                                                              household.digital,
                                                              household.hh_type,
                                                              obj.rep.env.now,
                                                              household.hh_id))
    else:

        household.output_data['Post_paper'].append(post_paper(household.rep.reps,
                                                              household.district.name,
                                                              household.input_data["LA"],
                                                              household.input_data["LSOA"],
                                                              household.digital,
                                                              household.hh_type,
                                                              household.env.now))

    if has_paper:
        obj.env.process(send_reminder(household, 'pq'))
    else:
        start_delayed(obj.env, send_reminder(household, 'pq'), h.next_day(obj.env.now))
