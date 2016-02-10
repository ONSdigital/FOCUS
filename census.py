"""Class used to represent area coordinators. 1 per area so FU can start at different times
for different areas if need be. Not needed to run multiple simulations if districts do not
interact. But useful if districts are needed to interact"""
from collections import namedtuple
import datetime
import math
import csv


FU_start = namedtuple('FU_start', ['Time'])
letter_sent = namedtuple('letter_sent', ['run', 'reps', 'Time', 'Household', 'Type', 'hh_type'])
visit = namedtuple('Visit', ['run', 'reps', 'Time', 'Household', 'Type'])
visit_out = namedtuple('Visit_out', ['run', 'reps', 'Time', 'Household', 'Type'])
visit_contact = namedtuple('Visit_contact', ['run', 'reps', 'Time', 'Household', 'Type'])
visit_wasted = namedtuple('Visit_wasted', ['run', 'reps', 'Time', 'Household', 'Type'])
visit_unnecessary = namedtuple('Visit_unnecessary', ['run', 'reps', 'Time', 'Household', 'Type'])
visit_success = namedtuple('Visit_success', ['run', 'reps', 'Time', 'Household', 'Type'])
enu_util = namedtuple('Enu_util', ['run', 'reps', 'Time', 'Count'])  # enumerator usage over time
enu_travel = namedtuple('Enu_travel', ['run', 'reps', 'Enu_id', 'Time', 'Distance', 'Travel_time'])
visit_assist = namedtuple('Visit_assist', ['run', 'reps', 'Time', 'Household', 'Type'])
visit_paper = namedtuple('Visit_paper', ['run', 'reps', 'Time', 'Household', 'Type'])


def print_resp(run):
    """returns the responses received to date for the current run and other counters"""

    htc_resp = {'htc1': 0, 'htc2': 0, 'htc3': 0, 'htc4': 0, 'htc5': 0}

    visit_counter = 0
    call_counter = 0
    total_dig_resp = 0
    total_pap_resp = 0
    visit_unnecessary_counter = 0
    visit_wasted_counter = 0
    visit_out_counter = 0
    visit_success_counter = 0
    visit_contact_counter = 0
    visit_assist_counter = 0
    visit_paper_counter = 0
    letter_sent_counter = 0
    letter_wasted_counter = 0
    letter_received_counter = 0

    for item in run.output_data:
        if type(item).__name__ == 'Responded' and item[0] == run.run and item[1] == run.reps:
            htc_resp[item[4]] += 1
            if item[7] == 'digital':
                total_dig_resp += 1
            elif item[7] == 'paper':
                total_pap_resp += 1
        elif type(item).__name__ == 'Visit' and item[0] == run.run and item[1] == run.reps:
            visit_counter += 1
        elif type(item).__name__ == 'Phone_call' and item[0] == run.run and item[1] == run.reps:
            call_counter += 1
        elif type(item).__name__ == 'Visit_unnecessary' and item[0] == run.run and item[1] == run.reps:
            visit_unnecessary_counter += 1
        elif type(item).__name__ == 'Visit_wasted' and item[0] == run.run and item[1] == run.reps:
            visit_wasted_counter += 1
        elif type(item).__name__ == 'Visit_out' and item[0] == run.run and item[1] == run.reps:
            visit_out_counter += 1
        elif type(item).__name__ == 'Visit_success' and item[0] == run.run and item[1] == run.reps:
            visit_success_counter += 1
        elif type(item).__name__ == 'Visit_contact' and item[0] == run.run and item[1] == run.reps:
            visit_contact_counter += 1
        elif type(item).__name__ == 'Visit_assist' and item[0] == run.run and item[1] == run.reps:
            visit_assist_counter += 1
        elif type(item).__name__ == 'Visit_paper' and item[0] == run.run and item[1] == run.reps:
            visit_paper_counter += 1
        elif type(item).__name__ == 'letter_sent' and item[0] == run.run and item[1] == run.reps:
            letter_sent_counter += 1
        elif type(item).__name__ == 'letter_wasted' and item[0] == run.run and item[1] == run.reps:
            letter_wasted_counter += 1
        elif type(item).__name__ == 'letter_received' and item[0] == run.run and item[1] == run.reps:
            letter_received_counter += 1

    print(run.run, run.reps)
    for key, value in sorted(htc_resp.items()):  # sort the dictionary for output purposes
        try:
            data = [run.run,
                    (run.input_data['households'][key]['allow_paper']),
                    (run.input_data['households'][key]['FU_on']),
                    (run.input_data['letters_on']),
                    run.reps,
                    (run.input_data['households'][key]['default_resp']),
                    (run.input_data['households'][key]['paper_prop']),
                    (run.input_data['households'][key]['FU_start_time']),
                    (run.input_data['households'][key]['dig_assist_eff']),
                    (run.input_data['households'][key]['dig_assist_flex']),
                    (run.input_data['district_area']),
                    (run.input_data['households'][key]['max_visits']),
                    run.total_enu_instances,
                    (run.input_data['households'][key]['conversion_rate']),
                    96,  # time letters sent
                    10,  # change this to reflect letter effect???
                    (value / run.hh_count[key]),
                    total_dig_resp,
                    total_pap_resp,
                    visit_counter,
                    visit_unnecessary_counter,
                    visit_wasted_counter,
                    visit_out_counter,
                    visit_success_counter,
                    visit_contact_counter,
                    visit_assist_counter,
                    visit_paper_counter,
                    call_counter,
                    letter_sent_counter,
                    letter_wasted_counter,
                    letter_received_counter,
                    run.seed]

            # add code to print to a file instead/as well
            with open('outputs/RAW_output4.csv', 'a', newline='') as csv_file:
                output_file = csv.writer(csv_file, delimiter=',')
                output_file.writerow(data)
                output_file.close()
        except:
            # skip any that cause errors -at some point add in what caused them!
            pass

    yield run.env.timeout(0)


# a helper process that creates an instance of a coordinator class
def fu_startup(run, env, district, data):

    Coordinator(run, env, district, 1)
    data.append(FU_start(env.now))
    yield env.timeout(0)


# a helper process that creates an instance of a letter phase class
def letter_startup(run, env, district, output_data, sim_days, targeted, letter_type, targets):

    LetterPhase(run, env, district, output_data, sim_days, targeted, letter_type, targets)
    yield env.timeout(0)


def pop_advisers(run, input_dict, storage_list, store):

    temp_date = str((run.sim_start + datetime.timedelta(hours=run.env.now)).date())
    day_cap = int(input_dict[temp_date]['capacity'])
    temp_diff = day_cap - len(store.items)

    if temp_diff < 0:
        # remove
        for i in range(abs(temp_diff)):
            item = yield store.get()
            # and place in a list to hold it
            storage_list.append(item)
            # and flag as not avail that day
            item.avail = False
    elif temp_diff > 0:
        # add
        for i in range(temp_diff):
            # from list of existing advisers
            temp_adviser = storage_list.pop(0)
            temp_adviser.avail = True
            store.put(temp_adviser)

    yield run.env.timeout(0)


def resp_rec(env, hh, run):
    hh.resp_rec = True
    run.total_responses += 1

    yield env.timeout(0)


class Coordinator(object):
    """represents the coordinator for the assigned district"""
    def __init__(self, run, env, district, repeat_update):
        self.env = env
        self.run = run
        self.district = district
        self.check = env.process(self.arrange_visits())  # will start the action process
        self.repeat_update = repeat_update
        self.update_count = 0
        self.current_hh_sep = 0

    def arrange_visits(self):
        while True:

            self.run.visit_list = []

            for household in self.run.district:
                if household.resp_rec is False and household.fu_start <= self.env.now and household.FU_on is True and\
                                household.visits < household.max_visits:
                    self.run.visit_list.append(household)

            """sort what is left by pri - lower numbers first"""
            self.run.visit_list.sort(key=lambda hh: hh.pri, reverse=False)

            # re-calculates travel time based on households left to visit in area
            # so based on what is left in district
            try:
                self.current_hh_sep = self.run.initial_hh_sep / (math.sqrt(1-(self.run.total_responses /
                                                                              len(self.run.district))))
            except:
                self.current_hh_sep = 0
                self.run.travel_time = 0

            yield self.env.timeout(self.repeat_update*24)  # sorted at 00:00 each day. What time would this happen?


class Enumerator(object):
    """represents an individual enumerator. Each instance can be different"""
    # need to add tracking to here....
    def __init__(self, run, id_num, time_start, time_end, from_date, input_data, do_visits):
        self.env = run.env
        self.run = run
        self.id_num = id_num
        #self.district = run.district
        self.sim_start = run.sim_start
        self.time_start = time_start
        self.time_end = time_end
        self.from_date = from_date
        self.input_data = input_data
        self.do_visits = do_visits
        self.distance_travelled = 0
        self.travel_time = 0
        self.visits = 0

        if self.do_visits is True:
            self.run.env.process(self.fu_visit_contact())  # starts the process which runs the visits

    def fu_visit_contact(self):
        """does the enumerator make contact with the hh"""

        while True:
            if self.working_test() is True and len(self.run.visit_list) != 0:

                self.run.output_data.append(enu_util(self.run.run, self.run.reps, self.env.now, len(self.run.enu_working)))
                # transfer to working list
                self.run.enu_working.append(self)
                self.run.enu_avail.remove(self)
                self.run.output_data.append(enu_util(self.run.run, self.run.reps, self.env.now, len(self.run.enu_working)))

                # visit
                current_hh = self.run.visit_list.pop(0)  # get next household to visit - and remove from master list

                """if hand helds available check if responded and if so do not visit -
                remove from list and move to next hh"""

                current_hh.visits += 1  # increase visits to that hh by 1
                self.visits += 1

                try:
                    self.distance_travelled += self.run.initial_hh_sep / (math.sqrt(1-(self.run.total_responses /
                                                                                       len(self.run.district))))
                except:
                    self.distance_travelled = 0

                self.travel_time += self.run.travel_time
                self.run.output_data.append(enu_travel(self.run.run, self.run.reps, self.id_num, self.env.now,
                                                       self.distance_travelled, self.travel_time))

                self.run.output_data.append(visit(self.run.run, self.run.reps, self.env.now, current_hh.id_num,
                                                  current_hh.hh_type))

                # visited but will reply have done so if not visited
                if current_hh.resp_planned is True and current_hh.resp_sent is False:
                    # add record of a visit that was not required but otherwise carry on
                    self.run.output_data.append(visit_unnecessary(self.run.run, self.run.reps, self.env.now, current_hh.id_num, current_hh.hh_type))

                hh_in = False  # contact rate

                if self.run.rnd.uniform(0, 100) <= self.input_data[current_hh.hh_type]['contact_rate']:
                    hh_in = True

                if hh_in is True and (current_hh.resp_type == 'paper' and current_hh.paper_allowed is False):
                    yield self.env.process(self.fu_visit_assist(current_hh))
                elif hh_in is True and (current_hh.resp_type == 'digital' and current_hh.paper_allowed is False):
                    yield self.env.process(self.fu_visit_outcome(current_hh))
                elif hh_in is True and current_hh.paper_allowed is True:
                    yield self.env.process(self.fu_visit_outcome(current_hh))
                else:
                    # not in
                    self.run.output_data.append(visit_out(self.run.run, self.run.reps, self.env.now, current_hh.id_num,
                                                          current_hh.hh_type))
                    yield self.env.timeout((3 / 60) + self.run.travel_time)  # travel time spent
                    # will need to add back to the overall list with an update pri
                    current_hh.pri += 1
                    # then put back in the list at the end if below max_visit number
                    if current_hh.visits < current_hh.max_visits:
                        self.run.visit_list.append(current_hh)
                    else:
                        '''add event to give paper if max visits received - but what will the HH then do?'''
                        self.run.output_data.append(visit_paper(self.run.run, self.run.reps, self.env.now, current_hh.id_num,
                                                          current_hh.hh_type))
                        current_hh.paper_allowed = True
                        current_hh.resp_level = current_hh.decision_level(self.input_data[current_hh.hh_type], "resp")
                        current_hh.help_level = current_hh.resp_level + current_hh.decision_level(self.input_data[current_hh.hh_type], "help")
                        current_hh.refuse_level = current_hh.help_level + current_hh.decision_level(self.input_data[current_hh.hh_type], "refuse")

                        self.env.process(current_hh.action())

                    # transfer enumerator back to available list
                    self.run.output_data.append(enu_util(self.run.run, self.run.reps, self.env.now, len(self.run.enu_working)))
                    self.run.enu_working.remove(self)
                    self.run.enu_avail.append(self)
                    self.run.output_data.append(enu_util(self.run.run, self.run.reps, self.env.now, len(self.run.enu_working)))

            elif self.working_test() is False and len(self.run.visit_list) != 0:
                    # not yet at work, time out until the next time they are due to start work
                yield self.env.timeout(self.hold_until())
            else:
                yield self.env.timeout(24)  # should be start of next day instead if this does not do that already...
                # yield self.env.timeout((self.run.sim_days * 24) - self.env.now)

    def fu_visit_assist(self, current_hh):
        """once contact is made determine if digital assistance is required"""

        if current_hh.resp_type == 'paper':
            dig_assist_test = self.run.rnd.uniform(0, 100)
            if dig_assist_test < current_hh.input_data['dig_assist_eff']:
                # persuades hh to switch to digital from paper
                current_hh.resp_type = 'digital'
                current_hh.delay = 0
                """how long would it take to change their minds?"""
                yield self.env.timeout(0.2 + self.run.travel_time)
                yield self.env.process(self.fu_visit_outcome(current_hh))

            elif current_hh.input_data['dig_assist_eff'] <= dig_assist_test <\
                    (current_hh.input_data['dig_assist_eff'] + current_hh.input_data['dig_assist_flex'])\
                    or current_hh.visits == current_hh.max_visits:
                # allows hh to use paper to respond
                """how long to get to the point of letting them have paper?"""
                yield self.env.timeout(0.2 + self.run.travel_time)
                current_hh.paper_allowed = True
                current_hh.resp_level = current_hh.decision_level(self.input_data[current_hh.hh_type], "resp")
                current_hh.help_level = current_hh.resp_level + current_hh.decision_level(self.input_data[current_hh.hh_type], "help")
                current_hh.refuse_level = current_hh.help_level + current_hh.decision_level(self.input_data[current_hh.hh_type], "refuse")
                yield self.env.process(self.fu_visit_outcome(current_hh))

            else:
                # suggests another form of digital assist - another visit in this case
                """how long would suggesting different forms of digital assist take?"""
                yield self.env.timeout(0.2 + self.run.travel_time)
                self.run.output_data.append(visit_assist(self.run.run, self.run.reps, self.env.now, current_hh.id_num, current_hh.hh_type))
                current_hh.pri -= 5  # they have asked for help so raise the priority of the hh
                # so put hh back in the list to visit if max visits not reached
                if current_hh.visits < current_hh.max_visits:
                    self.run.visit_list.append(current_hh)
                # transfer enumerator back to available list
                self.run.output_data.append(enu_util(self.run.run, self.run.reps, self.env.now, len(self.run.enu_working)))
                self.run.enu_working.remove(self)
                self.run.enu_avail.append(self)
                self.run.output_data.append(enu_util(self.run.run, self.run.reps, self.env.now, len(self.run.enu_working)))

        else:
            yield self.env.process(self.fu_visit_outcome(current_hh))

    def fu_visit_outcome(self, current_hh):

        hh_responds = False

        if self.run.rnd.uniform(0, 100) <= self.input_data[current_hh.hh_type]['conversion_rate']:
            hh_responds = True

        # in but already replied
        if current_hh.resp_sent is True:
            self.run.output_data.append(visit_wasted(self.run.run, self.run.reps, self.env.now, current_hh.id_num, current_hh.hh_type))
            yield self.env.timeout((5 / 60) + self.run.travel_time)

        # in and respond - there and then
        if current_hh.resp_sent is False and hh_responds is True:
            self.run.output_data.append(visit_success(self.run.run, self.run.reps, self.env.now, current_hh.id_num,
                                                      current_hh.hh_type))
            current_hh.resp_planned = True
            yield self.env.timeout((12 / 60) + self.run.travel_time)
            self.env.process(current_hh.respond(True, current_hh.delay))

        # in but no immediate response
        if current_hh.resp_sent is False and hh_responds is False:
            self.run.output_data.append(visit_contact(self.run.run, self.run.reps, self.env.now, current_hh.id_num,
                                                      current_hh.hh_type))
            yield self.env.timeout((5 / 60) + self.run.travel_time)
            """After a visit where they don't respond what do hh then do"""
            current_hh.resp_level = current_hh.decision_level(self.input_data[current_hh.hh_type], "resp")
            current_hh.help_level = current_hh.resp_level + current_hh.decision_level(self.input_data[current_hh.hh_type], "help")
            current_hh.refuse_level = current_hh.help_level + current_hh.decision_level(self.input_data[current_hh.hh_type], "refuse")
            current_hh.pri += 2
            # then put back in the list to visit at the end with new pri but only if below max_visit number
            if current_hh.visits < current_hh.max_visits:
                self.run.visit_list.append(current_hh)

            '''comment out the below? As the hh should really just do whatever they were going to?'''
            self.env.process(current_hh.action())

        # transfer enumerator back to available list
        self.run.output_data.append(enu_util(self.run.run, self.run.reps, self.env.now, len(self.run.enu_working)))
        self.run.enu_working.remove(self)
        self.run.enu_avail.append(self)
        self.run.output_data.append(enu_util(self.run.run, self.run.reps, self.env.now, len(self.run.enu_working)))

    def hold_until(self):

        if self.current_day() < self.from_date:
            date_1 = datetime.datetime.strptime(self.current_day(), '%Y-%m-%d').date()
            date_2 = datetime.datetime.strptime(self.from_date, '%Y-%m-%d').date()
            diff = (date_2 - date_1).days * 24
            return diff + self.time_start
        elif self.current_day() >= self.from_date:
            if self.env.now % 24 < self.time_start:
                return self.time_start - self.env.now % 24
            elif self.env.now % 24 >= self.time_end:
                return self.time_start + (24 - self.env.now % 24)

    def current_day(self):
        return str((self.sim_start + datetime.timedelta(hours=self.env.now)).date())

    def working_test(self):
        """returns true or false to depending on whether or not an enumerator is available"""

        if self.current_day() >= self.from_date and (self.time_start <= self.env.now % 24 < self.time_end):
            return True
        else:
            return False


class LetterPhase(object):
    def __init__(self, run, env, district, output_data, sim_days, targeted, letter_type, targets):
        self.run = run
        self.env = env
        self.district = district
        self.letter_delay = 72
        self.output_data = output_data
        self.sim_days = sim_days
        self.targets = targets.split(',')
        env.process(self.fu_letter(targeted, letter_type))

    def fu_letter(self, targeted, letter_type):
        # need to add an option to send bulk letters rather than targeted
        # targeted is print on demand which costs more or takes longer??
        for household in self.district:
            if targeted is True and household.resp_rec is False and household.hh_type in self.targets:
                # send a letter
                self.env.process(self.co_send_letter(household, letter_type))
            elif targeted is True and household.resp_rec is False and len(self.targets) == 0:
                self.env.process(self.co_send_letter(household, letter_type))
            elif targeted is False:
                self.env.process(self.co_send_letter(household, letter_type))
            # add an option to send to all ina group whether replied or not
                """what is the overhead of sending only to non responders?"""

        # then pause until the end
        yield self.env.timeout((self.sim_days * 24) - self.env.now)

    def co_send_letter(self, household, letter_type):
        self.output_data.append(letter_sent(self.run.run, self.run.reps, self.env.now, household.id_num, letter_type, household.hh_type))
        # send a letter which will take an amount of time to be received (which could vary)
        yield self.env.timeout(self.letter_delay)
        # then the hh needs to do something...
        self.env.process(household.rec_letter(letter_type))


class Adviser(object):
    """Call centre adviser - multitasking"""

    def __init__(self, run, id_num, start_time, end_time, start_date, end_date, ad_type, do_fu_calls):

        self.env = run.env
        self.run = run
        self.id_num = id_num
        self.do_fu_calls = do_fu_calls
        self.start_time = start_time
        self.end_time = end_time
        self.start_date = datetime.datetime.strptime(start_date, '%Y, %m, %d').date()
        self.end_date = datetime.datetime.strptime(end_date, '%Y, %m, %d').date()
        self.ad_type = ad_type

        self.avail = False
        self.time_answered = 0
        self.length_of_call = 0
        self.current_hh = 0

        # add a process that adds the adviser to the store at the passed time
        # and removes later and so on until the end of the sim
        # while out of the store place in a storage list/store?
        # test how quick this turns out to be???
        temp_switch = 1
        if temp_switch == 1:
            # do the new stuff
            self.run.env.process(self.set_availability())  # starts the process which runs the visits

        if self.do_fu_calls is True:
            # add a switch to turn this on or off
            run.env.process(self.fu_call())  # starts the process which runs the vi

    def set_availability(self):

        delay = self.start_time + 24*(self.start_date - self.run.sim_start.date()).days
        print(self.ad_type, delay)

        yield self.run.env.timeot(0)

        # so you know the numbers of days to delay
        #add the start time#
        #  create the events that add and remove the adviser from the adviser store
        #  loop through from start to end date and create event at start and end time
        #  calc start delayed time
        #  timedelta (date - startdate)





    def fu_call(self):

        while True:

            # if past start of FU
            if self.env.now >= self.run.fu_start:

                # get the working times for the day for the adviser
                temp_date = str((self.run.sim_start + datetime.timedelta(hours=self.env.now)).date())
                self.time_start = int(self.run.adviser_dict[temp_date]['time'].split('-')[0])
                self.time_end = int(self.run.adviser_dict[temp_date]['time'].split('-')[1])

                if self.time_start <= self.env.now % 24 < self.time_end and self.avail is True and len(self.district) != 0:
                    # working but check if taking a phone call
                    self.time_answered = self.env.now
                    if self in self.run.adviser_store.items and len(self.district) != 0:
                        # is not
                        # so remove from the store at this point
                        # and take a hh from the FU list
                        """ if live lists available check again here if hh has responded"""
                        current_hh = self.district.pop(0)
                        current_ad = yield self.run.adviser_store.get(lambda item: item.id_num == self.id_num)

                        """use visits contact rates below as we don't have any better at the moment.
                        But we do halve the success rate as people are less likely to respond in this manner"""
                        call_answered = False
                        if self.run.rnd.uniform(0, 100) <= int(self.run.visit_contact_rates_dict[current_hh.hh_type]):
                            call_answered = True
                        # equally likely to be in but half as effective at convincing to respond
                        call_response = False
                        if self.run.rnd.uniform(0, 100) <= int(self.run.visit_conversion_rates_dict[current_hh.hh_type])/2:
                            call_response = True

                        # add one to the number of calls received
                        current_hh.calls += 1

                        # check if answered and respond
                        if call_answered is True and call_response is True:
                            self.length_of_call = 0.2
                            yield self.env.timeout(self.length_of_call)
                            yield self.run.adviser_store.put(current_ad)
                            self.env.process(current_hh.respond(True, 0))
                        elif call_answered is True and call_response is False:
                            self.length_of_call = 0.2
                            yield self.env.timeout(self.length_of_call)
                            yield self.run.adviser_store.put(current_ad)
                            self.district.append(current_hh)
                            current_hh.pri += 2
                            current_hh.resp_level = 10
                            current_hh.help_level = 5
                            current_hh.refuse_level = 1
                            self.env.process(current_hh.action())
                        # elif answered and hh already responded add wasted call
                        elif call_answered is True and current_hh.resp_sent is True:
                            self.length_of_call = 0.05
                            yield self.env.timeout(self.length_of_call)
                            yield self.run.adviser_store.put(current_ad)
                            """add event to capture wasted call"""
                        else:
                            self.length_of_call = 0.05
                            yield self.env.timeout(self.length_of_call)
                            yield self.run.adviser_store.put(current_ad)
                            current_hh.pri += 1
                            self.district.append(current_hh)
                    else:
                        # is on a call so wait until that call is finished
                        yield self.env.timeout((self.time_answered + self.length_of_call) - self.env.now)
                else:
                    # not working yet - wait until they are - change until time next available
                    yield self.env.timeout(1)
            else:  # not yet at start of FU so wait until you are
                yield self.env.timeout(self.run.fu_start - self.env.now)

    def get_ad(self):

        current_ad = yield self.run.adviser_store.get()  # gets an adviser from the store...
        # but check it's the right one...if not put it back and get the next one
        print(current_ad.id_num)
        if current_ad.id_num != self.id_num:
            yield self.run.adviser_store.put(current_ad)
            self.env.process(self.get_ad())
        else:
            return current_ad


class AdviserChat(object):
    """dedicated web chat adviser"""

    def __init__(self, run, id_num):
        self.env = run.env
        self.run = run
        self.id_num = id_num
        self.avail = False


class AdviserIncomplete(object):
    """dedicated adviser that FU incomplete responses"""

    def __init__(self, run):
        self.env = run.env
        self.run = run


class alt_Enumerator(object):
    """represents an individual enumerator. Each instance can be different"""
    # need to add tracking to here....
    def __init__(self, run, id_num, start_time, end_time, start_date, end_date, enu_type, travel_speed, input_data,
                 visits_on):

        self.run = run
        self.id_num = id_num
        self.start_time = start_time
        self.end_time = end_time
        self.start_date = datetime.datetime.strptime(start_date, '%Y, %m, %d').date()
        self.end_date = datetime.datetime.strptime(end_date, '%Y, %m, %d').date()
        self.enu_type = enu_type
        self.travel_speed = travel_speed
        self.input_data = input_data
        self.visits_on = visits_on

        self.distance_travelled = 0
        self.travel_time = 0
        self.visits = 0

        if self.visits_on is True:
            run.env.process(self.fu_visit_contact())  # starts the process which runs the visits

    def fu_visit_contact(self):
        """does the enumerator make contact with the hh"""

        while True:

            if self.working_test() is True and len(self.run.visit_list) != 0:

                self.run.output_data.append(enu_util(self.run.run, self.run.reps, self.run.env.now, len(self.run.enu_working)))
                # transfer to working list
                self.run.enu_working.append(self)
                self.run.enu_avail.remove(self)
                self.run.output_data.append(enu_util(self.run.run, self.run.reps, self.run.env.now, len(self.run.enu_working)))

                # visit
                current_hh = self.run.visit_list.pop(0)  # get next household to visit - and remove from master list

                """if hand helds available check if responded and if so do not visit -
                remove from list and move to next hh"""

                current_hh.visits += 1  # increase visits to that hh by 1
                self.visits += 1

                try:
                    self.distance_travelled += self.run.initial_hh_sep / (math.sqrt(1-(self.run.total_responses /
                                                                                       len(self.run.district))))
                except:
                    self.distance_travelled = 0

                self.travel_time += self.run.travel_time
                self.run.output_data.append(enu_travel(self.run.run, self.run.reps, self.id_num, self.run.env.now,
                                                       self.distance_travelled, self.travel_time))

                self.run.output_data.append(visit(self.run.run, self.run.reps, self.run.env.now, current_hh.id_num,
                                                  current_hh.hh_type))

                # visited but will reply have done so if not visited
                if current_hh.resp_planned is True and current_hh.resp_sent is False:
                    # add record of a visit that was not required but otherwise carry on
                    self.run.output_data.append(visit_unnecessary(self.run.run, self.run.reps, self.run.env.now,
                                                                  current_hh.id_num, current_hh.hh_type))

                hh_in = False  # contact rate

                if self.run.rnd.uniform(0, 100) <= self.input_data[current_hh.hh_type]['contact_rate']:
                    hh_in = True

                if hh_in is True and (current_hh.resp_type == 'paper' and current_hh.paper_allowed is False):
                    yield self.run.env.process(self.fu_visit_assist(current_hh))
                elif hh_in is True and (current_hh.resp_type == 'digital' and current_hh.paper_allowed is False):
                    yield self.run.env.process(self.fu_visit_outcome(current_hh))
                elif hh_in is True and current_hh.paper_allowed is True:
                    yield self.run.env.process(self.fu_visit_outcome(current_hh))
                else:
                    # not in
                    self.run.output_data.append(visit_out(self.run.run, self.run.reps, self.run.env.now, current_hh.id_num,
                                                          current_hh.hh_type))
                    yield self.run.env.timeout((3 / 60) + self.run.travel_time)  # travel time spent
                    # will need to add back to the overall list with an update pri
                    current_hh.pri += 1
                    # then put back in the list at the end if below max_visit number
                    if current_hh.visits < current_hh.max_visits:
                        self.run.visit_list.append(current_hh)
                    else:
                        '''add event to give paper if max visits received - but what will the HH then do?'''
                        self.run.output_data.append(visit_paper(self.run.run, self.run.reps, self.run.env.now, current_hh.id_num,
                                                          current_hh.hh_type))
                        current_hh.paper_allowed = True
                        current_hh.resp_level = current_hh.decision_level(self.input_data[current_hh.hh_type], "resp")
                        current_hh.help_level = current_hh.resp_level + current_hh.decision_level(self.input_data[current_hh.hh_type], "help")
                        current_hh.refuse_level = current_hh.help_level + current_hh.decision_level(self.input_data[current_hh.hh_type], "refuse")

                        self.run.env.process(current_hh.action())

                    # transfer enumerator back to available list
                    self.run.output_data.append(enu_util(self.run.run, self.run.reps, self.run.env.now, len(self.run.enu_working)))
                    self.run.enu_working.remove(self)
                    self.run.enu_avail.append(self)
                    self.run.output_data.append(enu_util(self.run.run, self.run.reps, self.run.env.now, len(self.run.enu_working)))

            elif self.working_test() is False:
                    # not yet at work, time out until the next time they are due to start work
                yield self.run.env.timeout(self.hold_until())
            else:
                yield self.run.env.timeout(24)
                # yield self.env.timeout((self.run.sim_days * 24) - self.env.now)

    def fu_visit_assist(self, current_hh):
        """once contact is made determine if digital assistance is required"""

        if current_hh.resp_type == 'paper':
            dig_assist_test = self.run.rnd.uniform(0, 100)
            if dig_assist_test < current_hh.input_data['dig_assist_eff']:
                # persuades hh to switch to digital from paper
                current_hh.resp_type = 'digital'
                current_hh.delay = 0
                """how long would it take to change their minds?"""
                yield self.run.env.timeout(0.2 + self.run.travel_time)
                yield self.run.env.process(self.fu_visit_outcome(current_hh))

            elif current_hh.input_data['dig_assist_eff'] <= dig_assist_test <\
                    (current_hh.input_data['dig_assist_eff'] + current_hh.input_data['dig_assist_flex'])\
                    or current_hh.visits == current_hh.max_visits:
                # allows hh to use paper to respond
                """how long to get to the point of letting them have paper?"""
                yield self.run.env.timeout(0.2 + self.run.travel_time)
                current_hh.paper_allowed = True
                current_hh.resp_level = current_hh.decision_level(self.input_data[current_hh.hh_type], "resp")
                current_hh.help_level = current_hh.resp_level + current_hh.decision_level(self.input_data[current_hh.hh_type], "help")
                current_hh.refuse_level = current_hh.help_level + current_hh.decision_level(self.input_data[current_hh.hh_type], "refuse")
                yield self.run.env.process(self.fu_visit_outcome(current_hh))

            else:
                # suggests another form of digital assist - another visit in this case
                """how long would suggesting different forms of digital assist take?"""
                yield self.run.env.timeout(0.2 + self.run.travel_time)
                self.run.output_data.append(visit_assist(self.run.run, self.run.reps, self.run.env.now, current_hh.id_num, current_hh.hh_type))
                current_hh.pri -= 5  # they have asked for help so raise the priority of the hh
                # so put hh back in the list to visit if max visits not reached
                if current_hh.visits < current_hh.max_visits:
                    self.run.visit_list.append(current_hh)
                # transfer enumerator back to available list
                self.run.output_data.append(enu_util(self.run.run, self.run.reps, self.run.env.now, len(self.run.enu_working)))
                self.run.enu_working.remove(self)
                self.run.enu_avail.append(self)
                self.run.output_data.append(enu_util(self.run.run, self.run.reps, self.run.env.now, len(self.run.enu_working)))

        else:
            yield self.run.env.process(self.fu_visit_outcome(current_hh))

    def fu_visit_outcome(self, current_hh):

        hh_responds = False

        if self.run.rnd.uniform(0, 100) <= self.input_data[current_hh.hh_type]['conversion_rate']:
            hh_responds = True

        # in but already replied
        if current_hh.resp_sent is True:
            self.run.output_data.append(visit_wasted(self.run.run, self.run.reps, self.run.env.now, current_hh.id_num, current_hh.hh_type))
            yield self.run.env.timeout((5 / 60) + self.run.travel_time)

        # in and respond - there and then
        if current_hh.resp_sent is False and hh_responds is True:
            self.run.output_data.append(visit_success(self.run.run, self.run.reps, self.run.env.now, current_hh.id_num,
                                                      current_hh.hh_type))
            current_hh.resp_planned = True
            yield self.run.env.timeout((12 / 60) + self.run.travel_time)
            self.run.env.process(current_hh.respond(True, current_hh.delay))

        # in but no immediate response
        if current_hh.resp_sent is False and hh_responds is False:
            self.run.output_data.append(visit_contact(self.run.run, self.run.reps, self.run.env.now, current_hh.id_num,
                                                      current_hh.hh_type))
            yield self.run.env.timeout((5 / 60) + self.run.travel_time)
            """After a visit where they don't respond what do hh then do?"""
            current_hh.resp_level = current_hh.decision_level(self.input_data[current_hh.hh_type], "resp")
            current_hh.help_level = current_hh.resp_level + current_hh.decision_level(self.input_data[current_hh.hh_type], "help")
            current_hh.refuse_level = current_hh.help_level + current_hh.decision_level(self.input_data[current_hh.hh_type], "refuse")
            current_hh.pri += 2
            # then put back in the list to visit at the end with new pri but only if below max_visit number
            if current_hh.visits < current_hh.max_visits:
                self.run.visit_list.append(current_hh)

            '''comment out the below? As the hh should really just do whatever they were going to?'''
            self.run.env.process(current_hh.action())

        # transfer enumerator back to available list
        self.run.output_data.append(enu_util(self.run.run, self.run.reps, self.run.env.now, len(self.run.enu_working)))
        self.run.enu_working.remove(self)
        self.run.enu_avail.append(self)
        self.run.output_data.append(enu_util(self.run.run, self.run.reps, self.run.env.now, len(self.run.enu_working)))

    def hold_until(self):

        if self.current_date() < self.start_date:
            diff = (self.start_date - self.current_date()).days * 24
            return diff + self.start_time
        elif self.current_date() >= self.start_date:
            if self.run.env.now % 24 < self.start_time:
                return self.start_time - self.run.env.now % 24
            elif self.run.env.now % 24 >= self.end_time:
                return self.start_time + (24 - self.run.env.now % 24)

    def current_date(self):
        return (self.run.sim_start + datetime.timedelta(hours=self.run.env.now)).date()

    def working_test(self):
        """returns true or false to depending on whether or not an enumerator is available"""

        # return current data as date time object

        if (self.start_date <= self.current_date() <= self.end_date) \
                and (self.start_time <= self.run.env.now % 24 < self.end_time):
            return True
        else:
            return False













