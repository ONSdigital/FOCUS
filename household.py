"""file used to store the classes and definitions related to the households represented in the simulation """

import math
from collections import namedtuple
import datetime
import census
from simpy.util import start_delayed


# the below tuples allow for key variables to be stored as events occur
# namedtuple also useful for reading data.JSON from csv/SQL ready for conversion to database in future
response_times = namedtuple('Responded', ['run', 'reps', 'Time', 'Household', 'Type', 'Calls', 'Visits', 'Response'])  # time full response received
adviser_util = namedtuple('Adviser_util', ['run', 'reps', 'Time', 'Count', 'Household'])  # call centre staff usage over time
wait_times = namedtuple('Wait_time', ['run', 'reps', 'Time', 'Wait', 'Household'])  # hh wait times for phone call
partial_response_times = namedtuple('Partial_response', ['run', 'reps', 'Time', 'Household'])  # time partial response received
fu_call_time = namedtuple('FU_call', ['run', 'reps', 'Time', 'Household'])  # times of FU calls and to whom
hh_assist = namedtuple('HH_refuse', ['run', 'reps', 'Time', 'Household'])  # time of assist
hh_refuse = namedtuple('HH_refuse', ['run', 'reps', 'Time', 'Household'])  # time of refusal
phone_call_time = namedtuple('Phone_call', ['run', 'reps', 'Time', 'Household'])  # time of phone call from hh
do_nothing = namedtuple('Do_nothing', ['run', 'reps', 'Time', 'Household'])
call_in = namedtuple('Call_in', ['run', 'reps', 'Time', 'Household'])
call_out = namedtuple('Call_out', ['run', 'reps', 'Time', 'Household'])
call_success = namedtuple('Call_success', ['run', 'reps', 'Time', 'Household'])
wasted_call = namedtuple('Call_wasted', ['run', 'reps', 'Time', 'Household'])
hung_up = namedtuple('Hung_up', ['run', 'reps', 'Time', 'Household'])
letter_received = namedtuple('letter_received', ['run', 'reps', 'Time', 'Household', 'Type'])
letter_wasted = namedtuple('letter_wasted', ['run', 'reps', 'Time', 'Household', 'Type'])
paper_requested = namedtuple('paper_requested', ['run', 'reps', 'Time', 'Household', 'Type'])


class Household(object):

    # Create an instance of the class
    def __init__(self, run, env, hh_type, id_num, input_data):
        self.env = env
        self.id_num = id_num
        self.hh_type = hh_type
        self.input_data = input_data
        self.output_data = run.output_data
        self.run = run  # the run to which the hh belongs

        self.paper_allowed = str2bool(self.input_data['allow_paper'])
        self.FU_on = str2bool(self.input_data['FU_on'])

        self.resp_time = 0  # will record time of response
        self.call_renege = self.run.rnd.uniform(0.05, 0.25)  # time until caller hangs up will vary by group, random for now
        self.time_called = 0  # time of phone call
        self.max_visits = self.input_data['max_visits']
        self.pri = self.input_data['priority']  # set FU priority. Lower numbers have higher priority
        self.resp_type = self.set_preference()  # needs to be set based on inputs
        self.fu_start = self.input_data['FU_start_time']

        self.status = 'initialised'  # current status of household
        self.resp_planned = False  # records if a response is planned
        self.resp_sent = False  # records if a response has been sent
        self.resp_rec = False  # records if a response has been received
        self.letter_count = 0  # number of letters received
        self.visits = 0  # number of visits received
        self.calls = 0  # number of calls received

        """temp only for console output"""
        self.run.hh_count[self.hh_type] += 1

        self.resp_level = self.decision_level(self.input_data, 'resp')
        self.help_level = self.resp_level + self.decision_level(self.input_data, 'help')

        self.check = env.process(self.action())

    def action(self):

        """Defines the different actions a household may take in responding to a census. A COA is
        decided upon and this will run it's course unless other events (letters etc) modify that process whereby
        the process loops back to the COA event and a new response is chosen from a new more appropriate
        distribution"""

        while True and self.resp_sent is False and self.resp_planned is False:
            """action based upon the current values for resp for individual households"""

            action_test = self.run.rnd.uniform(0, 100)  # represents the "decision".


            # below is the decision tree that will use the set values
            if action_test <= self.resp_level:

                # they respond
                self.resp_planned = True
                response_time = response_profiles(self.run, "HH_resp_time")
                #print(self.id_num, "respond", response_time)
                self.status = "Responding"
                yield self.env.timeout(response_time)  # wait until time
                yield self.env.process(self.respond(True, self.delay))

            elif self.resp_level < action_test <= self.help_level:
                # ask for help
                contact_time = response_profiles(self.run, "contact")
                #print(self.id_num, "contact", contact_time)
                self.status = "assist"
                yield self.env.timeout(contact_time)
                yield self.env.process(self.contact())

            else:
                # do nothing
                self.output_data.append(do_nothing(self.run.run, self.run.reps, self.env.now, self.id_num))
                self.status = "Do nothing"
                #print(self.id_num, "nothing")
                yield self.env.timeout((self.run.sim_days*24) - self.env.now)  # do nothing so pause until end of sim

    # HH requires assistance so pick an option to use - need data.JSON on this or can test impact of variation
    def contact(self):

        """what proportion of people will make contact via the different options and how will that differ if they
        prefer paper over digital?"""
        if self.run.rnd.uniform(0, 100) <= 0:
            # Online, web chat, assistance
            yield self.env.process(self.webchat())
        else:
            # phone
            yield self.env.process(self.phone_call_connect())
        # text assist
        # Post assistance
        # completion events
        # and so on

    # will need to update this to use the new store and JSON data
    def webchat(self):
        """live web chat"""
        """need to add some event tracking"""

        if self.resp_sent is False:

            # get current day and extract number of call centre staff and time they are available for that day
            temp_date = str((self.run.start_date + datetime.timedelta(hours=self.run.env.now)).date())
            day_cap = int(self.run.adviser_chat_dict[temp_date]['capacity'])
            start_time = int((self.run.adviser_dict[temp_date]['time']).split('-')[0])
            end_time = int((self.run.adviser_dict[temp_date]['time']).split('-')[1])

            # if current time is between the above times
            if start_time <= self.env.now % 24 < end_time:

                self.time_called = self.env.now
                current_chat_ad = yield self.run.adviser_chat_store.get()  # gets a web chat adviser

                wait_time = self.env.now - self.time_called  # calc time a hh would have spent waiting if never hung up

                """How long are people prepared to wait online?"""
                if wait_time >= self.call_renege:

                    yield self.run.adviser_chat_store.put(current_chat_ad)
                    """after failing to get through what does a hh do?"""
                    self.resp_level = 0
                    self.help_level = self.resp_level + 60
                    self.refuse_level = self.help_level + 0
                    yield self.env.process(self.action())

                else:

                    if self.status == "assist":
                        self.pri -= 2
                        self.output_data.append(hh_assist(self.run.run, self.run.reps, self.env.now, self.id_num))
                        """how successful is web chat in assisting hh?"""
                        self.resp_level = 25
                        self.help_level = self.resp_level + 25
                        self.refuse_level = self.help_level + 0
                    elif self.status == "refuse":
                        self.pri -= 1
                        self.output_data.append(hh_refuse(self.run.run, self.run.reps, self.env.now, self.id_num))
                        """if they refuse what proportion are convinced to otherwise after a web chat?"""
                        self.resp_level = 2
                        self.help_level = self.resp_level + 0
                        self.refuse_level = self.help_level + 0

                    """how long does a web chat last?"""
                    yield self.env.timeout(0.25)
                    yield self.run.adviser_chat_store.put(current_chat_ad)
                    yield self.env.process(self.action())

            else:
                # tried when the chat lines are closed. Will be dealt with by automated functions
                """Need to understand how people respond when they do not get through?"""
                self.resp_level = 0
                self.help_level = self.resp_level + 80
                self.refuse_level = self.help_level + 0
                yield self.env.process(self.action())

    # will need to update this to use the new store and JSON data
    def online(self):
        """other online request for help, e.g, e-mail, web form, text"""

        if self.status == "assist":
            self.pri -= 2
            self.output_data.append(hh_assist(self.run.run, self.run.reps, self.env.now, self.id_num))
            """after using this method to ask for help what will the hh then do?"""
            self.resp_level = 0
            self.help_level = self.resp_level + 50
            self.refuse_level = self.help_level + 0
        elif self.status == "Refuse":
            self.pri -= 1
            self.output_data.append(hh_refuse(self.run.run, self.run.reps, self.env.now, self.id_num))
            """after using this method to refuse to complete what will the hh then do?"""
            self.resp_level = 0
            self.help_level = self.resp_level + 0
            self.refuse_level = self.help_level + 0

        yield self.env.process(self.action())

    def phone_call_connect(self):
        """Represents whether or not a call by a hh leads to an adviser answering"""

        if self.resp_sent is False:

            if len(self.run.ad_working) > 0 or len(self.run.adviser_store.items) > 0:
                # try and connect to an adviser
                self.time_called = self.env.now
                self.output_data.append(phone_call_time(self.run.run, self.run.reps, self.env.now, self.id_num))
                self.output_data.append(adviser_util(self.run.run, self.run.reps, self.env.now, len(self.run.ad_working), self.id_num))
                # to get util here divide by the store contents
                current_ad = yield self.run.adviser_store.get()  # gets an adviser from the store...
                self.run.ad_working.append(current_ad)  # and puts it in a working list

                current_ad.current_hh = self.id_num

                wait_time = self.env.now - self.time_called  # calc time a hh would have spent waiting if never hung up

                if wait_time != 0:
                    self.output_data.append(wait_times(self.run.run, self.run.reps, self.env.now, wait_time, self.id_num))

                """How long are people prepared to wait on the phone?"""
                # if greater than renege time for that hh then hang up
                if wait_time >= self.call_renege:

                    self.run.ad_working.remove(current_ad)
                    yield self.run.adviser_store.put(current_ad)
                    self.output_data.append(hung_up(self.run.run, self.run.reps, self.time_called + self.call_renege, self.id_num))
                    """after failing to get through what does a hh do?"""
                    self.resp_level = 0
                    self.help_level = self.resp_level + 50

                    yield self.env.process(self.action())

                # if not then connect the call
                elif wait_time < self.call_renege:
                    self.output_data.append(adviser_util(self.run.run, self.run.reps, self.env.now,len(self.run.ad_working), self.id_num))
                    current_ad.time_answered = self.time_called + wait_time  # sets the time this adviser last answered

                    # connected what is the outcome
                    yield self.env.process(self.phone_call_assist(current_ad))

            else:
                # called when the lines are closed. Will be dealt with by automated line - how does this work?
                """Need to understand how people respond when they do not get through?"""
                self.resp_level = 0
                self.help_level = self.resp_level + 80
                yield self.env.process(self.action())

    def phone_call_assist(self, current_ad):
        """represents the phase of a phone call where the adviser decides how they will assist the hh"""

        if self.paper_allowed is False and self.resp_type == 'paper':  # if paper is allowed should we skip this and go for default values? Yes...
            call_ans_test = self.run.rnd.uniform(0, 100)
            if call_ans_test < self.input_data['dig_assist_eff']:
                # persuades hh to switch to digital from paper
                self.resp_type = 'digital'
                current_ad.length_of_call = 0.1
                yield self.env.timeout(current_ad.length_of_call)
                self.output_data.append(adviser_util(self.run.run, self.run.reps, self.env.now,len(self.run.ad_working), self.id_num))
                yield self.env.process(self.phone_call_result(current_ad))

            elif self.input_data['dig_assist_eff'] <= call_ans_test < (self.input_data['dig_assist_eff'] + self.input_data['dig_assist_flex']):
                """what proportion of people would we give a paper form too if they asked?"""
                # allows hh to use paper to respond
                current_ad.length_of_call = 0.1
                yield self.env.timeout(current_ad.length_of_call)
                self.run.ad_working.remove(current_ad)
                yield self.run.adviser_store.put(current_ad)
                self.output_data.append(adviser_util(self.run.run, self.run.reps, self.env.now,len(self.run.ad_working), self.id_num))
                self.output_data.append(paper_requested(self.run.run, self.run.reps, self.env.now, self.id_num, self.hh_type))

                # pass values as always - but now allowed to use paper so can use default values
                self.paper_allowed = True
                self.resp_level = self.decision_level(self.input_data, "resp")
                self.help_level = self.resp_level + self.decision_level(self.input_data, "help")
                self.env.process(self.action())
            else:
                # suggests another form of digital assist - a visit in this case
                """how long would suggesting different forms of digital assist take?"""
                current_ad.length_of_call = 0.1
                yield self.env.timeout(current_ad.length_of_call)
                self.run.ad_working.remove(current_ad)
                yield self.run.adviser_store.put(current_ad)
                self.output_data.append(adviser_util(self.run.run, self.run.reps, self.env.now,len(self.run.ad_working), self.id_num))
                self.pri -= 5  # they have asked for help so raise the priority of the hh
        else:
            # dig so carry on and see what the result is
            yield self.env.process(self.phone_call_result(current_ad))

    def phone_call_result(self, current_ad):
        """represents the phase where a response may be received"""

        """need to understand how successful taking calls is at gaining responses for each group of interest.
        This is the effectiveness of this form of digital assist"""
        success_test = 15
        call_result_test = self.run.rnd.uniform(0, 100)

        if call_result_test <= success_test:  # responded over the phone
            """how long would these calls last?"""
            current_ad.length_of_call = 0.1
            yield self.env.timeout(current_ad.length_of_call)
            self.run.ad_working.remove(current_ad)
            yield self.run.adviser_store.put(current_ad)
            self.output_data.append(adviser_util(self.run.run, self.run.reps, self.env.now,len(self.run.ad_working), self.id_num))
            self.resp_planned = True
            yield self.env.process(self.respond(True, self.delay))  # and create a successful response
            # some don't so back to action but with dig preferences
        else:
            """how long would these calls last?"""
            current_ad.length_of_call = 0.1
            yield self.env.timeout(current_ad.length_of_call)
            self.run.ad_working.remove(current_ad)
            yield self.run.adviser_store.put(current_ad)
            self.output_data.append(adviser_util(self.run.run, self.run.reps, self.env.now,len(self.run.ad_working), self.id_num))
            # back to the action progress with default values...and eventually with updated parameters
            # reflecting how this type of hh would now respond...
            """After a call where they don't respond there and then what do hh then do?"""
            self.resp_level = 60
            self.help_level = self.resp_level + 10
            self.refuse_level = self.help_level + 0
            yield self.env.process(self.action())

    def respond(self, success=False, delay=0):
        """represents the hh responding - not the response being received by census"""

        if self.resp_sent is False:

            call_response_test = self.run.rnd.uniform(0, 100)

            """what percent by each group sends in incomplete responses?"""
            if call_response_test < 0 and success is False:
                # 5% chance response is incomplete/wrong in some way
                # what % will be incomplete by the groups we are interested in?? abd will we follow these up?
                self.pri += 3  # hardcoded for now - allow to be changed from user inputs in future
                self.output_data.append(partial_response_times(self.run.run, self.run.reps, self.resp_time, self.id_num))
                """do these need to be followed up by a dedicated team?"""

            else:
                #self.run.total_responses += 1
                self.resp_sent = True
                self.resp_time = self.env.now
                # add to event log
                self.output_data.append(response_times(self.run.run, self.run.reps, self.resp_time, self.id_num,
                                                       self.hh_type, self.calls, self.visits, self.resp_type))

                if self.delay == 0:
                    self.env.process(census.resp_rec(self.env, self, self.run))
                else:
                    start_delayed(self.env, census.resp_rec(self.env, self, self.run), delay)

                yield self.env.timeout((self.run.sim_days*24) - self.env.now)  # do nothing more until sim ends

    def rec_letter(self, letter_type, effect):
        """represents the hh receiving a letter"""

        self.letter_count += 1

        if self.resp_sent is True:
            # record wasted letter but otherwise do nothing more
            self.output_data.append(letter_wasted(self.run.run, self.run.reps, self.env.now, self.id_num, letter_type))
            yield self.env.timeout(0)
        elif self.resp_sent is False: # and self.resp_type == 'digital':
            self.output_data.append(letter_received(self.run.run, self.run.reps, self.env.now, self.id_num, letter_type))

            """how effective are letters? Does effectiveness change the more you send? Do different letters
            have different effectiveness on different groups?"""
            self.resp_level = effect
            self.help_level = 0

            # then back to action with the updated values
            yield self.env.process(self.action())

    def set_preference(self):
        """sets whether the hh prefers paper or digital and the associated time to receive responses from both"""
        paper_test = self.run.rnd.uniform(1, 100)

        if paper_test <= int(self.input_data['paper_prop']):
            """how long a delay is there from a paper response being sent and census knowing?"""
            self.delay = 72
            return "paper"

        self.delay = 0
        return "digital"

    def decision_level(self, input_data, input_type):
        """general lookup that will return a response level of passed type:

             - Either a default that represents response levels when no barriers are put in place of responses:
             - or an alternative that modifies the response levels for those who want to use a mode that is not allowed
               by default (though some groups may still respond in that manner). Essentially the alternative shifts
               those people who would have responded automatically to either ask for help, refuse or do nothing."""

        # if paper allowed use defaults
        if self.resp_type == 'digital' or self.paper_allowed is True:
            temp_key = 'default_' + input_type
            return input_data[temp_key]
        else:
            temp_key = 'alt_' + input_type
            return input_data[temp_key]


# pass key and return values
def lookup_letter(letter_dict, letter_type, return_value):
    return letter_dict[letter_type][return_value]


def response_profiles(run, dist_name):

    """what distributions should be used to represent the below?"""
    if dist_name == "HH_resp_time":
        return (run.rnd.betavariate(1, 2))*((run.sim_days*24) - run.env.now)
    elif dist_name == "Refuse":
        return run.rnd.uniform(9, (run.sim_days*24) - run.env.now)
    else:
        return run.rnd.uniform(9, (run.sim_days*24) - run.env.now)


def next_day(env):
    return math.ceil(env.now/24)*24 - env.now


def days_delay(delay, env):
    return next_day(env) + (delay * 24.00)

"""converts string inputs to a bool"""
def str2bool(value):
    return str(value).lower() in ("True", "true", "1")
