"""store the named tuples used to record output as well as the values of the labels that defines what sorts of
output are recorded. The more output that is selected the slower the code will run"""

from collections import namedtuple

generic_output = namedtuple('Generic_output', ['rep', 'district', 'LA', 'lsoa11cd', 'digital', 'hh_type', 'hh_id', 'time'])
visit_output = namedtuple('Visit_output', ['rep', 'district', 'LA', 'lsoa11cd', 'digital', 'hh_type', 'hh_id', 'visits', 'time'])
reminder_wasted = namedtuple('Reminder_wasted', ['rep', 'district', 'LA', 'lsoa11cd', 'digital', 'hh_type', 'hh_id', 'time','type'])
reminder_unnecessary = namedtuple('Reminder_unnecessary', ['rep', 'district', 'LA', 'lsoa11cd', 'digital', 'hh_type', 'hh_id', 'time', 'type'])
reminder_success = namedtuple('Reminder_success', ['rep', 'district', 'LA', 'lsoa11cd', 'digital', 'hh_type', 'hh_id', 'time', 'type'])
reminder_received = namedtuple('Reminder_received', ['rep', 'district', 'LA', 'lsoa11cd', 'digital', 'hh_type', 'hh_id', 'time', 'type'])
call_wait_times = namedtuple('Call_wait_times', ['rep', 'district', 'LA', 'lsoa11cd', 'digital', 'hh_type', 'hh_id', 'time', 'wait_time'])
hh_record = namedtuple('hh_record', ['rep', 'district', 'LA', 'lsoa11cd', 'hh_type', 'action', 'digital', 'start_paper', 'time'])
warnings = namedtuple('Warnings', ['rep', 'warning', 'detail'])
initial_action = namedtuple('Initial_action', ['type', 'digital', 'time', 'engaged'])
hh_geography = namedtuple('hh_geography', ['la', 'lsoa', 'district_name', 'hh_type', 'digital'])

"""
record_generic_output = True
record_do_nothing = True
record_call = True
record_call_defer = True
record_call_renege = True
record_call_contact = True
record_call_convert = True
record_call_request = True
record_call_success = True
record_call_failed = True
record_return_sent = True
record_reminder_wasted = True
record_reminder_unnecessary = True
record_reminder_received = True
record_reminder_success = True
record_call_wait_times = True
record_warnings = True
record_non_response = True
record_letters = True
record_posted = True
record_visit = True
record_visit_wasted = True
record_visit_unnecessary = True
record_visit_contact = True
record_visit_out = True
record_visit_convert = True
record_visit_assist = True
record_visit_success = True
record_visit_failed = True
record_return_received = True
record_responded = True
record_hh_record = True
record_key_info = True
"""
record_passive_summary = True
record_active_summary = True
record_active_paper_summary = True
record_visit_summary = True
record_time_summary = True
record_paper_summary = True

record_generic_output = False
record_do_nothing = False
record_call = False
record_call_defer = False
record_call_renege = False
record_call_contact = False
record_call_convert = False
record_call_request = False
record_call_success = False
record_call_failed = False
record_return_sent = False
record_reminder_wasted = False
record_reminder_unnecessary = False
record_reminder_received = False
record_reminder_success = True
record_call_wait_times = False
record_warnings = False
record_non_response = False
record_letters = False
record_posted = False
record_visit = True
record_visit_wasted = False
record_visit_unnecessary = False
record_visit_contact = False
record_visit_out = False
record_visit_convert = False
record_visit_assist = False
record_visit_success = True
record_visit_failed = False
record_return_received = False
record_responded = False
record_hh_record = True
record_key_info = False
