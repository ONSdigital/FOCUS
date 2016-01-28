"""a stub for the district class"""


class District(object):

    def __init__(self, run, env, input_data):
        self.run = run
        self.env = env
        self.input_data = input_data


        '''each instance of a District contains 1 coordinator,
        a number of households
        and a set of enumerators - though schedules and numbers would be set via a default at a higher level

        call center staff are shared across  districts so those instances are generated elsewhere

        default letters at higher level and ue at district level unless overridden


        so:

        1) Create a run instance (effectively the areas to be represented)
        2) Create resources to be shared across the UK - Advisers, paper?
        3) Create each district object
        4) which creates its HH
        5) and ts own coordinator who in turn creates the enumerators


        '''



