"""run class is the instance that contains all that is required for current run of simulation"""
import district


class Run(object):
    """contains the methods and data for an individual run"""

    def __init__(self, env, input_data, rnd, run, reps, seed):

        self.env = env
        self.input_data = input_data
        self.rnd = rnd
        self.reps = reps
        self.seed = seed
        self.districts = []  # list containing each instance of the district class

        self.create_districts()

    def create_districts(self):

        for dist in self.input_data['districts']:

            print(dist)

            self.districts.append(district.District(self.rnd, self.env, dist, self.input_data['districts'][dist]))



