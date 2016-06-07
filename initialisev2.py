"""run class is the instance that contains all that is required for current run of simulation"""
import district


class Run(object):
    """contains the methods and data for an individual run"""

    def __init__(self, env, input_data, rnd, run, reps, seed):

        # values fed into class
        self.env = env
        self.input_data = input_data
        self.rnd = rnd
        self.reps = reps
        self.seed = seed

        # variables created within the class
        self.total_ad_instances = 0
        self.total_web_chat_instances = 0
        self.districts = []  # list containing each instance of the district class
        self.ad_avail = []

        # methods to run on start
        self.create_advisers(self.input_data, "")
        self.create_districts()

    def create_advisers(self, input_data, input_key):

        # create the advisers - both web and phone
        id_ad_num = self.total_ad_instances
        id_web_chat_num = self.total_ad_instances
        for key, value in input_data.items():
            if isinstance(value, dict):

                self.create_advisers(value, key)

            elif key == "number" and ("telephone" in input_key) == True:
                for i in range(int(input_data["number"])):

                    print('ad created')

                    #self.ad_avail.append(census.Adviser(self,
                    #                                    id_ad_num,
                    #                                    input_data["start_time"],
                    #                                    input_data["end_time"],
                    #                                    input_data["start_date"],
                    #                                    input_data["end_date"],
                    #                                    input_key,
                    #                                    input_data["FU_on"]))
                    id_ad_num += 1
                    self.total_ad_instances += 1

            elif key == "number" and ("web" in input_key) == True:
                for i in range(int(input_data["number"])):
                    print('web chat created')

                    #self.ad_chat_avail.append(census.AdviserChat(self,
                    #                                             id_ad_chat_num,
                    #                                             input_data["start_time"],
                    #                                             input_data["end_time"],
                    #                                             input_data["start_date"],
                    #                                             input_data["end_date"],
                    #                                             input_key))
                    id_web_chat_num += 1
                    self.total_web_chat_instances += 1

        return input_key

    def create_districts(self):

        for dist in self.input_data['districts']:

            print(dist)

            self.districts.append(district.District(self, self.rnd, self.env, dist, self.input_data['districts'][dist]))
