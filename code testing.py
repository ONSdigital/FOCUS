"""module used to contain code used to analyse results as part of QA of the code including checking behaviour
is as expected."""

import post_process
import os


# function that analyses data to produce visit success rates for each round of visits for each type of hh
# to get an overall figure remove the hh_type form the groupby

def visit_success_rates(output_path):

    # read required files into dataframes
    pandas_data = post_process.csv_to_pandas(output_path, ['Visit_success', 'Visit'])

    # get success dataframe
    visit_success = pandas_data['Visit_success']['1']

    # sum the number of successes achieved by type and attempt
    success_sum = visit_success.groupby(['hh_type', 'attempt', 'contacts']).size().reset_index(name='sum')
    #print(success_sum)

    visits = pandas_data['Visit']['1']
    visit_sum = visits.groupby(['hh_type', 'attempt', 'contacts']).size().reset_index(name='sum')
    #print(visit_sum)

    success_sum.index = visit_sum.index
    success_sum['prop_success'] = success_sum['sum'].div(visit_sum['sum'], axis='index')
    print(success_sum)




output_path = os.path.join(os.getcwd(), "outputs")
visit_success_rates(output_path)
