"""module used to aggregate raw csv files into master datalists ready for analysis of further output"""
import pandas as pd
import os
import glob
import csv
import numpy as np
from collections import defaultdict
import datetime as dt
import math
from bokeh.plotting import ColumnDataSource, figure
from bokeh.io import output_file, save
from bokeh.models import DatetimeTickFormatter, HoverTool
import matplotlib.pyplot as plt
plt.style.use('ggplot')


def cumulative_sum(df, start, end, step, geog, resp_type='all'):
    """takes a data frame and returns average cumulative totals for specified geography in correct format for data
     vis teams map template"""

    # create bins and group names to use
    bin_values = np.arange(start, end + step, step)
    group_names = np.arange(start, end, step)

    # filter df to only have correct entries
    if resp_type == 'digital':
        df = df[df['digital'] == True].copy()

    elif resp_type == 'paper':
        df = df[df['digital'] == False].copy()

    # add a new column to passed data frame containing the categories the entry belongs too
    df['categories'] = pd.cut(df['time'], bin_values, labels=group_names)
    # group by each combination of district and category and count the number of each category
    cat_sum = df.groupby([geog, 'categories', 'rep'])['categories'].size()
    # calculate the cum sum of the totals for each rep
    cat_sum = cat_sum.groupby(level=[0, 2]).cumsum().reset_index()
    cat_sum.rename(columns={0: 'count'}, inplace=True)
    # and then take the mean
    cat_sum = cat_sum.groupby([geog, 'categories'])['count'].mean().reset_index()
    # pivot it so the categories become the columns
    cat_sum_flipped = cat_sum.pivot(index=geog, columns='categories', values='count')
    # and then add back in any missing categories and fill the gaps
    cat_sum_flipped = cat_sum_flipped.reindex(columns=group_names).ffill(axis=1).replace('Nan', 0, regex=True)

    return cat_sum_flipped


def divide_all(data_numerator, data_denominator, runs=()):

    divide_runs = defaultdict(list)

    for key, value in data_numerator.items():
        divide_runs[key] = divide_single(data_numerator[key], data_denominator)

    return divide_runs


def divide_single(data_numerator, data_denominator):

    int_num_df = pd.DataFrame({'numerator_result': data_numerator.groupby(['rep', 'hh_type']).size()}).reset_index()
    out_num_df = pd.DataFrame(int_num_df.groupby(['hh_type']).mean()['numerator_result']).reset_index()

    # add item to divide by do div and keep only new...
    int_den_df = pd.DataFrame({'denominator_result': data_denominator.groupby(['rep', 'hh_type']).size()}).reset_index()
    out_den_df = pd.DataFrame(int_den_df.groupby(['hh_type']).mean()['denominator_result']).reset_index()

    returns = pd.merge(out_num_df, out_den_df, on='hh_type')

    returns['returns'] = returns[['numerator_result']].div(getattr(returns, 'denominator_result'), axis=0)
    returns = returns[['hh_type', 'returns']]

    return returns


def divide_single_count(data_numerator, data_denominator):

    int_num_df = pd.DataFrame({'numerator_result': data_numerator.groupby(['rep', 'district']).size()}).reset_index()
    out_num_df = pd.DataFrame(int_num_df.groupby(['district']).mean()['numerator_result']).reset_index()

    out_den_df = pd.DataFrame(data_denominator.groupby(['district']).mean()['hh_count']).reset_index()

    returns = pd.merge(out_num_df, out_den_df, on='district')

    returns['returns'] = returns[['numerator_result']].div(getattr(returns, 'hh_count'), axis=0)
    returns = returns[['returns']]

    return returns


def add_hh_count(data_lists):

    hh_count = defaultdict(list)

    for df in data_lists['hh_count']:
        hh_count['hh_count'].append(df)

    return hh_count


def add_days(input_date, add_days):

    return input_date + dt.timedelta(days=add_days)


def bin_records(input_df, start_date, drop_filter=[]):
    """takes files and produces binned data by day. The categories are set from the passed start date"""

    # drop any rows that are not relevant - only applicable when using "hh record"
    if drop_filter:
        for item in drop_filter:
            input_df = input_df[input_df.action != item]

    # get max bin category
    time_max = roundup(input_df[['time']].max(axis=0), 24)
    bins = list(np.arange(0, time_max, 24))
    # set group names to be the dates from the specified start date
    group_names = []
    days = len(bins)

    for i in range(0, days - 1):
        group_names.append(add_days(start_date, i))

    # get number of replications
    reps = input_df['rep'].max()
    counts_df = pd.DataFrame(index=group_names)

    # for each replication bin the data
    for i in range(1, reps+1):

        # filter to get each reps data then count
        temp_df = input_df[(input_df.rep == i)].copy()

        temp_df[str(i)] = pd.cut(temp_df['time'], bins, labels=group_names)
        counts = pd.value_counts(temp_df[str(i)])
        # add back in missing dates??
        counts = pd.DataFrame(counts)

        counts_df = pd.concat([counts_df, counts], axis=1)
        # now average the rows

    # finally get the average count for the run
    averages = counts_df.mean(axis=1)
    return averages


def roundup(x, y):
    # roundup x to the nearest y
    # e.g. if y is 100 it will roundup to the nearest 100

    return int(math.ceil(x / float(y))) * y


def rounddown(x, y):
    # rounddown x to the nearest y
    # e.g. if y is 100 it will rounddown to the nearest 100

    return int(math.floor(x / float(y))) * y


def combined_chart(input1, input2, label1, label2, filename):
    # takes two series objects and combines to produce a html bokeh chart with the labels and filename given

    label1 = label1.lower()
    label2 = label2.lower()

    label1_str = label1.capitalize()
    label2_str = label2.capitalize()

    combined = pd.concat([input1, input2], axis=1)
    combined.reset_index(level=0, inplace=True)
    # ensure values are same precision

    combined.columns = ['date', label1, label2]

    combined['date_formatted'] = pd.to_datetime(combined['date'], format='%Y-%m-%d')
    combined['date_formatted'] = combined['date_formatted'].dt.strftime('%Y-%m-%d')
    max_y = roundup(max(combined[[label1, label2]].max(axis=0)), 100)

    source = ColumnDataSource(combined)

    tools = 'box_zoom,' \
            'crosshair,' \
            'resize,' \
            'reset,' \
            'pan,' \
            'wheel_zoom,' \
            'save'

    # Add lines to the figure p
    p = figure(x_axis_label='Date', y_axis_label='Returns', width=1600, height=800, tools=tools, responsive=True)
    p.line(x='date', y=label1, source=source, color='green', legend=label1_str)
    p.line(x='date', y=label2, source=source, color='blue', legend=label2_str)

    # take this from key info file....or passed to function...or a list of all the key dates...if we need them
    fu_date = dt.datetime.strptime('2011-04-01', '%Y-%m-%d').date()
    p.line([fu_date, fu_date], [0, max_y], color='red')

    hover = HoverTool(tooltips=
                      [(label1_str, '@' + label1 + '{int}'),
                       (label2_str, '@' + label2 + '{int}'),
                       ('Date', '@date_formatted')
                       ])

    p.add_tools(hover)

    # do some formatting
    p.xaxis.formatter = DatetimeTickFormatter(
        days=["%d %b %y"]
    )
    p.xaxis.major_label_orientation = math.pi / 4
    p.xgrid[0].ticker.desired_num_ticks = 11
    p.xaxis.axis_label_text_font_size = '12pt'
    p.xaxis.major_label_text_font_size = '12pt'

    p.yaxis.axis_label_text_font_size = '12pt'
    p.yaxis.major_label_text_font_size = '12pt'

    p.legend.label_text_font_size = '12pt'
    title_text = 'Effect of interventions on ' + filename[:-5]
    p.title.text = title_text

    # Specify the name of the output file and show the result
    output_path = os.path.join(os.getcwd(), 'charts', filename)
    output_file(output_path)
    save(p)


def produce_return_charts(df1, df2, label1, label2, start_date_df, filename, filter_type='E&W'):
    """Produces for a given strategy (run) the difference between the passive and the active. ie, the self
    response and response with simulated interventions"""

    # if location of dataframes passed rather than df itself
    # for each run/ed

    int_s1 = pd.Series()
    int_s2 = pd.Series()

    int_df1 = pd.DataFrame()
    int_df2 = pd.DataFrame()

    list_of_runs = df1.keys()

    if filter_type == 'E&W':
        # just use the whole df
        for run in list_of_runs:

            start_date = dt.date(*map(int, start_date_df[run]['start_date'][0].split('-')))

            series1 = bin_records(df1[run], start_date)
            series2 = bin_records(df2[run], start_date, ['do_nothing', 'help'])

            int_s1 = int_s1.add(series1, fill_value=0)
            int_s2 = int_s2.add(series2, fill_value=0)

        # returns series objects with the average (of reps) number of returns for each ed
        # add to master series
        filename = filter_type + filename
        combined_chart(int_s1, int_s2, label1, label2, filename)

    else:

        for run in list_of_runs:

            start_date = dt.date(*map(int, start_date_df[run]['start_date'][0].split('-')))
            # produce for each area/type etc...
            for district in df1[run][filter_type].unique():

                df1_temp = df1[run][df1[run][filter_type] == district].copy()
                df2_temp = df2[run][df2[run][filter_type] == district].copy()

                series1 = bin_records(df1_temp, start_date)
                series2 = bin_records(df2_temp, start_date, ['do_nothing', 'help'])

                series1.name = district
                series2.name = district

                # above returns numbers in each cat...change col name to LA?LSOA etc
                int_df1 = int_df1.add(pd.DataFrame(series1), fill_value=0)
                int_df2 = int_df2.add(pd.DataFrame(series2), fill_value=0)

        # get col names and produce a chart for each one
        for district in list(int_df1):

            series1 = pd.Series(int_df1[district])
            series2 = pd.Series(int_df2[district])

            filename_temp = filter_type + str(district) + " " + filename
            combined_chart(series1, series2, label1, label2, filename_temp)


def response_rate(df1, df2, bins, filter_type='LSOA', passive=False):
    """returns a pandas series object of the number of each filter type in each bin. For example: will by default, if
    the data contains the right columns, returns the average number of lsoas within a certain range"""

    bins = np.arange(bins[0], bins[1], bins[2])

    if passive:
        # drop the do nothings and helps
        drop_list = ['do_nothing', 'help']

        for item in drop_list:
            df1 = df1.drop(df1[(df1.action == item)].index).copy()

    # do the below for each rep...
    counts_list = []
    reps = df1['rep'].max()

    for i in range(1, reps+1):

        output_list = []

        # for each rep and LSOA or other...
        for item in df1[filter_type].unique():
            len_df1 = len(df1[(df1[filter_type] == item) & (df1['rep'] == i)])
            len_df2 = len(df2[(df2[filter_type] == item) & (df2['rep'] == i)])

            output_list.append((len_df1 / len_df2) * 100)

        # now bin the data
        output_list = pd.cut(output_list, bins)
        # counts is number of LSOA in each category for that rep
        counts = pd.value_counts(output_list)
        counts = pd.DataFrame(counts)
        counts_list.append(counts)

    counts_df = pd.concat(counts_list, axis=1)
    averages = counts_df.mean(axis=1)
    return averages


def pyramid(s1, s2, bins):
    """ produces parallel horizontal bar charts that display the distribution of response rates achieved from two
     strategies. s1, s2 are lists that contain the data frames to be used for each output(strategy) as well as the
     name of the strategy and whether it is the special case of the passive option. If it is the passive option the
     passed dataframes should be the same"""

    s1_all = pd.Series()
    s2_all = pd.Series()

    list_of_cca = s1[0].keys()

    for cca in list_of_cca:
        input1 = response_rate(s1[0][cca], s1[1][cca], bins, passive=s1[3])
        input2 = response_rate(s2[0][cca], s2[1][cca], bins, passive=s2[3])
        # add the series together - need to check sum is correct!!!
        s1_all = s1_all.add(input1, fill_value=0)
        s2_all = s2_all.add(input2, fill_value=0)

    combined_list = pd.concat([s1_all, s2_all], axis=1)
    combined_list.reset_index(level=0, inplace=True)
    combined_list.columns = ['category', s1[2], s2[2]]

    x1 = combined_list[s1[2]].tolist()
    x2 = combined_list[s2[2]].tolist()
    y = np.arange(bins[0], bins[1]-bins[2], bins[2])

    width = 1

    fig, axes = plt.subplots(ncols=2, sharey=True)
    axes[0].barh(y, x1, width,  align='center', color='green', zorder=10)
    axes[0].set(title=s1[2])
    axes[1].barh(y, x2, width, align='center', color='blue', zorder=10)
    axes[1].set(title=s2[2])
    axes[0].set_xlabel("count")
    axes[1].set_xlabel("count")

    axes[0].invert_xaxis()
    axes[0].set(yticks=y, yticklabels=y)
    axes[0].yaxis.tick_right()
    axes[0].set_ylabel("Response rate")

    for ax in axes.flat:
        ax.margins(0.03)
        ax.grid(True)

    fig.tight_layout()
    fig.subplots_adjust(wspace=0.09)
    filename = s1[2] + ' versus ' + s2[2] + '.png'
    output_path = os.path.join(os.getcwd(), 'charts', filename)
    plt.savefig(output_path)


def returns_summary(hh_record_df, returns_df,  geog='LA', resp_type='all', scenario='current'):
    """returns at the passed level the overall returns by day including averages for E&W. Used for
     producing data in the correct format for the data vis team map"""

    # gets list of runs
    runs = sorted(list(hh_record_df.keys()))

    int_df = pd.DataFrame()
    int_hh_s = pd.Series()

    # could be runs or enumeration districts
    for current_run in runs:

        # calculate the total number of households in each area and in total -  same for each rep so use rep 1
        hh_record_df_temp = hh_record_df[str(current_run)].copy()
        hh_record_df_temp = hh_record_df_temp[hh_record_df_temp['rep'] == 1].copy()
        hh_count = hh_record_df_temp.groupby(geog).size()  # hh per area in current ed
        hh_totals = hh_count.sum()  # total of households

        # produce cumulative summary of overall returns
        cumulative_returns = cumulative_sum(returns_df[str(current_run)], 0, 1824, 24, geog, resp_type)
        int_df = int_df.add(cumulative_returns, fill_value=0)

        # but also add up the totals for the household counts
        int_hh_s = int_hh_s.add(hh_count, fill_value=0)

    # then after all runs/ed's div by totals...and clac average
    int_hh_s.index = int_df.index
    cumulative_returns_per = int_df.div(int_hh_s, axis='index')
    cumulative_returns_per.to_csv(os.path.join(os.getcwd(), 'charts', resp_type + " returns summary scenario " +
                                               scenario + ".csv"))

    # also need an E+W average for each
    overall_returns = int_df.sum(axis=0)
    total_hh = int_hh_s.sum(axis=0)
    average_returns = (overall_returns / total_hh) * 100
    average_returns = pd.DataFrame(average_returns).T
    average_returns.to_csv(os.path.join(os.getcwd(), 'charts', resp_type + " average returns " +
                                        scenario + ".csv"))


def intervention_summary(data_path, data_types=(), filter_type='LA'):
    """data_path is the output data location. The types are the outputs to include in the summary. The first entry
    in the data_types tuple should be the data by which all the other data will be divided. Output is a table
    showing percentage rates for each type of event """

    # define datatypes with dtype= [] and see if faster??
    summary_df = pd.DataFrame()

    for data_type in data_types:

        input_data = csv_to_pandas(data_path, [data_type])

        runs = sorted(list(input_data[data_type].keys()))

        event_count_df = pd.DataFrame()

        for current_run in runs:

            # for each rep add up by the filter type the number of events
            temp_df = input_data[data_type][current_run]
            event_count = temp_df.groupby([filter_type, 'rep']).size().reset_index()  # events in ...
            event_count.rename(columns={0: 'count'}, inplace=True)
            # take mean of all reps
            event_count = event_count.groupby([filter_type])['count'].mean()
            event_count.name = current_run
            # and add to dataframe
            event_count_df = pd.concat([event_count_df, event_count], axis=1)

        # across all areas calc the totals for that data type
        event_count_totals = event_count_df.sum(axis=1)
        event_count_totals.name = data_type
        # add totals to df
        summary_df = pd.concat([summary_df, event_count_totals], axis=1)

    # convert to percentages
    summary_df = summary_df.div(summary_df[data_types[0]], axis=0)*100
    summary_df.to_csv(os.path.join(os.getcwd(), 'charts', 'summary results for ' + data_types[0] + ".csv"))


def combine_csv(input_path):
    """used to combine csv files into to a single data frame for later analysis"""
    temp_df = pd.DataFrame()

    glob_folder = os.path.join(input_path, '*.csv')
    file_list = glob.glob(glob_folder)  # get a list of all files in the folder

    for file in file_list:
        temp_df = temp_df.add(pd.read_csv(file, index_col=0), fill_value=0)

    return temp_df


# active_summary_path = os.path.join(os.getcwd(), 'charts', 'active summary', 'la')
# returns_df = combine_csv(active_summary_path)
# returns_df.to_csv(os.path.join(os.getcwd(), 'charts', 'active summary', 'la', 'active la' + '.csv'))s

"""
###############

below code runs functions that use raw output to generate charts. Not needed in some cases and due to be replaced.


##############

output_path = os.path.join(os.getcwd(), 'outputs', '2017-07-13 08.29.44')
current_scenario = output_path.split('/')[-1]

# produce output tables
intervention_summary(output_path, data_types=('pq_sent',
                                              'pq_wasted',
                                              'pq_success',
                                              'pq_contact',
                                              'pq_unnecessary',
                                              'pq_received',
                                              'pq_failed'
                                              ), filter_type='LA')

intervention_summary(output_path, data_types=('Visit',
                                              'Visit_wasted',
                                              'Visit_success',
                                              'Visit_contact',
                                              'Visit_unnecessary',
                                              'Visit_out',
                                              'Visit_failed',
                                              'Visit_postcard_posted'), filter_type='digital')

intervention_summary(output_path, data_types=('Visit_postcard_posted',
                                              'postcard_wasted',
                                              'postcard_success',
                                              'postcard_contact',
                                              'postcard_unnecessary',
                                              'postcard_received',
                                              'postcard_failed'), filter_type='digital')

pandas_data = csv_to_pandas(output_path, ['hh_record', 'Responded', 'key info'])

returns_summary(pandas_data['hh_record'], pandas_data['Responded'], geog='LA', scenario=current_scenario)
returns_summary(pandas_data['hh_record'], pandas_data['Responded'], resp_type='LA', scenario=current_scenario)
returns_summary(pandas_data['hh_record'], pandas_data['Responded'], resp_type='LA', scenario=current_scenario)

pandas_data = csv_to_pandas(output_path, ['hh_record', 'Responded', 'key info'])
df1_loc = pandas_data['hh_record']
df2_loc = pandas_data['Responded']

# produce comparison of final results
# pss location of dataframes - possibly use this method for all analysis - so update "produce return charts" code???
# passive option should have the same data passed for each entry eg...
pyramid([df1_loc, df1_loc, 'passive', True], [df2_loc, df1_loc, 'active', False],  bins=[60, 102, 2])

pandas_data = csv_to_pandas(output_path, ['hh_record', 'Responded', 'key info', 'Return_sent'])
df0_loc = pandas_data['key info']
df1_loc = pandas_data['hh_record']
df2_loc = pandas_data['Return_sent']

#produce return chart over time  - pass df of data to use....
produce_return_charts(df2_loc, df1_loc, 'Active', 'Passive', df0_loc, ' returns ' + current_scenario + '.html', filter_type='LA')
"""


######################

def sum_hh(hh_record):
    """ takes hh_record data and returns a simple sum of the households present"""

    hh_count = 0

    n = len(hh_record)  # number of districts

    for i in range(1, n + 1):
        hh_count_df = hh_record[str(i)]
        # for count use only rep 1 figures
        hh_count += len(hh_count_df[hh_count_df['rep'] == 1])
        print(hh_count)

    return hh_count


def user_journey_single():
    """a function that allows the user to extract an individual user journey from the raw outputs only. Simply searches
     the csv output files that exists and prints a sorted list (by time) of events that household experienced. Primarily
      used for debugging."""

    user_list = []
    output_path = "outputs"
    hh_id = input('Enter hh to extract: ')

    folder_list = glob.glob(output_path + '/*/')  # create list of folders at output path

    for folder in folder_list:
        folder_name = folder.split(os.path.sep)[-2]

        glob_folder = os.path.join('outputs', folder_name, '*.csv')
        file_list = glob.glob(glob_folder)  # get a list of all files in the folder

        try:
            for file in file_list:
                file_name = file.split(os.path.sep)[-1][0]

                full_path = os.path.join(output_path, folder_name)
                full_path = full_path + "/" + file_name + ".csv"

                with open(full_path, 'r') as f:
                    reader = csv.DictReader(f)
                    rows = [row for row in reader if row['hh_id'] == hh_id]

                for row in rows:
                    # append folder_name tag
                    row['event'] = folder_name
                    user_list.append(row)

        except:
            pass

    listsorted = sorted(user_list, key=lambda x: float(x['time']))

    for row in listsorted:
        print(row)


def csv_to_pandas(output_path, output_type):
    """converts selected (raw) csv output files to a dictionary of dictionaries of Pandas data frames for each output
    type and run. Output_type is a list of the types of output to include (e.g. responses, visits). To refer to the
    finished data_dict use: data_dict['type of output']['run']. Data frame will be every replication of that output
    type for that run"""

    folder_list = glob.glob(output_path + '/*/')  # create list of folders at output path
    data_dict = defaultdict(list)  # create a dict ready for the data frames

    for folder in folder_list:
        folder_name = folder.split(os.path.sep)[-2]
        if folder_name in output_type:

            glob_folder = os.path.join(output_path, folder_name, '*.csv')
            file_list = glob.glob(glob_folder)  # get a list of all files(sim runs) in the folder

            data_dict[folder_name] = defaultdict(list)

            # for each file (sim run) add to dictionary for that type of output
            for file in file_list:
                file_name = file.split(os.path.sep)[-1][:-4]
                data_dict[folder_name][file_name] = pd.read_csv(file, header=0)  # add each run to dict
    return data_dict


def produce_rep_results(current_path):
    """combines the summary files producing csv files of the results for all districts for each rep and the average
    when all reps are combined. these can then be plotted separately (distribution), combined or both"""

    # go through the folders equal times for number of reps run
    # for each folder that is at district level pick out relevant rep

    #current_path = os.path.join(current_path, 'summary')

    folders = list(os.walk(current_path))  # a list of all the folders in the summary results
    for folder in folders:
        os.chdir(folder[0])

        if glob.glob('*.csv'):  # search each folder for csv files
            file_list = glob.glob('*.csv')
            total_reps = len(file_list)
            os.chdir("..")  # when csv files found step up a level
            districts = len([direc for direc in os.listdir(os.getcwd()) if os.path.isdir(direc)])
            # then for number of reps pull out relavent rep for each district and add together
            df_totals = pd.DataFrame()
            for rep in range(1, total_reps+1):
                # could have a dict with number of dataframes equal to reps and go through only once...
                # e.g. add all reps to dict with df before moving on...
                df = pd.DataFrame()
                for district in range(1, districts+1):
                    df_to_add = pd.read_csv(os.path.join(str(district), str(rep) + ".csv"),  index_col=0)
                    np.fromfile(os.path.join(str(district), str(rep) + ".csv"))
                    os.remove(os.path.join(str(district), str(rep) + ".csv"))
                    df = df.add(df_to_add, fill_value=0)

                df_totals = df_totals.add(df, fill_value=0)  # add to overall record (all districts combined)
                if not os.path.isdir('reps_combined'):
                    os.makedirs('reps_combined')
                df.to_csv(os.path.join('reps_combined', 'rep_' + str(rep) + ".csv"))

            df_totals /= total_reps
            df_totals.to_csv('average.csv')

    # clean up empty folders
    for dirpath, dirnames, files in os.walk(current_path):
        if not dirnames and not files:
            os.rmdir(dirpath)

    # and change the working directory back to the start directory
    os.chdir(current_path)


def plot_summary(summary_path, summary_outpath, output_name, reps=False, average=True, cumulative=True, individual=False,
                 percent=0):
    """creates a plot using the summary data to show response over time. default is to show the average of all areas.
    If rep is True then it will also plot the individual rep results (faded). If individual is True it will plot the
    average response over time for each group in the dataset rather than the whole. If cumulative is false it will
    show daily return rates rather than cumulative."""

    if average:
        df = pd.read_csv(os.path.join(summary_path, 'average.csv'), index_col=0)
        if cumulative:
            df = df.sum(axis=0).cumsum(axis=0)
        else:
            df = df.sum(axis=0)

        if percent > 0:
            df = df.div(percent)*100

        df.rename = 'average'
        df.plot.line(label='total')

    if reps:
        # for each rep
        file_list = glob.glob(os.path.join(summary_path, 'reps_combined', '*.csv'))
        for file in file_list:
            df = pd.read_csv(file, index_col=0)
            if cumulative:
                df = df.sum(axis=0).cumsum(axis=0)
            else:
                df = df.sum(axis=0)

            if percent > 0:
                df = df.div(percent) * 100

            df.rename = 'reps'
            df.plot.line(alpha=0.1, color='blue')

    if individual:
        df = pd.read_csv(os.path.join(summary_path, 'average.csv'), index_col=0)
        df.sort_index(axis=0, ascending=True, inplace=True)
        if cumulative:
            df = df.cumsum(axis=1)

        # plot each row in succession
        for index, row in df.iterrows():
            row.plot.line(label=index)

    filename = output_name + '.png'
    output_path = os.path.join(summary_outpath, filename)
    if not reps:
        plt.legend(loc='best')
    plt.xlabel('days')
    if percent:
        plt.ylabel('Percent')
    else:
        plt.ylabel('Count')
    plt.savefig(output_path, dpi=450)
    plt.close()


def sum_pyramid(hh_record, summary_outpath, input_data_left, input_data_right, name_left, name_right, bin_size=5):
    """takes lsoa response totals for two strategies and produces a pyramid chart showing the number of lsoas within
    user defined bins."""

    bins = range(0, 100 + bin_size, bin_size)

    # use hh_record to get total hh in each lsoa
    n = len(hh_record)  # number of districts
    num_hh_lsoa = pd.Series()  # lsoa counts

    for i in range(1, n + 1):
        hh_count = hh_record[str(i)]
        # for count use only rep 1 figures

        hh_count = hh_count[hh_count['rep'] == 1]
        hh_count = hh_count.groupby('lsoa11cd').size()
        num_hh_lsoa = num_hh_lsoa.add(hh_count, fill_value=0)

    print('Overall response rate is: ', input_data_right['0'].sum()/num_hh_lsoa.sum())

    # divide returns by number of HH for each area and convert to percentage
    input_data_left = input_data_left.div(num_hh_lsoa, axis='index') * 100
    input_data_right = input_data_right.div(num_hh_lsoa, axis='index') * 100

    # bin the data
    input_data_left = pd.cut(input_data_left[input_data_left.columns[0]], bins)
    x1 = pd.value_counts(input_data_left)
    x1 = x1.reindex(input_data_left.cat.categories)  # sort the cats

    input_data_right = pd.cut(input_data_right[input_data_right.columns[0]], bins)
    x2 = pd.value_counts(input_data_right)
    x2 = x2.reindex(input_data_right.cat.categories)  # sort the cats

    # determine x and y limits to use in plots
    x_max = max(x1.max(), x2.max())
    y_min = min(int(x1[x1 > 0].index[0][1:3]), int(x2[x2 > 0].index[0][1:3]))

    if x1[x1 > 0].index[0][1:3] < x2[x2 > 0].index[0][1:3]:
        x_pos_from = x1[x1 > 0].index[0]
    else:
        x_pos_from = x2[x2 > 0].index[0]

    x1 = x1[x_pos_from:]
    x2 = x2[x_pos_from:]

    y = np.arange(y_min, 100, bin_size)

    # and plot
    width = 1

    fig, axes = plt.subplots(ncols=2, sharey=True)
    axes[0].barh(y, x1, width, align='center', color='green', zorder=10)
    axes[0].set(title=name_left)
    axes[0].set_xlim([0, x_max])
    axes[1].barh(y, x2, width, align='center', color='blue', zorder=10)
    axes[1].set(title=name_right)
    axes[1].set_xlim([0, x_max])
    axes[0].set_xlabel("count")
    axes[1].set_xlabel("count")

    axes[0].invert_xaxis()
    axes[0].set(yticks=y, yticklabels=y)
    axes[0].yaxis.tick_right()
    axes[0].set_ylabel("Response rate")

    for ax in axes.flat:
        ax.margins(0.03)
        ax.grid(True)

    fig.tight_layout()
    fig.subplots_adjust(wspace=0.09)
    filename = name_left + ' vs ' + name_right + '.png'  # add default title or user defined
    output_path = os.path.join(summary_outpath, filename)
    plt.savefig(output_path, dpi=450)


def bokeh_line_chart(input1, input2, label1, label2, output_path, filename, cumulative=False):
    """take two series objects to create a bokeh line chart with tooltips"""

    if cumulative:

        input1_df = pd.read_csv(input1, index_col=0).sum(axis=0).cumsum(axis=0)
        input2_df = pd.read_csv(input2, index_col=0).sum(axis=0).cumsum(axis=0)

    else:

        input1_df = pd.read_csv(input1, index_col=0).sum(axis=0)
        input2_df = pd.read_csv(input2, index_col=0).sum(axis=0)

    label1 = label1.lower()
    label2 = label2.lower()

    label1_str = label1.capitalize()
    label2_str = label2.capitalize()

    combined = pd.concat([input1_df, input2_df], axis=1)
    combined.reset_index(level=0, inplace=True)
    # ensure values are same precision

    combined.columns = ['day', label1, label2]

    max_y = roundup(max(combined[[label1, label2]].max(axis=0)), 100)

    source = ColumnDataSource(combined)

    tools = 'box_zoom,' \
            'crosshair,' \
            'resize,' \
            'reset,' \
            'pan,' \
            'wheel_zoom,' \
            'save'

    # Add lines to the figure p
    p = figure(x_axis_label='Day', y_axis_label='Returns', width=1600, height=800, tools=tools, responsive=True)
    p.line(x='day', y=label1, source=source, color='green', legend=label1_str)
    p.line(x='day', y=label2, source=source, color='blue', legend=label2_str)

    fu_day = (dt.datetime.strptime('2017-04-19', '%Y-%m-%d').date() -
              dt.datetime.strptime('2017-03-22', '%Y-%m-%d').date()).days

    p.line([fu_day, fu_day], [0, max_y], color='red')

    hover = HoverTool(tooltips=
                      [(label1_str, '@' + label1 + '{int}'),
                       (label2_str, '@' + label2 + '{int}'),
                       ('Day', '@day')
                       ])

    p.add_tools(hover)

    # do some formatting
    p.xaxis.major_label_orientation = math.pi / 4
    p.xgrid[0].ticker.desired_num_ticks = 11
    p.xaxis.axis_label_text_font_size = '12pt'
    p.xaxis.major_label_text_font_size = '12pt'

    p.yaxis.axis_label_text_font_size = '12pt'
    p.yaxis.major_label_text_font_size = '12pt'

    p.legend.label_text_font_size = '12pt'
    p.legend.location = 'top_left'
    title_text = 'Effect of interventions on ' + filename[:-5]
    p.title.text = title_text

    # Specify the name of the output file and show the result
    output_path = os.path.join(output_path, filename)
    output_file(output_path)
    save(p)


def visit_effectiveness(df_visits, df_visits_success, group='E&W'):
    """a function that returns the effectiveness of subsequent rounds of visits. df_visits is the location of the
    dataframes with the data for each district (user defined collection of areas simulated)"""

    df_list = [df_visits, df_visits_success]

    totals = pd.DataFrame()

    for df_set in df_list:

        overall_totals = pd.DataFrame()

        for key, value in df_set.items():

            if group == 'E&W':
                district_sum = value.groupby(['visits', 'rep'])['visits'].size().reset_index()
                district_sum.rename(columns={0: 'count'}, inplace=True)
                district_sum = district_sum.groupby(['visits'])['count'].mean().reset_index()
            else:
                district_sum = value.groupby([group, 'visits', 'rep'])['visits'].size().reset_index()
                district_sum.rename(columns={0: 'count'}, inplace=True)
                district_sum = district_sum.groupby([group, 'visits'])['count'].mean().reset_index()

            if overall_totals.empty:
                overall_totals = overall_totals.append(district_sum)
            else:
                if group == 'E&W':
                    overall_totals = pd.merge(overall_totals, district_sum, how='outer', on=['visits'])
                    overall_totals['count'] = overall_totals.count_x.fillna(0) + overall_totals.count_y.fillna(0)

                else:
                    overall_totals = pd.merge(overall_totals, district_sum, how='outer', on=[group, 'visits'])
                    overall_totals['count'] = overall_totals.count_x.fillna(0) + overall_totals.count_y.fillna(0)

                if group == 'E&W':
                    cols = [1, 2]
                else:
                    cols = [2, 3]

                overall_totals.drop(overall_totals.columns[cols], axis=1, inplace=True)

        if totals.empty:
            totals = totals.append(overall_totals)
        else:
            if group == 'E&W':
                totals = pd.merge(totals, overall_totals, how='outer', on=['visits'])
            else:
                totals = pd.merge(totals, overall_totals, how='outer', on=[group, 'visits'])

    totals['percent_success'] = totals['count_y']/totals['count_x']
    print(totals)  # this is count of all visits across all groups (la's)
    print(totals['count_x'].sum(), totals['count_y'].sum(), totals['count_y'].sum()/totals['count_x'].sum(), )


def count_reminders(df_list):
    """simple returns by type the number of reminders sent"""

    total_reminders = 0

    for df_set in df_list:

        reminder_count = 0

        for key, value in df_set.items():
            reminder_count += df_set[key].shape[0]

        print(reminder_count)
        total_reminders += reminder_count

    print("total: ", total_reminders)


def rep_dist(input_path):
    # function to produce graphical representation of the spread of results and SD
    # simply sums the contents of sumamry files and adds to list
    file_list = glob.glob(os.path.join(input_path, 'reps_combined', '*.csv'))
    results_list = []

    for file in file_list:
        df = pd.read_csv(file, index_col=0)
        total = df.values.sum()

        results_list.append(total)

    print(np.mean(results_list), np.std(results_list))
    # once have a list of the results plot, produce SD and...
    plt.hist(results_list)
    plt.show()


def visit_unnecessary(df_visits, df_visit_unnecessary, group='E&W'):
    """a function that returns the number of visits that were not required by hh_type for the whole simulation)"""

    df_list = [df_visits, df_visit_unnecessary]

    totals = pd.DataFrame()

    for df_set in df_list:

        overall_totals = pd.DataFrame()

        for key, value in df_set.items():

            district_sum = value.groupby(['hh_type', 'rep'])['hh_id'].size().reset_index()
            district_sum.rename(columns={0: 'count'}, inplace=True)
            district_sum = district_sum.groupby(['hh_type'])['count'].mean().reset_index()

            if overall_totals.empty:
                overall_totals = overall_totals.append(district_sum)
            else:
                overall_totals = pd.merge(overall_totals, district_sum, how='outer', on=['hh_type'])
                overall_totals['count'] = overall_totals.count_x.fillna(0) + overall_totals.count_y.fillna(0)

                overall_totals.drop(overall_totals.columns[[1, 2]], axis=1, inplace=True)

        if totals.empty:
            totals = totals.append(overall_totals)
        else:
            totals = pd.merge(totals, overall_totals, how='outer', on=['hh_type'])

    totals['percent_success'] = (totals['count_y'] / totals['count_x'])*100
    print(totals)  # this is count of all visits across all groups (la's)
    print(totals['count_x'].sum(), totals['count_y'].sum(), totals['count_y'].sum() / totals['count_x'].sum(), )


#response_data = os.path.join(os.getcwd(), 'outputs', 'C2EO300 2017-12-12 15.57.25', 'summary', 'active_summary', 'la' )
#rep_dist(response_data)

#input_path = os.path.join(os.getcwd(), 'outputs', 'C1EO331D10_C1SO331D10 2017-12-19 16.02.53')
#pandas_data = csv_to_pandas(input_path, ['reminder_sent', 'reminder2_sent', 'IAC_rem_sent'])
#count_reminders([pandas_data['reminder_sent'], pandas_data['reminder2_sent'], pandas_data['IAC_rem_sent']])


#left_current_path = os.path.join(os.getcwd(), 'outputs', 'baseline 2017-09-21 14.00.28')
#right_current_path = os.path.join(os.getcwd(), 'outputs', 'digital first 2017-09-21 22.11.03')

#### change to allow display in % terms, just supply total to divide by


#input_path = os.path.join(os.getcwd(), 'outputs', 'C1EO331D10_C1SO331D10 2017-12-19 16.52.41')
#pandas_data = csv_to_pandas(input_path, ['hh_record', 'Visit', 'Visit_unnecessary'])
#visit_unnecessary(pandas_data['Visit'], pandas_data['Visit_unnecessary'])


#input_path = os.path.join(os.getcwd(), 'outputs', 'C1EO331D4_C1SO331D4 2017-12-19 16.02.13')
#pandas_data = csv_to_pandas(input_path, ['hh_record', 'Visit', 'Visit_success'])
#visit_effectiveness(pandas_data['Visit'], pandas_data['Visit_success'])

#input_path = os.path.join(os.getcwd(), 'outputs', 'C2EO300 2017-11-29 13.48.01')
#pandas_data = csv_to_pandas(input_path, ['hh_record'])
#default_path = os.path.join(os.getcwd(), 'outputs', 'C2EO300 2017-11-29 13.48.01', 'summary', 'active_summary', 'la')
#summary_outpath = os.path.join(input_path, 'summary')
#percent = sum_hh(pandas_data['hh_record'])
#plot_summary(default_path, summary_outpath, 'returns', average=True, reps=True, cumulative=True, percent=percent)

#input_path = os.path.join(os.getcwd(), 'outputs', 'lsoa_nomis_12 2017-08-17 13.06.10')
#pandas_data = csv_to_pandas(left_current_path, ['hh_record'])

#input_left = pd.read_csv(os.path.join(left_current_path, 'summary', 'active_totals', 'lsoa', 'average.csv'), index_col=0)
#name_left = 'Baseline'
#input_right = pd.read_csv(os.path.join(right_current_path, 'summary', 'active_totals', 'lsoa', 'average.csv'), index_col=0)
#name_right = 'Digital first'


#sum_pyramid(pandas_data['hh_record'], os.path.join(left_current_path, 'summary'), input_left, input_right, name_left,
#            name_right, bin_size=1)


