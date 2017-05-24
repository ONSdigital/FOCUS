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


def user_journey_single():
    """a function that allows the user to extract an individual user journey. Simply searches the csv output files that
     exists and prints a sorted list (by time) of events that household experienced"""

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


def user_journey_all():
    """a summary of all user journeys?"""


def csv_to_pandas(output_path, output_type):
    """converts selected csv output files to a dictionary of dictionaries of Pandas data frames for each output type
    and run. Output_type is a list of the types of output to include (e.g. responses, visits). To refer to the finished
    data_dict use: data_dict['type of output']['run']. Data frame will be every replication of that output type for
    that run"""

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

            filename_temp = 'ed ' + str(district) + " " + filename
            combined_chart(series1, series2, label1, label2, filename_temp)


def response_rate(df1, df2, bins, filter_type='LSOA', passive=False):
    """returns a pandas series object of the number of each filter type in each bin"""

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

    list_of_ed = s1[0].keys()

    for ed in list_of_ed:
        input1 = response_rate(s1[0][ed], s1[1][ed], bins, passive=s1[3])
        input2 = response_rate(s2[0][ed], s2[1][ed], bins, passive=s2[3])
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
    cumulative_returns_per.to_csv(os.path.join(os.getcwd(), 'summary results', resp_type + " returns summary scenario " +
                                               scenario + ".csv"))

    # also need an E+W average for each
    overall_returns = int_df.sum(axis=0)
    total_hh = int_hh_s.sum(axis=0)
    average_returns = (overall_returns / total_hh) * 100
    average_returns = pd.DataFrame(average_returns).T
    average_returns.to_csv(os.path.join(os.getcwd(), 'summary results', resp_type + " average returns " +
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
    summary_df.to_csv(os.path.join(os.getcwd(), 'summary results', 'summary results for ' + data_types[0] + ".csv"))


def combine_csv(input_path):
    """used to combine csv files into to a single data frame for later analysis"""
    temp_df = pd.DataFrame()

    glob_folder = os.path.join(input_path, '*.csv')
    file_list = glob.glob(glob_folder)  # get a list of all files in the folder

    for file in file_list:
        temp_df = temp_df.add(pd.read_csv(file, index_col=0), fill_value=0)

    return temp_df


#active_summary_path = os.path.join(os.getcwd(), 'charts', 'active summary', 'la')
#returns_df = combine_csv(active_summary_path)
#print(returns_df)

"""

output_path = os.path.join(os.getcwd(), 'outputs', '2017-05-17 16.24.26')
current_scenario = output_path.split('/')[-1]

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

#returns_summary(pandas_data['hh_record'], pandas_data['Responded'], geog='LA', scenario=current_scenario)
#returns_summary(pandas_data['hh_record'], pandas_data['Responded'], resp_type='LA', scenario=current_scenario)
#returns_summary(pandas_data['hh_record'], pandas_data['Responded'], resp_type='LA', scenario=current_scenario)

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
produce_return_charts(df2_loc, df1_loc, 'Active', 'Passive', df0_loc, ' returns ' + current_scenario + '.html',
                      filter_type='hh_type')
"""
