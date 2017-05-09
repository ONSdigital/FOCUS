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
import dask
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

            glob_folder = os.path.join('outputs', folder_name, '*.csv')
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
    row_names = list(df[geog].unique())

    # filter df to only have correct entries
    if resp_type == 'digital':
        df = df[df['digital'] == True].copy()

    elif resp_type == 'paper':
        df = df[df['digital'] == False].copy()

    # add a new column to passed data frame containing the categories the entry belongs too
    df['categories'] = pd.cut(df['time'], bin_values, labels=group_names)
    # group by each combination of district and category (and rep?) and count the number of each category
    cat_sum = df.groupby([geog, 'categories', 'rep'])['categories'].size()
    # calculate the cum sum of the totals
    cat_sum = cat_sum.groupby(level=[0, 2]).cumsum().reset_index()
    cat_sum.rename(columns={0: 'count'}, inplace=True)
    cat_sum = cat_sum.groupby([geog, 'categories'])['count'].mean().reset_index()

    # pivot it so the categories become the columns
    cat_sum_flipped = cat_sum.pivot(index=geog, columns='categories', values='count')

    # and then add back in any missing categories and fill the gaps
    cat_sum_flipped = cat_sum_flipped.reindex(columns=group_names).ffill(axis=1).replace('Nan', 0, regex=True)
    cat_sum_flipped = cat_sum_flipped.reindex(index=geog, columns=group_names).ffill(axis=0).replace('Nan', 0, regex=True)

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
            input_df = input_df.drop(input_df[(input_df.action == item)].index)

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


# for given level of geog to use
def produce_return_charts(df1, df2, label1, label2, start_date, filename, filter_type='E&W'):
    """Produces for a given strategy (run) the difference between the passive and the active. ie, the self
    response and response with simulated interventions"""

    if filter_type == 'E&W':
        # just use the whole df

        series1 = bin_records(df1, start_date)
        series2 = bin_records(df2, start_date, ['do_nothing', 'help'])
        filename = filter_type + filename
        combined_chart(series1, series2, label1, label2, filename)

    else:
        # produce for each area
        for district in df1[filter_type].unique():
            # extract just the current LA from the df and pass that to the functions
            filename_temp = district + filename
            df1_temp = df1[df1[filter_type] == district].copy()
            df2_temp = df2[df2[filter_type] == district].copy()
            series1 = bin_records(df2_temp, start_date, ['do_nothing', 'help'])
            series2 = bin_records(df1_temp, start_date)
            combined_chart(series1, series2, label1, label2, filename_temp)


def response_rate(df1, df2, bins, filter_type='LSOA', passive=False):
    """returns a pandas series object of the number of each filter type in each bin"""

    bins = np.arange(bins[0], bins[1], bins[2])

    if passive:
        # drop the do nothings and helps
        drop_list = ['do_nothing', 'help']

        for item in drop_list:
            df1 = df1.drop(df1[(df1.action == item)].index).copy()

        print('passive items dropped')

    # do the below for each rep...
    counts_list = []
    reps = df1['rep'].max()

    for i in range(1, reps+1):

        output_list = []

        # for each rep and LSOA or other...
        for item in df1[filter_type].unique():
            len_df1 = len(df1[(df1[filter_type] == item) & (df1['rep'] == i)])
            len_df2 = len(df2[(df2[filter_type] == item) & (df2['rep'] == i)])
            print(item)

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


def waterfall(s1, s2, bins):
    """ produces parallel horizontal bar charts that display the distribution of response rates achieved from two
     strategies. s1, s2 are lists that contain the data frames to be used for each output(strategy) as well as the
     name of the strategy and whether it is the special case of the passive option"""

    input1 = response_rate(s1[0], s1[1], bins, passive=s1[3])
    input2 = response_rate(s2[0], s2[1], bins, passive=s2[3])

    combined_list = pd.concat([input1, input2], axis=1)
    combined_list.reset_index(level=0, inplace=True)
    combined_list.columns = ['category', s1[2], s2[2]]

    x1 = combined_list[s1[2]].tolist()
    x2 = combined_list[s2[2]].tolist()
    y = np.arange(bins[0], bins[1]-bins[2], bins[2])

    width = 3

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


def returns_summary(hh_record_df, returns_df,  geog='LA', resp_type='all'):
    """returns at the passed level the overall returns by day including averages for E&W. Used for
     producing data in the correct format for the data vis team map"""

    # gets list of runs
    runs = sorted(list(hh_record_df.keys()))

    for current_run in runs:

        # calculate the total number of households in each area and in total -  same for each rep so use rep 1
        hh_record_df = hh_record_df[str(current_run)]
        hh_record_df = hh_record_df[hh_record_df['rep'] == 1].copy()
        hh_count = hh_record_df.groupby(geog).size()  # hh per area
        hh_totals = hh_count.sum()  # total of households

        # produce cumulative summary of overall returns
        cumulative_returns = cumulative_sum(returns_df[str(current_run)], 0, 1824, 24, geog, resp_type)
        hh_count.index = cumulative_returns.index
        cumulative_returns_per = cumulative_returns.div(hh_count, axis='index')
        cumulative_returns_per.to_csv(os.path.join(os.getcwd(), 'summary results', resp_type + " returns summary run " +
                                                   current_run + ".csv"))

        # also need an E+W average for each
        overall_returns = cumulative_returns.sum(axis=0)
        average_returns = (overall_returns / hh_totals) * 100
        average_returns = pd.DataFrame(average_returns).T
        average_returns.to_csv(os.path.join(os.getcwd(), 'summary results', resp_type + " average returns run " +
                                            current_run + ".csv"))



output_path = os.path.join(os.getcwd(), 'outputs')
pandas_data = csv_to_pandas(output_path, ['Return_sent', 'hh_record', 'Responded', 'key info'])

#returns_summary(pandas_data['hh_record'], pandas_data['Responded'])
#returns_summary(pandas_data['hh_record'], pandas_data['Responded'], resp_type='paper')
#returns_summary(pandas_data['hh_record'], pandas_data['Responded'], resp_type='digital')

# do we always want to select this data frame - yes for the default output
glob_folder = os.path.join('outputs', 'hh_record', '*.csv')
file_list = glob.glob(glob_folder)  # get a list of all files in the folder

default_key = str(file_list[0].split(os.path.sep)[-1])[:-4]
df1 = pandas_data['Return_sent'][default_key]
df2 = pandas_data['hh_record'][default_key]
start_date = pandas_data['key info'][default_key].start_date[0]
start_date = dt.date(*map(int, start_date.split('-')))

# produce return chart over time
#produce_return_charts(df1, df2, 'Active', 'Passive', start_date, ' returns ' + default_key + '.html')

# example of how to produce a second chart based on next run - can also be used as second strategy in waterfall
# df3 = pandas_data['Return_sent']['2']
# df4 = pandas_data['hh_record']['2']
# post_process.produce_return_charts(df3, df4, '  returns run 2.html')

# produce comparison of final results
waterfall([df2, df2, 'passive', True], [df1, df2, 'active', False], bins=[65, 105, 5])
