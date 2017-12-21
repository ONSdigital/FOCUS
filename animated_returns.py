"""a new module with a specific purpose of creating animated return charts that show how the return rate reported
is made up of a large number of replications that each differ"""
import matplotlib.pyplot as plt
from matplotlib import animation
import os
import glob
import pandas as pd
import post_process as pp


def plot_summary_animated(summary_path, reps=True, average=True, cumulative=True, actual=False, percent=0):
    """creates a plot using the summary data to show response over time. default is to show the average only.
    if rep is True then it will also plot the individual reps results (faded). If cumulative is false it will
    show the daily return rates. """

    plot_list = []

    if actual:
        # if a path is passed also display the raw data on the chart

        df = pd.read_csv(actual, index_col=1)
        df.drop('Date', axis=1, inplace=True)

        if cumulative:
            df = df.sum(axis=1).cumsum(axis=0)
        else:
            df = df.sum(axis=1)

        if percent > 0:
            df = df.div(percent) * 100

        df.rename = 'actual'
        plot_list.append(df)

    if average:
        df = pd.read_csv(os.path.join(summary_path, 'average.csv'), index_col=0)
        if cumulative:
            df = df.sum(axis=0).cumsum(axis=0)
        else:
            df = df.sum(axis=0)

        if percent > 0:
            df = df.div(percent) * 100

        df.rename = 'average'
        plot_list.append(df)

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
            plot_list.append(df)

    return plot_list


def plotlist(n):
    # return the nth list...
    return data_list[n]


input_folder = 'C2EO300 2017-12-21 10.31.46'

print(os.getcwd())
default_path = os.path.join('outputs', input_folder, 'summary', 'active_summary', 'la')
input_path = os.path.join(os.getcwd(), 'outputs', input_folder)
raw_data_path = os.path.join(os.getcwd(), 'raw_inputs', 'summary_returns_2017.csv')
pandas_data = pp.csv_to_pandas(input_path, ['hh_record'])
percent = pp.sum_hh(pandas_data['hh_record'])
print(percent)
data_list = plot_summary_animated(default_path, cumulative=False, reps=True, actual=raw_data_path, percent=percent)

fig = plt.figure()

font_size = 24


n = len(data_list)  # Number of frames
ymax = pp.roundup(max([max(lst) for lst in data_list]), 3)


def init():
    plt.gcf().clear()
    plt.gca().set_ylim(top=ymax)
    data_list[0].plot.line(color='green')
    data_list[1].plot.line(color='red')


def animate(i):
    plt.gca().set_ylim(top=ymax)
    plotlist(i).plot.line(alpha=0.1, color='blue')
    fig.suptitle('Overall return rates over time', fontsize=font_size)
    plt.xlabel('Day', fontsize=font_size)
    plt.ylabel('Returns', fontsize=font_size)
    plt.rcParams['xtick.labelsize'] = font_size
    plt.rcParams['ytick.labelsize'] = font_size

anim = animation.FuncAnimation(plt.gcf(), animate, init_func=init,  repeat=True, blit=False, frames=n,
                               interval=500)

anim.save('C2EO300_daily_anim.mp4', dpi=280)

figManager = plt.get_current_fig_manager()
figManager.window.showMaximized()

plt.show()
