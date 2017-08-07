"""a new module with a specific purpose of creating animated return charts that show how the return rate reported
is made up of a large number of replications that each differ"""
import matplotlib.pyplot as plt
from matplotlib import animation
import os
import glob
import pandas as pd
import post_process as pp


def plot_summary_animated(summary_path, reps=True, average=True, cumulative=True):
    """creates a plot using the summary data to show response over time. default is to show the average only.
    if rep is True then it will also plot the individual reps results (faded). If cumulative is false it will
    show the daily return rates. """

    plot_list = []

    if average:
        df = pd.read_csv(os.path.join(summary_path, 'average.csv'), index_col=0)
        if cumulative:
            df = df.sum(axis=0).cumsum(axis=0)
        else:
            df = df.sum(axis=0)
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
            df.rename = 'reps'
            plot_list.append(df)

    return plot_list


def plotlist(n):
    # return the nth list...
    return data_list[n]

print(os.getcwd())
default_path = os.path.join('outputs', 'lsoa_nomis_10 2017-08-07 13.22.38', 'summary', 'active_summary', 'la')
data_list = plot_summary_animated(default_path, cumulative=False, reps=True)

fig = plt.figure()
fig.suptitle('Overall return rates over time', fontsize=20)
plt.xlabel('Day', fontsize=16)
plt.ylabel('returns', fontsize=16)

n = len(data_list)  # Number of frames
ymax = pp.roundup(max([max(lst) for lst in data_list]), 200)


def init():
    plt.gcf().clear()
    plt.gca().set_ylim(top=ymax)
    data_list[0].plot.line(color='red')


def animate(i):
    plt.gca().set_ylim(top=ymax)
    plotlist(i).plot.line(alpha=0.1, color='blue')


anim = animation.FuncAnimation(plt.gcf(), animate, init_func=init,  repeat=True, blit=False, frames=n,
                               interval=500)

figManager = plt.get_current_fig_manager()
figManager.window.showMaximized()

plt.show()
