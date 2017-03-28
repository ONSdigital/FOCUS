import numpy as np
from matplotlib import pyplot as plt
import numpy.random as ra
import csv
import os


#read in csv data
input_path = os.path.join(os.getcwd(), 'summary_returns.csv')

with open(input_path, 'r') as f:
    reader = csv.reader(f)
    next(reader)

    raw_data = list(reader)

# convert to format ready for numpy arrays
all_days = []
all_returns = []

for row in raw_data:
    all_days.append(int(float(row[1])))
    all_returns.append(int(float(row[2])))

days = np.array(all_days)
responses = np.array(all_returns)

Cen_day = 20  # update to calculate from input dates

all_days_alt = []
all_returns_alt = []

# create a probability distribution -  based on 0% red
prob = responses / float(sum(responses))
cum_prob = np.cumsum(prob)

for row in raw_data:
    all_days_alt.append(int(float(row[1])))
    all_returns_alt.append(int(float(row[2])))
    if raw_data.index(row) > Cen_day:
        amount_over = raw_data.index(row) - Cen_day
        reduction = amount_over/(len(raw_data) - Cen_day)
        all_returns_alt[-1] *= (1-reduction)

days_alt = np.array(all_days_alt)
responses_alt = np.array(all_returns_alt)

# create a probability distribution -  based on 0% red
prob_alt = responses_alt / float(sum(responses_alt))
cum_prob_alt = np.cumsum(prob_alt)

response_times_alt = []
   # for each hh generate day from dist
for i in range(10000):
    R = ra.uniform(0, 1, 1)
    response_times_alt.append(int(days_alt[np.argwhere(cum_prob_alt == min(cum_prob_alt[(cum_prob_alt - R) > 0]))]))

gen_days = (np.array(response_times_alt)) .astype(int)
for day in gen_days:
    if day < 0:
        print(day)

times = np.arange(1, len(all_returns_alt)+1, 1)
lc = np.bincount(gen_days, minlength=len(times))

plot1, = plt.plot(lc/float(sum(lc)), 'r--', label='sampled responses')
plot2, = plt.plot(prob, 'g', label='Original responses')
plot3, = plt.plot(prob_alt, 'b', label='alt responses')
plt.xlabel('days')
plt.ylabel('Probability')
plt.legend(handles=[plot1, plot2, plot3])
plt.show()