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
    all_days.append(int(row[1]))
    all_returns.append(float(row[2]))

days = np.array(all_days)
responses = np.array(all_returns)

# create a probability distribution -  based on 0% red
prob = responses / float(sum(responses))
cum_prob = np.cumsum(prob)

# based on a 10% reduction in the FU period construct new dist
# sum up probs in latter half and work out prop drop - do drop - and apply

response_times = []

# for each hh generate day from dist
for i in range(1000):
    R = ra.uniform(0, 1, 1)
    response_times.append(int(days[np.argwhere(cum_prob == min(cum_prob[(cum_prob - R) > 0]))]))


gen_days = (np.array(response_times)) .astype(int)
for day in gen_days:
    if day < 0:
        print(day)

times = np.arange(1, len(all_returns)+1, 1)
lc = np.bincount(gen_days, minlength=len(times))


plot1, = plt.plot(lc/float(sum(lc)), 'r--', label='sampled responses')
plot2, = plt.plot(prob, 'g', label='Original responses')
plt.xlabel('days')
plt.ylabel('Probability')
plt.legend(handles=[plot1, plot2])
plt.show()