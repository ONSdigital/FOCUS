import csv
import warnings
import numpy as np
import pandas as pd
import scipy.stats as st
import matplotlib
import matplotlib.pyplot as plt
import random

matplotlib.rcParams['figure.figsize'] = (16.0, 12.0)
plt.style.use('ggplot')


# Create models from data
def best_fit_distribution(data, bins=200, ax=None):
    """Model data by finding best fit distribution to data"""
    # Get histogram of original data
    y, x = np.histogram(data, bins=bins, normed=True)
    print('hist done')
    x = (x + np.roll(x, -1))[:-1] / 2.0

    # Distributions to check
    distributions = [
        st.norm,
        st.betaprime,
        st.weibull_min,
        st.uniform
    ]

    distributions2 = [
        st.alpha,
        st.betaprime,
        st.bradford,
        st.burr,
        st.chi,
        st.chi2,
        st.cosine,
        st.dgamma,
        st.dweibull,
        st.erlang,
        st.expon,
        st.exponnorm,
        st.exponweib,
        st.f,
        st.fisk,
        st.foldcauchy,
        st.foldnorm,
        st.genpareto,
        st.gennorm,
        st.genexpon,
        st.genextreme,
        st.gamma,
        st.gengamma,
        st.gilbrat,
        st.gompertz,
        st.halfnorm,
        st.halfgennorm,
        st.hypsecant,
        st.invgamma,
        st.invgauss,
        st.invweibull,
        st.johnsonsb,
        st.ksone,
        st.kstwobign,
        st.laplace,
        st.levy,
        st.logistic,
        st.loglaplace,
        st.lognorm,
        st.maxwell,
        st.mielke,
        st.nakagami,
        st.ncf,
        st.nct,
        st.norm,
        st.pareto,
        st.pearson3,
        st.powerlognorm,
        st.powernorm,
        st.rayleigh,
        st.rice,
        st.recipinvgauss,
        st.semicircular,
        st.t,
        st.triang,
        st.truncexpon,
        st.truncnorm,
        st.tukeylambda,
        st.uniform,
        st.vonmises,
        st.vonmises_line,
        st.wald,
        st.weibull_min,
        st.weibull_max
    ]

    # Best holders
    best_distribution = st.norm
    best_params = (0.0, 1.0)
    best_sse = np.inf

    # Estimate distribution parameters from data
    for distribution in distributions:
        print(distribution)

        # Try to fit the distribution
        try:
            # Ignore warnings from data that can't be fit
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore')

                # fit dist to data
                params = distribution.fit(data)

                # Separate parts of parameters
                arg = params[:-2]
                loc = params[-2]
                scale = params[-1]

                # Calculate fitted PDF and error with fit in distribution
                pdf = distribution.pdf(x, loc=loc, scale=scale, *arg)
                sse = np.sum(np.power(y - pdf, 2.0))

                # if axis pass in add to plot
                try:
                    if ax:
                        pd.Series(pdf, x).plot(ax=ax)
                    end
                except Exception:
                    pass

                # identify if this distribution is better
                if best_sse > sse > 0:
                    best_distribution = distribution
                    best_params = params
                    best_sse = sse

        except Exception:
            pass

    return (best_distribution.name, best_params)


def make_pdf(dist, params, size=10000):
    """Generate distributions's Propbability Distribution Function """

    # Separate parts of parameters
    arg = params[:-2]
    loc = params[-2]
    scale = params[-1]

    # Get sane start and end points of distribution
    start = dist.ppf(0.01, *arg, loc=loc, scale=scale) if arg else dist.ppf(0.01, loc=loc, scale=scale)
    end = dist.ppf(0.99, *arg, loc=loc, scale=scale) if arg else dist.ppf(0.99, loc=loc, scale=scale)

    # Build PDF and turn into pandas Series
    x = np.linspace(start, end, size)
    y = dist.pdf(x, loc=loc, scale=scale, *arg)
    pdf = pd.Series(y, x)

    return pdf

# generate or import data
input_path = 'calls3.csv'
with open(input_path, 'r') as f:
    reader = csv.reader(f)
    next(reader)

    raw_data = list(reader)

values_list = []
for row in raw_data:
    values_list.append(row)

# values list is binned by day call totals - as histogram

# fake some raw data based on the real...
input_data = []
for row in values_list:
    for i in range(int(row[2])):
        input_data.append(int(row[0])-0.5)

input_data = pd.Series(input_data)

# Plot for comparison
plt.figure(figsize=(12, 8))

ax = input_data.plot(kind='hist', bins=10, normed=True, alpha=0.5)
#plt.show()

# Save plot limits
dataYLim = ax.get_ylim()

# Find best fit distribution
best_fit_name, best_fir_paramms = best_fit_distribution(input_data, 72, ax)
best_dist = getattr(st, best_fit_name)

# Update plots
ax.set_ylim(dataYLim)
ax.set_title('Call patterns')
ax.set_xlabel('time')
ax.set_ylabel('Frequency')

# Make PDF
pdf = make_pdf(best_dist, best_fir_paramms)
#print(best_fir_paramms)

# Display
#plt.figure(figsize=(12, 8))
ax = pdf.plot(lw=2, label='PDF', legend=True)
input_data.plot(kind='hist', bins=10, normed=True, alpha=0.5, label='Data', legend=True, ax=ax)

param_names = (best_dist.shapes + ', loc, scale').split(', ') if best_dist.shapes else ['loc', 'scale']
param_str = ', '.join(['{}={:0.2f}'.format(k, v) for k, v in zip(param_names, best_fir_paramms)])
dist_str = '{}({})'.format(best_fit_name, param_str)

ax.set_title('Best fit to calls is \n' + dist_str)
ax.set_xlabel('Time')
ax.set_ylabel('Frequency')

plt.show()
