# In this file I do time series analysis to:
# 1. Analize the nature of trends of exchange rates
# 2. Explore the seasonality
# 3. Check, whether cross-correlation is good for modelling
# 3a. If CCF won't be good: do some feature engineering

# Import data ##################################################################
library(RPostgreSQL)
library(dplyr)
library(tidyr)
library(lubridate)
library(ggplot2)
library(TSA)
library(tseries)
library(forecast)

# Get connection
con <- dbConnect("PostgreSQL",
                 dbname="forex",
                 host="localhost",
                 port="5454",
                 user="airflow",
                 password="airflow")

# Get data tables
ex_rates <- dbReadTable(con, 'ex_rates')
master <- dbReadTable(con, 'master')
inr_names <- dbReadTable(con, "inr_measure_final")
bop_names <- dbReadTable(con, "bop_measure_final")

# TSA ##########################################################################
# Trend analysis ###############################################################

ex_rates %>% 
  ggplot(aes(x = time_period, y = rate)) +
    geom_line() +
    facet_wrap(~currency, scales = "free_y")

# Overall, the trends do seem to be different for the different exchange rates:
# whereas some seem to have exponential trends, others may not even have a clear trend at all

acfs <- ex_rates %>% 
  group_by(currency) %>% 
  arrange(time_period) %>% 
  reframe(acf_val = acf(rate, lag.max=5000)$acf,
          acf_lag = acf(rate, lag.max=5000)$lag)

acfs %>% 
  ggplot(aes(x = acf_lag, y = acf_val)) +
    geom_point(size = 0.2) +
    geom_segment(aes(xend = acf_lag, y = 0, yend = acf_val, col = acf_lag),
                 linewidth = 0.1, alpha = 0.7) +
    facet_wrap(~currency) +
    theme(legend.position = "none")

# ACF plots also show the differences between the strength of trends in time-series
# While some ACFs decay really slowly, others have a, comparatively, fast decaying 
# Also, models with strong assumptions would have weak longevity, so any model 
# that makes assumptions about trends shouldn't be considered.

# Also, there aren't any clear seasonality, although some ACFs do oscillate, 
# they do it in a decaying manner.
 
# Seasonality ##################################################################

# For checking the seasonality component, I will use periodograms, to check seasonality

ex_rates %>% 
  group_by(currency) %>% 
  arrange(currency) %>% 
  reframe(per = periodogram(rate)$spec[1:10],
          freq = periodogram(rate)$freq[1:10]) %>% 
  ggplot(aes(x = freq, y = per)) +
    geom_line() +
    facet_wrap(~currency, scales = "free")

decompose(filter(ex_rates, currency == 32)$rate)

# What we can see is that there is no long term seasonality,
# Furthermore, short-term seasonality also does not seem to be present.

# Cross-correlation ############################################################

# Get quarterly ex rates aggregates from master

aggregates <- master %>% 
  drop_na() %>% 
  group_by(currency, year(date), quarter(date), inr_measure, bop_measure) %>% 
  summarize(
    mean_rate = mean(rate),
    min_rate = min(rate),
    max_rate = max(rate),
    median_rate = median(rate),
    min_bop = min(bop_value, na.rm = TRUE),
    min_inr = min(inr_value, na.rm = TRUE)
  )

# For each currency get the ccf and plot it

ccf_results <- aggregates %>% 
  group_by(currency, inr_measure) %>% 
  mutate(diff_inr = min_inr - lag(min_inr)) %>% 
  drop_na() %>% 
  reframe(
    ccf_lag = ccf(min_rate, diff_inr)$lag,
    ccf_val = ccf(min_rate, diff_inr)$acf
  ) %>% 
  inner_join(inr_names, by = join_by(inr_measure == index))

ccf_results %>% 
  ggplot(aes(x = ccf_lag, y = ccf_val, 
             col = as.factor(full_name)),
          lw = 0.1) +
    geom_line(aes(x = ccf_lag), y = 0.5, 
            col='blue', 
            linetype = "dashed") +
    geom_line(aes(x = ccf_lag), y = -0.5, 
            col='blue', 
            linetype = "dashed") +
    geom_point(aes(x = ccf_lag, y = ccf_val, 
                   col = as.factor(full_name)), 
               size=1, alpha = 0.5) +
    geom_segment( aes(x = ccf_lag, xend = ccf_lag, 
                      y = 0, yend = ccf_val), alpha = 0.5) +
    ylim(c(-0.5, 0.5)) +
    facet_wrap(~currency) +
  guides(color = guide_legend(title="Interest rate type")) +
  xlab("") +
  ylab("CCF") +
  ggtitle("CCFs between interest rate changes and\nquarterly aggregated exchange rate(EURO as a denominator)") +
  theme(legend.position = c(0.75, 0.1))

# It doesn't appear, as if the correlation is really strong, and in some cases 
# it isn't present, however this needs further inquiry, since a 
# rapid change may indicate a shift in demand for the currency. 

# Balance of payment data and exchange rates

ccf_results <- aggregates %>% 
  group_by(currency, bop_measure) %>% 
  drop_na() %>% 
  reframe(
    ccf_lag = ccf(min_rate, min_bop)$lag,
    ccf_val = ccf(min_rate, min_bop)$acf
  ) %>% 
  inner_join(bop_names, by = join_by(bop_measure == index))

ccf_results %>% 
  ggplot(aes(x = ccf_lag, y = ccf_val, 
             col = as.factor(full_name)),
         lw = 0.1) +
  geom_line(aes(x = ccf_lag), y = 0.5, 
            col='blue', 
            linetype = "dashed") +
  geom_line(aes(x = ccf_lag), y = -0.5, 
            col='blue', 
            linetype = "dashed") +
  geom_point(aes(x = ccf_lag, y = ccf_val, 
                 col = as.factor(full_name)), 
             size=1, alpha = 0.5) +
  geom_segment( aes(x = ccf_lag, xend = ccf_lag, 
                    y = 0, yend = ccf_val), alpha = 0.5) +
  ylim(c(-1.0, 1.0)) +
  facet_wrap(~currency) +
  guides(color = guide_legend(title="Balance of payment measure")) +
  xlab("") +
  ylab("CCF") +
  ggtitle("CCFs between varioues BOP measures and\nquarterly aggregated exchange rate(EURO as a denominator)") +
  theme(legend.position = c(0.75, 0.1))

# Balance of payment data does seem to correlate mildly with exchange rate.
# However the sign of the correlation does vary from currency to currency.
# I expect the tendencies to be roughly the same, when I will calculate the 
# exchange rates with different denominators and use differences or ratios of the measures. 


