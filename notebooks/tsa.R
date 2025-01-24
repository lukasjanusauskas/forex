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
macro <- dbReadTable(con, 'macro')

dim_currency <- dbReadTable(con, "dim_currency")
inr_names <- dbReadTable(con, "inr_measure_final")
bop_names <- dbReadTable(con, "bop_measure_final")
entities <- dbReadTable(con, "entity_dimension_final")

# TSA ##########################################################################
# Trend analysis ###############################################################

ex_rates %>% 
  ggplot(aes(x = time_period, y = rate)) +
    geom_line() +
    facet_wrap(~currency, scales = "free_y")

# There may seem to be something wrong with Bulgarian lev(currency 2), but it is 
# actualy pegged to the Euro, so having it in our analysis won't be useful.

ex_rates <- ex_rates %>% 
  filter(currency != 2)

# Overall, the trends do seem to be different for the different exchange rates:
# whereas some seem to have exponential trends, others may not even have a clear trend at all

acfs <- ex_rates %>% 
  group_by(currency) %>% 
  arrange(time_period) %>% 
  reframe(acf_val = acf(rate, lag.max=500)$acf,
          acf_lag = acf(rate, lag.max=500)$lag,
          pacf_val = pacf(rate, lag.max=500)$acf,
          pacf_lag = pacf(rate, lag.max=500)$lag)

acfs

acfs %>% 
  inner_join(dim_currency, by = join_by(currency == ex_id)) %>% 
  ggplot(aes(x = acf_lag, y = acf_val)) +
    geom_point(size = 0.2) +
    geom_segment(aes(xend = acf_lag, y = 0, yend = acf_val, col = acf_lag),
                 linewidth = 0.1, alpha = 0.7) +
    facet_wrap(~ name) +
    xlab("ACF lag") +
    ylab("ACF") +
    ggtitle("ACF of exchange rates (denominator - Euro)") +
    theme(legend.position = "none",
          axis.text.x = element_blank(),
          axis.ticks.x = element_blank())

# ACF plots also show the differences between the strength of trends in time-series
# While some ACFs decay really slowly, others have a, comparatively, fast decaying 
# Also, models with strong assumptions would have weak longevity, so any model 
# that makes assumptions about trends shouldn't be considered.

# Also, there aren't any clear seasonality, although some ACFs do oscillate, 
# they do it in a decaying manner.

acfs %>% 
  inner_join(dim_currency, by = join_by(currency == ex_id)) %>% 
  ggplot(aes(x = pacf_lag, y = pacf_val)) +
    geom_point(size = 0.2) +
    geom_segment(aes(xend = pacf_lag, y = 0, yend = pacf_val, col = pacf_lag),
                 linewidth = 0.1, alpha = 0.7) +
    facet_wrap(~ name) +
    xlab("PACF lag") +
    ylab("PACF") +
    ggtitle("PACF of exchange rates (denominator - Euro)") +
    theme(legend.position = "none",
          axis.text.x = element_blank(),
          axis.ticks.x = element_blank())

# The PACF clearly tells us, that there is no clear seasonality component

# Stationarity #################################################################

ex_rates %>% 
  ggplot(aes(x = time_period, y = rate)) +
    geom_line() +
    facet_wrap(~currency, scales = "free_y")

# The data is definitely not mean-stationary the majority of time
# However, this seems to be variance-stationary

ex_rates %>% 
  group_by(currency) %>% 
  mutate(diff_rate = rate - lag(rate)) %>% 
  ggplot(aes(x = time_period, y = diff_rate)) +
    geom_line() +
    facet_wrap(~currency, scales = "free_y")

# After only one differencing it produces a zero-mean stationary time-series
# However, the series are, essentially white-noise

ex_rates %>% 
  group_by(currency) %>% 
  mutate(diff_rate = rate - lag(rate)) %>% 
  reframe(p.val = Box.test(diff_rate)$p.value) %>% 
  print(n = nrow(.))

# Forecasting this would be impossible, since 
# A much better approach is forecasting the aggregated(monthly or quarterly)
# exchange rate

# Get quarterly ex rates aggregates from master

aggregates <- master %>% 
  drop_na() %>% 
  mutate(date_str = paste0(year(date), '-', quarter(date)),
         q_date = parse_date_time(date_str, orders=c('%y-%q')),
         q_date = as.Date(q_date)) %>% 
  select(-c("date_str")) %>% 
  group_by(currency, q_date, inr_measure, bop_measure) %>% 
  summarize(
    mean_rate = mean(rate),
    min_rate = min(rate),
    max_rate = max(rate),
    median_rate = median(rate),
    min_bop = min(bop_value, na.rm = TRUE),
    min_inr = min(inr_value, na.rm = TRUE)
  )

aggregates %>% 
  filter(inr_measure == 3,
         bop_measure == 3) %>% 
  pivot_longer(cols = ends_with("rate"),
               names_to = "agg_type") %>% 
  ggplot(aes(x = q_date, y = value, col = agg_type)) +
    geom_line() +
    facet_wrap(~ currency, scales = "free_y") +
    ggtitle("Quarterly aggregates of ex rates") +
    xlab("Date") +
    ylab("Rate aggregates") +
    guides(color = guide_legend(title = "Aggregates")) +
    theme(legend.position = c(0.9, 0.1),
          axis.ticks.x = element_blank(),
          axis.text.x = element_blank())

# These are still a little bit daunting to predict, however this is something
# much more manageable, than the white noise, that we have encountered previously.

# Cross-correlation ############################################################

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

# Preliminary feature engineering: EU aggregate data ###########################

eu_id <- filter(entities, country_code == 'EU27_2020')$index
agg_eu <- macro %>% 
  mutate(date = as.Date(strptime(date, format = "%Y-%m-%d"))) %>% 
  filter(index == eu_id) %>%
  select(-c("inr_measure", "inr_value")) %>% 
  inner_join(aggregates, by = join_by(date == q_date,
                                      measure == bop_measure)) %>% 
  rename(eu_bop = bop_value)


agg_eu %>% 
  group_by(currency, measure) %>% 
  reframe(
    ccf_lag = ccf(min_bop - eu_bop, mean_rate, plot = FALSE)$lag,
    ccf_val = ccf(min_bop - eu_bop, mean_rate, plot = FALSE)$acf
  ) %>% 
  inner_join(bop_names, by = join_by(measure == index)) %>% 
  ggplot(aes(x = ccf_lag, y = ccf_val, col = full_name)) +
    geom_point() +
    geom_segment(aes(xend = ccf_lag, yend = 0)) +
    geom_line(y = 0.5, lty = "dashed", col = "blue") +
    geom_line(y = -0.5, lty = "dashed", col = "blue") +
    ylim(c(-1, 1)) +
    facet_wrap(~currency, scales = "free") +
    guides(color = guide_legend(title = "Balance of payment measure")) +
    theme(legend.position = c(0.75, 0.1)) +
    xlab("CCF lag") +
    ylab("CCF") +
    ggtitle("CCFs of exchange rates vs balance of payment measures,\ncompared with EU")

# When we take the goods(3) or current account(6) and do this feature engineering 
# we get some-what useful results, although it is not something fantastic. 
# However it does not exceed the results we obtained before any engineering.

# Summary: #####################################################################
# There are many different trend dynamics, however variation seems to be stationary

# By calculating the difference between a single lag(differencing once) we obtain 
# a zero mean stationary model, that seems to be white noise

# Therefore the better strategy would be to forecast quarterly exchange rate aggregates.
# For that we may use short or immediate interest rates and balance of payment data,
# both indicate the demand for a currency.
