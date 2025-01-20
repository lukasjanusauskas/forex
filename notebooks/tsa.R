# In this file I do time series analysis to:
# 1. Determine, whether the time series, I am modelling are stationary
# 2. Explore the cross-correlation
# 3. Explore the periodicity of the time series
# 4. Explore time-series outliers 

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

# Periodicity ##################################################################

p.values.nolag <- ex_rates %>% 
  group_by(currency) %>% 
  arrange(time_period) %>% 
  group_map(~ adf.test(ts(.x$rate))$p.value)

mean(p.values.nolag < 0.05)

calc_lagged_ts <- function(x){
  lagged <- lag(x, default = x[1])
  return(ts(x - lagged))
}

p.values.lag <- ex_rates %>% 
  group_by(currency) %>% 
  arrange(time_period) %>% 
  group_map(~ adf.test(calc_lagged_ts(.x$rate))$p.value)

mean(p.values.lag < 0.05)
# Only 1/9th of exchange rates pass the Augmented Dickey-Fuller test at 5% level
# Without any differencing, but first order differencing transformed the data into a 
# stationary time series

ex_rates %>% 
  group_by(currency) %>% 
  mutate(rate_diff = rate - lag(rate)) %>% 
  arrange(time_period) %>% 
  ggplot(aes(x = time_period, y = rate_diff)) +
    geom_line() +
    facet_wrap(~ currency, scale="free_y") +
    geom_smooth(method = 'lm')

# All of the differenced times series are approximately centered around zero.
# Which is to be excepted, however it means, that we need to construct 
# a different time series(weekly aggregates for example, or use windowing), 
# to not get a rubbish forecast.

# Uni-variate ARIMA forecast ###############################################################

test_sample_arima <- function(x) {
  model <- auto.arima(x)
  return( residuals(model) )
}

lagged <- ex_rates %>% 
  group_by(currency) %>% 
  mutate(month_lag = rate - lag(rate, n=10)) %>% 
  drop_na()

train_df <- lagged %>% 
  filter(time_period < ymd('2024-11-30'))

test_df <- lagged %>% 
  filter(time_period >= ymd('2024-11-30'))

currencies <- unique(ex_rates$currency)
results <- data.frame(list("curr"=NA, "date"=NA, "pred"=NA, "month_lag"=NA))

for (curr in currencies) {
  train <- train_df %>% 
    filter(currency == curr) %>%
    ungroup() %>% 
    select(month_lag)
  
  test <- test_df %>% 
    filter(currency == curr) %>% 
    ungroup()
  
  if (nrow(test) == 0) {
    next
  }
  
  model <- auto.arima(train)
  pred <- predict(model, nrow(test))$pred
  
  res <- cbind(rep(curr, nrow(test)), test$time_period, c(pred), c(test$month_lag))
  colnames(res) <- c("curr", "date", "pred", "month_lag")
  
  results <- rbind(results, res)
}

results %>% 
  drop_na() %>% 
  ggplot(aes(x = date, y=month_lag)) +
    geom_line(col='black') +
    geom_line(aes(y=pred), col='red') +
  facet_wrap(~curr, scales='free_y')

# For most currencies, this forecast is horrible, however, I will try to improve on it with XGBoost later

# Cross-correlation ############################################################

# Get quarterly ex rates aggregates from master
# TODO: fix aggregate calculation - it doesn't take into account measures

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

# Outlier detection ############################################################

# Big changes in interest rates and exchange rate 

aggregates %>% 
  inner_join(inr_names, by = join_by(inr_measure == index)) %>%  
  group_by(currency, inr_measure) %>% 
  mutate(diff_inr = min_inr - lag(min_inr),
         date = paste0(`year(date)`, '-Q', `quarter(date)`)) %>% 
  mutate(date = parse_date_time(date, orders = "%y-%q")) %>% 
  drop_na() %>% 
  ggplot(aes(x = date, y = diff_inr, col = as.factor(full_name))) +
    geom_line(alpha = 0.8) +
    facet_wrap(~ currency, scales = "free_y") +
    theme(legend.position = c(0.7, 0.1)) +
    guides(color = guide_legend(title = "Interest rate measure"))

# All of the entities, have, what seems to be, outliers in the differenced measures.
# Therefore a univariate outlier detection method will be employed to get these outliers and 
# See, how do these changes correlate with exchange rates. And then we might be able 
# to come up with a strategy to compare these measures between entities.

# If the distributions are normal, we may use the z-score method.

aggregates %>% 
  inner_join(inr_names, by = join_by(inr_measure == index)) %>%  
  group_by(currency, inr_measure) %>% 
  mutate(diff_inr = min_inr - lag(min_inr),
         date = paste0(`year(date)`, '-Q', `quarter(date)`)) %>% 
  mutate(date = parse_date_time(date, orders = "%y-%q")) %>% 
  drop_na() %>% 
  ggplot(aes(x = diff_inr, fill = as.factor(full_name))) +
    geom_histogram(position = "dodge") +
    facet_wrap(~ currency, scales = "free_y") +
    theme(legend.position = c(0.7, 0.1)) +
    guides(color = guide_legend(title = "Interest rate measure"))

# The distributions are definetly not Gaussian
# Therefore I will use the IQR criterion(default boxplot.stats criterion)

currencies <- unique(aggregates$currency)
sample <- aggregates %>% 
  filter(inr_measure == 3)

outlier_tested <- aggregates %>% 
  group_by(currency, inr_measure) %>% 
  mutate(
    iqr = IQR(min_inr),
    q1 = quantile(min_inr, 0.25),
    q3 = quantile(min_inr, 0.75)
  ) %>% 
  mutate(is_inr_outlier = 
           (min_inr < q1 - 1.5 * iqr) |
           (min_inr > q3 + 1.5 * iqr)) %>% 
  select(-c("iqr", "q1", "q3"))

outlier_tested %>%
  mutate(date = parse_date_time(
    paste0(`year(date)`, "-Q", `quarter(date)`),
    orders = "%y-%q"
  )) %>% 
  filter(inr_measure == 3) %>%  # Short-term interest rates
  ggplot(aes(x = date, y = mean_rate, col = is_inr_outlier)) +
    geom_line() +
    facet_wrap(~ currency, scales = "free_y") +
    theme(legend.position = c(0.7, 0.1))

# Interest rate outliers can indicate different trajectories for exchange rates,
# However, not all of them are explained or effected by them, so we need some more data

# 


# Periodicity ##################################################################




