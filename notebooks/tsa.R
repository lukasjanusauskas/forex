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

currencies <- unique(lagged$currency)
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
  group_by(currency, year(date), quarter(date)) %>% 
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
  group_by(currency) %>% 
  mutate(diff_inr = min_inr - lag(min_inr)) %>% 
  drop_na() %>% 
  reframe(
    ccf_lag = ccf(min_rate, diff_inr)$lag,
    ccf_val = ccf(min_rate, diff_inr)$acf
  )

ccf_results %>% 
  ggplot(aes(x = ccf_lag, y = ccf_val)) +
    geom_point() +
    geom_segment( aes(x = ccf_lag, xend = ccf_lag, y = 0, yend = ccf_val) ) +
    geom_line(aes(x = ccf_lag), y = 0.5, 
              col='blue', 
              linewidth=0.3, 
              linetype = "dashed") +
  facet_wrap(~currency)

# It doesn't appear, as if the correlation is really strong, and in some cases 
# it isn't present, however this needs further inquiry, since a 
# rapid change may indicate a shift in demand for the currency. 

ccf_results <- aggregates %>% 
  group_by(currency) %>% 
  mutate(diff_bop = min_bop - lag(min_bop)) %>% 
  drop_na() %>% 
  reframe(
    ccf_lag_diff = ccf(min_rate, diff_bop)$lag,
    ccf_val_diff = ccf(min_rate, diff_bop)$acf,
    ccf_lag = ccf(min_rate, min_bop)$lag,
    ccf_val = ccf(min_rate, min_bop)$acf
  )

ccf_results %>% 
  ggplot(aes(x = ccf_lag, y = ccf_val)) +
  geom_point() +
  geom_segment( aes(x = ccf_lag, xend = ccf_lag, y = 0, yend = ccf_val) ) +
  geom_line(aes(x = ccf_lag), y = 0.5, 
            col='blue', 
            linewidth=0.3, 
            linetype = "dashed") +
  geom_line(aes(x = ccf_lag), y = -0.5, 
            col='blue', 
            linewidth=0.3, 
            linetype = "dashed") +
  facet_wrap(~currency)
