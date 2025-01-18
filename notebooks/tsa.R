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

# Get data tables for quality checks
ex_rates <- dbReadTable(con, 'ex_rates')
balance_of_pay <- dbReadTable(con, 'balance_of_pay')
interest_rate <- dbReadTable(con, 'interest_rate')

# TSA ########################################################################## 

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
