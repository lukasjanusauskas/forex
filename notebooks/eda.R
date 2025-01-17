# The purpose of this analysis is to check the quality and availability of the data

# Import data ##################################################################
library(RPostgreSQL)
library(dplyr)
library(tidyr)
library(lubridate)
library(ggplot2)

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

# Get the views data
macro <- dbReadTable(con, 'macro')

# Dimension tables for better understanding(deciphering codes)
dim_currency <- dbReadTable(con, 'dim_currency')
dim_entity <- dbReadTable(con, 'dim_entity')

# Main tasks: ##################################################################
# 0. Check, whether the schema is correct
# 1. Check duplicates
# 2. Check data accuracy(whether it contradicts other publicly available sources)
# 3. Explore missing values
# 4. Explore differences between consecutive dates
# 5. Explore the dates of available data for entities and measures

str(ex_rates)
str(balance_of_pay)
str(interest_rate)

# Schemas of these tables are the way they are supposed to be
# Currencies, entities and measures are encoded, dates are in the correct format, 
# values  are numeric

any(duplicated(ex_rates[c('currency', 'time_period')]))
any(duplicated(balance_of_pay[c('entity', 'measure', 'date')]))
any(duplicated(interest_rate[c('entity', 'meas', 'date')]))

# There are no duplicate values

# For accuracy checks I will take a sample of data in check it by hand

ex_rates %>%
  filter(time_period == ymd("2025-01-16")) %>% 
  head(3)
dim_currency

# Checking by hand: one Euro should be worth approximately 1.65 AUD, 1.96 BGN, 6.19 BRL in 2025-01-16
# There is a small discrepancy in the value of Brazilian Peso because of the way the data is published

ex_rates %>% 
  filter(is.na(rate))

time_periods <- ex_rates %>% 
  group_by(currency) %>% 
  summarize(earliest_date = min(time_period),
            latest_date = max(time_period))

time_periods %>% 
  filter(earliest_date > ymd("2015-01-02"))

dim_currency %>%  filter(ex_id == 24)

# Icelandic Krona was not available in FOREX markets from 2008 to 2018.

time_periods %>% 
  filter(latest_date < max(time_periods$latest_date)) %>%
  inner_join(dim_currency, by = join_by(currency == ex_id)) %>% 
  select(c("name", "latest_date"))

# For some currencies data is no longer available from 2020(ARS, DZD, MAD, TWD) or 2022(RUB, HRK)

# After double-checking, I can conclude, that the data is simply not available in the ECB database, 
# since they no longer track the exchange rates of these currencies within their databases.

# However, these are not the main exchange rates, that are traded most often,
# so fixing this issue by including another data source would be a ton of work with very small returns.

time_periods_bop <- balance_of_pay %>% 
  group_by( entity, measure ) %>% 
  summarize(earliest_date = min(date),
            latest_date = max(date))

time_periods_bop %>% 
  filter(latest_date < max(time_periods_bop$latest_date) | 
           earliest_date > min(time_periods_bop$latest_date)) %>% 
  inner_join(dim_entity, by = join_by(entity == bop_id)) %>% 
  select(c('name', 'earliest_date', 'latest_date')) %>% 
  unique()

# At the time, when I gathered the data of 2024-Q2, Turkey's, India's and OECD's 
# balance of payment data isn't present in the dataset.

# And overall, these measurements take long time to be published, so most countries
# don't even have 2024-Q3 data

time_periods_inr <- interest_rate %>% 
  group_by( entity, meas ) %>% 
  summarize(earliest_date = min(date),
            latest_date = max(date))

time_periods_bop %>% 
  filter(latest_date < max(time_periods_bop$latest_date) | 
           earliest_date > min(time_periods_bop$latest_date)) %>% 
  inner_join(dim_entity, by = join_by(entity == int_id)) %>% 
  select(c('name', 'earliest_date', 'latest_date')) %>% 
  unique()

# Again, India's data on interest rates isn't available for Q3, when rest of the 
# entities, present in the dataset, have their interest rates reported for Q3 (of 2024).
# This can be the result of _____________

ex_rates %>% 
  group_by(currency) %>% 
  arrange(time_period) %>% 
  summarize(max_diff = max(time_period - lag(time_period), na.rm = TRUE)) %>% 
  filter(max_diff > 5)

# There have been no cases, when the difference between dates was longer than 5 days

ex_rates %>% 
  group_by(currency) %>% 
  arrange(time_period) %>% 
  filter(time_period - lag(time_period) == 5) %>% 
  ungroup() %>% 
  select(c("time_period")) %>% 
  unique()

# The cases, when the have no apparent pattern

seconds_to_days <- function(seconds) {
  return(as.integer(seconds) / (60 * 60 * 24))
}

balance_of_pay %>% 
  group_by(entity) %>% 
  arrange(date) %>% 
  summarize(max_diff = seconds_to_days(max(date - lag(date), na.rm = TRUE))) %>% 
  filter(max_diff >= 93.0)

# No big gaps between balance of payment data

macro %>% 
  filter(is.na(inr_value)) %>% 
  inner_join(dim_entity, by = join_by(index)) %>% 
  select(c(name, date)) %>% 
  unique() %>% 
  arrange(name)

# Such entities as EU, OECD don't have interest rate records, 
# Argentina (which is a, unlike the aforementioned, a single country) doesn't 
# have interest rate records in the database
# Furthermore, Turkey's interest rate in 2023 isn't recorded
# This seems to be more OECD relatd, since there are Turkey's interest rate records in, for example, Trading Economics datasets.

# Summary: #####################################################################
# There are a lot of types of missing data, that may be causes of geopolitics or 
# other unknown reasons, therefore imputing them doesn't seem to be a good strategy

# The data seems to be accurate and not distorted in any way.

# And there seem to be no huge gaps between measurements of FOREX or balance of payment data