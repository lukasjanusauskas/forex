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

# Dimension tables for better understanding(deciphering codes)
dim_currency <- dbReadTable(con, 'dim_currency')
dim_entity <- dbReadTable(con, 'dim_entity')

# Main tasks:
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


