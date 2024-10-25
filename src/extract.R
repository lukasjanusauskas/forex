library(dplyr)
library(tidyr)
library(RPostgreSQL)
library(countrycode)

# Get a Postgres connection
conn <- dbConnect("PostgreSQL", user = "lukas", dbname = "forex")

# Exchange rates ################################################

# Notes:
# The frequency that I want is daily (D)
# Exchange rate types: SP00
# Series variation: Average(not much choice after other cleaning)

# Turn string dates into actual dates
# Turn observation status into a factor

if (!dbExistsTable(conn, "rates")) {
  ex_rates <- read.csv("data/data.csv")

  ex_rates <- ex_rates  %>%
    filter(FREQ == "D",
           EXR_TYPE == "SP00")  %>%
    mutate(TIME_PERIOD = as.Date(TIME_PERIOD),
           OBS_STATUS = as.factor(OBS_STATUS))  %>%
    select(c("CURRENCY", "CURRENCY_DENOM", "TIME_PERIOD",
             "OBS_STATUS", "OBS_VALUE"))

  dbWriteTable(conn, "rates", ex_rates)
}

# PPP #########################################################

# Country names will have to be normalized with the `countrynames` library.
# It is essential for many reasons, that TIME_PERIOD shohuld be interger

if (!dbExistsTable(conn, "ppp")){
  ppp_defl <- read.csv("data/ppp_deflator.csv", skip = 4)  %>%
    select(-c("Indicator.Name", "Indicator.Code", "X", "Country.Name")) %>%
    rename("Country.code" = "Country.Code") %>%
    pivot_longer(!c("Country.code"),
                 names_to = "TIME_PERIOD",
                 values_to = "ppp")  %>%
    mutate(TIME_PERIOD = as.integer(gsub("X", "", TIME_PERIOD)))

  dbWriteTable(conn, "ppp", ppp_defl)
}

# CPI ###########################################################
if (!dbExistsTable(conn, "cpi")) {
  cpi <- read.csv("data/cpi.csv")  %>%
    mutate("Country.code" = LOCATION)  %>%
    select(c("Country.code", "SUBJECT", "TIME_PERIOD", "OBS_VALUE"))  %>%
    pivot_wider(values_from = "OBS_VALUE",
                names_from = "SUBJECT",
                names_prefix = "CPI_")

  dbWriteTable(conn, "cpi", cpi)
}

# Earnings #######################################################
if (!dbExistsTable(conn, "earnings")){
  earnings <- read.csv("data/earnings.csv")  %>%
    mutate("Country.code" = REF_AREA)  %>%
    select(c("Country.code", "UNIT_MEASURE", "TIME_PERIOD", "OBS_VALUE"))

  dbWriteTable(conn, "earnings", earnings)
}

# GDP ###########################################################

# Mostly same data cleaning can be applied to GDDP data,
# since the dimensions are simillar and both df's are from world bank

if (!dbExistsTable(conn, "gdp")) {
  gdp <- read.csv("data/gdp.csv", skip = 4)  %>%
    select(-c("Indicator.Name", "Indicator.Code", "X", "Country.Name")) %>%
    pivot_longer(!c("Country.Code"),
                 names_to = "TIME_PERIOD",
                 values_to = "gdp")  %>%
    rename("Country.code" = "Country.Code") %>%
    mutate(TIME_PERIOD = as.integer(gsub("X", "", TIME_PERIOD)))

  dbWriteTable(conn, "gdp", gdp)
}
