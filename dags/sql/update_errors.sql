drop table forecast_errors;
create temporary table forecast_errors as
(
select  er.currency,
		er.rate,
		er.time_period,
		f.forecast_date,
		er.rate - f."0" as forecast_error
from ex_rates er inner join forecast f 
	ON f.index = er.time_period 
		and f.currency = er.currency 
		and f.forecast_error is null
)

update forecast
set 
	forecast_error = err.forecast_error
from forecast_errors err 
where
	forecast.currency = err.currency and
	forecast.forecast_date = err.forecast_date and
	forecast.index = err.time_period;
	
select * 
from forecast f
order by currency, index;
