-- info.csv lists the amount of hourly data available for each gauge at each year.
select*
from {{ ref('info') }}