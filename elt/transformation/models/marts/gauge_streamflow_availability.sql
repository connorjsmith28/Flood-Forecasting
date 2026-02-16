-- This data shows what data is available for the HUC = 10 gauges for each year.

with info as (
    select * from {{ ref('stg_info') }}
),
basin as (
    select * from {{ ref('stg_gagesii_basin') }}
)

select
    inf.STAID,
    b.staname,    
    b.lat_gage,
    b.lng_gage,
    b.state_gage,
    b.fips_site,
    b.countyname_site,
    inf."data_availability [hrs]",
    inf."1980",
    inf."1981",
    inf."1982",
    inf."1983",
    inf."1984",
    inf."1985",
    inf."1986",
    inf."1987",
    inf."1988",
    inf."1989",
    inf."1990",
    inf."1991",
    inf."1992",
    inf."1993",
    inf."1994",
    inf."1995",
    inf."1996",
    inf."1997",
    inf."1998",
    inf."1999",
    inf."2000",
    inf."2001",
    inf."2002",
    inf."2003",
    inf."2004",
    inf."2005",
    inf."2006",
    inf."2007",
    inf."2008",
    inf."2009",
    inf."2010",
    inf."2011",
    inf."2012",
    inf."2013",
    inf."2014",
    inf."2015",
    inf."2016",
    inf."2017",
    inf."2018",
    inf."2019",
    inf."2020",
    inf."2021",
    inf."2022",
    inf."2023",
    inf."2024"
from info as inf
right join basin as b on b.site_id = inf.STAID
where
    b.huc02 in ('10U', '10L')
