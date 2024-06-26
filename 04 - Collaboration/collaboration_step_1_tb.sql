/***************************************************************************************************
  _______           _            ____          _             
 |__   __|         | |          |  _ \        | |            
    | |  __ _  ___ | |_  _   _  | |_) | _   _ | |_  ___  ___ 
    | | / _` |/ __|| __|| | | | |  _ < | | | || __|/ _ \/ __|
    | || (_| |\__ \| |_ | |_| | | |_) || |_| || |_|  __/\__ \
    |_| \__,_||___/ \__| \__, | |____/  \__, | \__|\___||___/
                          __/ |          __/ |               
                         |___/          |___/            
Demo:         Tasty Bytes
Version:      v1.1
Vignette:     Collaboration
Script:       collaboration_step_1_tb.sql         
Create Date:  2023-01-13
Author:       Jacob Kranzler
Copyright(c): 2023 Snowflake Inc. All rights reserved.
****************************************************************************************************
Description: 
    Collaboration
      Part 1 - Public, Free Marketplace Listing
          1) Acquire Free Listing - Weathersource
          2) Harmonizing Orders and Weather
          3) Views & SQL Functions

      Part 2 - Personalized, Marketplace Listing
          1) Explore Personalized - Safegraph POI Listing
          2) Analyze Top Selling Locations + Common Table Expression
          
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2023-01-13          Jacob Kranzler      Initial Release
2023-01-18          Jacob Kranzler      Added USE ROLE accountadmin; to Reset
2023-09-20          Joshua Shaffer      Added ACCOUNTADMIN role as a required step
***************************************************************************************************/

/*- Part 1 -*/
/*----------------------------------------------------------------------------------
    Step 1 - 
      Our Tasty Bytes Financial Analysts have brought it to our attention that there
      are unexplainable days in various cities where our truck sales went to 0. One
      example they have provided was for Hamburg, Germany in February of 2022.
----------------------------------------------------------------------------------*/

-- first set our role and warehouse context
USE ROLE tasty_data_engineer;
USE WAREHOUSE tasty_de_wh;

/* CRITICAL STEP: DO NOT SKIP */
-- FIND AND REPLACE THE STRING "<firstname>_<lastname>" WITH YOUR NAME EX. "SRIDHAR_RAMASWAMY"
CREATE DATABASE IF NOT EXISTS frostbyte_tasty_bytes_v2_<firstname>_<lastname> CLONE frostbyte_tasty_bytes_v2;
/* ========================== */

-- now to begin let's see if we can confirm our analysts findings using our Point of Systems Order data.
SELECT 
    o.date,
    SUM(o.price) AS daily_sales
FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.orders_v o
WHERE 1=1
    AND o.country = 'Germany'
    AND o.primary_city = 'Hamburg'
    AND DATE(o.order_ts) BETWEEN '2022-02-01' AND '2022-02-28'
GROUP BY o.date
ORDER BY o.date ASC;


/*----------------------------------------------------------------------------------
    Step 2 - 
        From what we saw above, it looks like we are missing sales for February 16th
        through February 21st for Hamburg. Within our first party data there is not
        much else we can use to investigate this but something larger must have been
        at play here. 
        
        One idea we can immediately explore via leveraging the Snowflake Marketplace
        is extreme weather and a free, public listing provided by Weather Source.
----------------------------------------------------------------------------------*/

-- to begin, let's follow the steps below to access this data

    /*--- 
        -> Snowsight Home Button
            -> Marketplace
              -> Search: Weather Source LLC: frostbyte
                -> If you had the ACCOUNTADMIN role, you would see an option to "Get Data":"
                  -> For this lab, obviously all users should not have ACCOUNTADMIN, so the data was provisioned in advance
                    -> You can query the data, however, by clicking the "Open" Button

        Weather Source is a leading provider of global weather and climate data and our OnPoint Product Suite
        provides businesses with the necessary weather and climate data to quickly generate meaningful and
        actionable insights for a wide range of use cases across industries.
         - Documentation: https://docs.google.com/spreadsheets/d/1uDIKlfic6T2VIwAcBfs87nLR8_Ak5fTV/edit#gid=2108580577
    ---*/
    
-- with the shared FROSTBYTE_WEATHERSOURCE database in place, let's create a Harmonized layer view joining
-- Weather Source Daily History to our Country dimension table to filter to the Countries and Cities we serve
CREATE OR REPLACE VIEW frostbyte_tasty_bytes_v2_<firstname>_<lastname>.harmonized.daily_weather_v
    AS
SELECT 
    hd.*,
    TO_VARCHAR(hd.date_valid_std, 'YYYY-MM') AS yyyy_mm,
    pc.city_name AS city,
    c.country AS country_desc
FROM frostbyte_weathersource.onpoint_id.history_day hd
JOIN frostbyte_weathersource.onpoint_id.postal_codes pc
    ON pc.postal_code = hd.postal_code
    AND pc.country = hd.country
JOIN frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.country c
    ON c.iso_country = hd.country
    AND c.city = hd.city_name;


-- with the view in place let's take a look at the Average Daily Weather Temperature for Hamburg in February 2022
    --> Snowsight Chart:
        --> Chart Type: Line | X-Axis: DATE_VALID_STD(none) | Line: AVG_TEMPERATURE_AIR_2M_F(none) 
SELECT 
    dw.country_desc,
    dw.city_name,
    dw.date_valid_std,
    AVG(dw.avg_temperature_air_2m_f) AS avg_temperature_air_2m_f
FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.harmonized.daily_weather_v dw
WHERE 1=1
    AND dw.country_desc = 'Germany'
    AND dw.city_name = 'Hamburg'
    AND YEAR(date_valid_std) = '2022'
    AND MONTH(date_valid_std) = '2' -- February
GROUP BY dw.country_desc, dw.city_name, dw.date_valid_std
ORDER BY dw.date_valid_std DESC;


-- based on our results above, it does not seem like temperature was the cause, let's see if
-- precipitation or wind could have been a factor.
    --> Snowsight Chart:
        --> Chart Type: Line | X-Axis: DATE | Line: MAX_WIND_SPEED_100M_MPH(none) 
SELECT 
    dw.country_desc,
    dw.city_name,
    dw.date_valid_std,
    MAX(dw.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph,
    AVG(dw.tot_precipitation_in) AS tot_precipitation_in
FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.harmonized.daily_weather_v dw
WHERE 1=1
    AND dw.country_desc IN ('Germany')
    AND dw.city_name = 'Hamburg'
    AND YEAR(date_valid_std) = '2022'
    AND MONTH(date_valid_std) = '2' -- February
GROUP BY dw.country_desc, dw.city_name, dw.date_valid_std
ORDER BY dw.date_valid_std DESC;


/*----------------------------------------------------------------------------------
    Step 3 - 
        Ah ha! It looks like during those same days we did not have sales, Hamburg
        experienced hurricane level wind and precipitation.
        
        Let's now work to harmonize this live, marketplace Weather Data with our
        Orders Data and eventually deploy an Analytics view for our financial analysts
        to use for these exact scenarios
----------------------------------------------------------------------------------*/

-- as we are a global company, let's first create two SQL functions to convert Fahrenheit to Celsius and Inches to Millimeters
    --> create the SQL function that translates Fahrenheit to Celsius
CREATE OR REPLACE FUNCTION frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.fahrenheit_to_celsius(temp_f NUMBER(35,4))
RETURNS NUMBER(35,4)
AS
$$
    (temp_f - 32) * (5/9)
$$;

    --> create the SQL function that translates Inches to Millimeter
CREATE OR REPLACE FUNCTION frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.inch_to_millimeter(inch NUMBER(35,4))
RETURNS NUMBER(35,4)
    AS
$$
    inch * 25.4
$$;


-- before creating our Analytics view, let's create our SQL to pull Daily Sales and Weather for Hamburg, Germany
SELECT 
    fd.date_valid_std AS date,
    fd.city_name,
    fd.country_desc,
    ZEROIFNULL(SUM(odv.price)) AS daily_sales,
    ROUND(AVG(fd.avg_temperature_air_2m_f),2) AS avg_temperature_fahrenheit,
    ROUND(AVG(frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.fahrenheit_to_celsius(fd.avg_temperature_air_2m_f)),2) AS avg_temperature_celsius,
    ROUND(AVG(fd.tot_precipitation_in),2) AS avg_precipitation_inches,
    ROUND(AVG(frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.inch_to_millimeter(fd.tot_precipitation_in)),2) AS avg_precipitation_millimeters,
    MAX(fd.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.harmonized.daily_weather_v fd
LEFT JOIN frostbyte_tasty_bytes_v2_<firstname>_<lastname>.harmonized.orders_v odv
    ON fd.date_valid_std = DATE(odv.order_ts)
    AND fd.city_name = odv.primary_city
    AND fd.country_desc = odv.country
WHERE 1=1
    AND fd.country_desc = 'Germany'
    AND fd.city = 'Hamburg'
    AND fd.yyyy_mm = '2022-02'
GROUP BY fd.date_valid_std, fd.city_name, fd.country_desc
ORDER BY fd.date_valid_std ASC;


-- in the query above we are now able to see sales and weather for each day
-- let's remove our filters and promote this to an Analytics view
CREATE OR REPLACE VIEW frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.daily_city_metrics_v
COMMENT = 'Daily Weather Source Metrics and Orders Data for our Cities'
    AS
SELECT 
    fd.date_valid_std AS date,
    fd.city_name,
    fd.country_desc,
    ZEROIFNULL(SUM(odv.price)) AS daily_sales,
    ROUND(AVG(fd.avg_temperature_air_2m_f),2) AS avg_temperature_fahrenheit,
    ROUND(AVG(frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.fahrenheit_to_celsius(fd.avg_temperature_air_2m_f)),2) AS avg_temperature_celsius,
    ROUND(AVG(fd.tot_precipitation_in),2) AS avg_precipitation_inches,
    ROUND(AVG(frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.inch_to_millimeter(fd.tot_precipitation_in)),2) AS avg_precipitation_millimeters,
    MAX(fd.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.harmonized.daily_weather_v fd
LEFT JOIN frostbyte_tasty_bytes_v2_<firstname>_<lastname>.harmonized.orders_v odv
    ON fd.date_valid_std = DATE(odv.order_ts)
    AND fd.city_name = odv.primary_city
    AND fd.country_desc = odv.country
WHERE 1=1
GROUP BY fd.date_valid_std, fd.city_name, fd.country_desc;


-- using our new Analytics View, we can showcase how we will empower our financial analysts to
-- dive deeper into their own questions with enriched data
SELECT 
    dcm.date,
    dcm.city_name,
    dcm.country_desc,
    dcm.daily_sales,
    dcm.avg_temperature_fahrenheit,
    dcm.avg_temperature_celsius,
    dcm.avg_precipitation_inches,
    dcm.avg_precipitation_millimeters,
    dcm.max_wind_speed_100m_mph
FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.daily_city_metrics_v dcm
WHERE 1=1
    AND dcm.country_desc = 'Germany'
    AND dcm.city_name = 'Hamburg'
    AND dcm.date BETWEEN '2022-02-01' AND '2022-03-01'
ORDER BY date DESC;


/*- Part 2 -*/
/*----------------------------------------------------------------------------------
    Step 1 - 
        Having seen a free, public Snowflake listing we can now look at a personalized
        listing as Tasty Bytes recieves one from Safegraph, a leader in POI data.
        
        This data helps further our understanding of our truck locations and sets
        us up for cutting edge Geospatial analysis.
----------------------------------------------------------------------------------*/

-- let's start by taking a look at the Safegraph Listing we use from the Marketplace

    /*--- 
     -> Snowsight Home Button
         -> Marketplace
             -> Search: POI Data: SafeGraph
             
    Global Points of Interest (POI) - SafeGraph Places
        Build products on top of accurate and up-to-date POIs from around the world. This dataset contains
        business listing information such as location name, street address, industry, lat/long and brand.
        Covers locations including but not limited to major retail chains, local businesses, convenience stores,
        hotels, airports, schools, hospitals & more.
          - Documentation: https://docs.safegraph.com/docs#section-core-places
    ---*/

-- before we dive into how Tasty Bytes can leverage this SafeGraph POI data, let's first
-- query the raw Safegraph POI to look for Museums in Paris, France
SELECT 
    cpg.placekey,
    cpg.location_name,
    cpg.longitude,
    cpg.latitude,
    cpg.street_address,
    cpg.city,
    cpg.country,
    cpg.polygon_wkt
FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_safegraph.core_poi_geometry cpg
WHERE 1=1
    AND cpg.top_category = 'Museums, Historical Sites, and Similar Institutions'
    AND cpg.sub_category = 'Museums'
    AND cpg.city = 'Paris'
    AND cpg.country = 'France';


/*----------------------------------------------------------------------------------
    Step 2 - 
        As our food trucks only visit two locations a day, our trucks need to be sure
        they are serving the best locations. To help make this process data driven,
        let's harmonize our first party data with Safegraph to see what categories
        our top selling locations typically fall into
----------------------------------------------------------------------------------*/

-- first, let's find our top 50 selling location_id's
SELECT TOP 50
    oh.location_id,
    SUM(oh.order_total) AS total_sales
FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.order_header oh
GROUP BY oh.location_id
ORDER BY total_sales DESC;


-- let's now use the query above as a Common Table Expression (CTE) and find the top
-- Safegraph Categories our high selling locations fall into
WITH _top_50_location_id AS
(
    SELECT TOP 50
        oh.location_id,
        SUM(oh.order_total) AS total_sales
    FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.order_header oh
    GROUP BY oh.location_id
    ORDER BY total_sales DESC
)
SELECT
    TOP 10 
    cpg.top_category,
    COUNT(DISTINCT t50.location_id) AS count_top_50_location
FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_safegraph.core_poi_geometry cpg
JOIN frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.location l 
    ON cpg.placekey = l.placekey
JOIN _top_50_location_id t50
    ON l.location_id = t50.location_id
GROUP BY cpg.top_category
ORDER BY count_top_50_location DESC;


/*----------------------------------------------------------------------------------
   While getting this insight into categories is great, the other large benefit
   to our Safegraph share is the Geo Coordinates (Lat, Long). Please see the Geospatial
   vignette for a deeper dive into the art of the possible.
----------------------------------------------------------------------------------*/



/**********************************************************************/
/*------               Vignette Reset Scripts                   ------*/
/**********************************************************************/

DROP VIEW IF EXISTS frostbyte_tasty_bytes_v2_<firstname>_<lastname>.harmonized.daily_weather_v;
DROP VIEW IF EXISTS frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.daily_city_metrics_v;
DROP FUNCTION IF EXISTS frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.fahrenheit_to_celsius(NUMBER(35,4));
DROP FUNCTION IF EXISTS frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.inch_to_millimeter(NUMBER(35,4));