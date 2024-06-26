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
Vignette:     Geospatial
Script:       geospatial_step_1_tb.sql     	
Create Date:  2023-01-13
Author:       Jacob Kranzler
Copyright(c): 2022 Snowflake Inc. All rights reserved.
****************************************************************************************************
Description: 
    Geospatial
    1 - Create a Geographic Point from Safegraph Latitude and Longitude Data
    2 - Calculate Distance between our Top Selling Locations
    3 - Collect Coordinates and find a Bounding Polygon, its area and Center Point of our Top Selling Locations
    4 - Derive Locations furthest away from our Top Selling Hub
          
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2023-01-13          Jacob Kranzler      Initial Release
2023-03-01          David Card          Updated lat/lon order in st_makepoint function
***************************************************************************************************/

/*----------------------------------------------------------------------------------
    Step 1 - 
      Tasty Bytes receives Safegraph POI data covering the locations our truck serve,
      live from the Snowflake Marketplace via their personalized Global Points of Interest
      listing. Within the POI metrics provided are latitude and longitude coordinates which
      will allow us to begin conducting Geospatial analysis.     
----------------------------------------------------------------------------------*/

-- first we will assume our role and warehouse
USE ROLE tasty_data_engineer;
USE WAREHOUSE tasty_de_wh;

/* CRITICAL STEP: DO NOT SKIP */
-- FIND AND REPLACE THE STRING "<firstname>_<lastname>" WITH YOUR NAME EX. "SRIDHAR_RAMASWAMY"
CREATE DATABASE IF NOT EXISTS frostbyte_tasty_bytes_v2_<firstname>_<lastname> CLONE frostbyte_tasty_bytes_v2;
/* ========================== */


-- to begin our analysis, let's first find our top selling locations in Paris for 2022
SELECT TOP 10
    o.location_id,
    o.location_name,
    SUM(o.price) AS total_sales_usd
FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.orders_v o
WHERE 1=1
    AND o.primary_city = 'Paris'
    AND YEAR(o.date) = 2022
GROUP BY o.location_id, o.location_name
ORDER BY total_sales_usd DESC;


-- now, using the latitude and longitude for our locations provided by Safegraph let's create a Geographic Point 
    
    /** 
      • ST_MAKEPOINT: Constructs a GEOGRAPHY object that represents a point with the specified longitude and latitude.
    **/

SELECT TOP 10 
    o.location_id,
    ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point,
    SUM(o.price) AS total_sales_usd
FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.orders_v o
WHERE 1=1
    AND o.primary_city = 'Paris'
    AND YEAR(o.date) = 2022
GROUP BY o.location_id, o.latitude, o.longitude
ORDER BY total_sales_usd DESC;


/*----------------------------------------------------------------------------------
    Step 2 - 
     Starting with our Geographic Point, we can now begin to dive into some of 
     the powerful Geospatial functions Snowflake offers natively. Let's first start
     with calculating the distances between those top selling locations.
----------------------------------------------------------------------------------*/

-- using the ST_DISTANCE function, let's calculate the distance between the top locations

    /** 
      • ST_DISTANCE: Returns the minimum geodesic distance between two GEOGRAPHY 
       or the minimum Euclidean distance between two GEOMETRY objects.
    **/
    
WITH _top_10_locations AS 
(
    SELECT TOP 10
        o.location_id,
        ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point,
        SUM(o.price) AS total_sales_usd
    FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.orders_v o
    WHERE 1=1
        AND o.primary_city = 'Paris'
        AND YEAR(o.date) = 2022
    GROUP BY o.location_id, o.latitude, o.longitude
    ORDER BY total_sales_usd DESC
)
SELECT
    a.location_id,
    b.location_id,
    ROUND(ST_DISTANCE(a.geo_point, b.geo_point)/1609,2) AS geography_distance_miles,
    ROUND(ST_DISTANCE(a.geo_point, b.geo_point)/1000,2) AS geography_distance_kilometers
FROM _top_10_locations a  
JOIN _top_10_locations b
    ON a.location_id <> b.location_id -- avoid calculating the distance between the point itself
QUALIFY a.location_id <> LAG(b.location_id) OVER (ORDER BY geography_distance_miles) -- avoid duplicate: a to b, b to a distances
ORDER BY geography_distance_miles;


/*----------------------------------------------------------------------------------
    Step 3 - 
     Understanding how to calculate distance, we will now collect Coordinates for those
     same top selling locations so we can build a Minimum Bounding Polygon, calculate the
     area of it and determine the Center Point.
----------------------------------------------------------------------------------*/

-- first, let's create a geographic collection of the points and build our minimum bounding polygon

    /** 
      • ST_NPOINTS: Returns the number of points in a GEOGRAPHY or GEOGRAPHY object.
      • ST_COLLECT: This function combines all the GEOGRAPHY objects in a column into one GEOGRAPHY object.
      • ST_ENVELOPE: Returns the minimum bounding box (a rectangular “envelope”) that encloses a specified
          GEOGRAPHY or GEOMETRY object.
      • ST_AREA: Returns the area of the Polygon(s) in a GEOGRAPHY or GEOMETRY object.
    **/

WITH _top_10_locations AS 
(
    SELECT TOP 10
        o.location_id,
        ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point,
        SUM(o.price) AS total_sales_usd
    FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.orders_v o
    WHERE 1=1
        AND o.primary_city = 'Paris'
        AND YEAR(o.date) = 2022
    GROUP BY o.location_id, o.latitude, o.longitude
    ORDER BY total_sales_usd DESC
)
SELECT
    ST_NPOINTS(ST_COLLECT(tl.geo_point)) AS count_points_in_collection,
    ST_COLLECT(tl.geo_point) AS collection_of_points,
    ST_ENVELOPE(collection_of_points) AS minimum_bounding_polygon,
    ROUND(ST_AREA(minimum_bounding_polygon)/1000000,2) AS area_in_sq_kilometers
FROM _top_10_locations tl;


-- now let's find the Geometric Center Point for these key locations

    /** 
      • ST_CENTROID: Returns the Point representing the geometric center of a GEOGRAPHY or GEOMETRY object.
    **/
    
WITH _top_10_locations AS 
(
    SELECT TOP 10
        o.location_id,
        ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point,
        SUM(o.price) AS total_sales_usd
    FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.orders_v o
    WHERE 1=1
        AND o.primary_city = 'Paris'
        AND YEAR(o.date) = 2022
    GROUP BY o.location_id, o.latitude, o.longitude
    ORDER BY total_sales_usd DESC
)
SELECT  
    ST_COLLECT(tl.geo_point) AS collect_points,
    ST_CENTROID(collect_points) AS geometric_center_point
FROM _top_10_locations tl;


-- to assist in our next query, let's copy (CMD + C) the geometric_center_point result from above and SET it as a SQL Variable
SET center_point = '*** REPLACE THIS WITH THE geometric_center_point FROM ABOVE BEFORE MOVING ON ***';


/*----------------------------------------------------------------------------------
    Step 4 - 
        Using the Variable we have set, we can now calculate distances between
        locations and our top selling center point.
----------------------------------------------------------------------------------*/

-- let's find the top 50 locations furthest away from our Top Selling Center Point
-- so that we can further analyze these later to see if they should be removed from our truck schedules
WITH _2022_paris_locations AS
(
    SELECT DISTINCT 
        o.location_id,
        o.location_name,
        ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point
    FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.orders_v o
    WHERE 1=1
        AND o.primary_city = 'Paris'
        AND YEAR(o.date) = 2022
)
SELECT TOP 50
    ll.location_id,
    ll.location_name,
    ROUND(ST_DISTANCE(ll.geo_point, TO_GEOGRAPHY($center_point))/1000,2) AS kilometer_from_top_selling_center
FROM _2022_paris_locations ll
ORDER BY kilometer_from_top_selling_center DESC;



/**********************************************************************/
/*------               Vignette Reset Scripts                   ------*/
/**********************************************************************/

UNSET center_point;