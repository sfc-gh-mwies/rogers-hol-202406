![rogers-snowflake](/img/Screenshot%202024-06-07%20at%203.21.17%E2%80%AFPM.png)
# Hands-On Lab 2024-06-26 - Snowflake Fundamentals+

## [Intro Presentation](Snowflake%20101.pdf): Snowflake 101
  * Background on Snowflake's 10-year evolution
  * System Defined Roles and Privileges, Role Based Access Control
  * Storage, Caching and time travel
  * Cloud services layer, Information Schema, SNOWFLAKE database, Query History
  * Lab Setup and Explanation

## Hands-On Lab Part 1
### [01 - Snowflake Fundamentals](/01%20-%20Snowflake%20Fundamentals)
* Explore UI, Databases, Objects
* Virtual Warehouses and Settings
* Development: Zero Copy Cloning, Time-Travel for Table Restore, Table Swap, Drop and Undrop
* Basic Dashboarding
* Semi-Structured Data
* Semi-Structured Data and the Variant Data Type
* Querying Semi-Structured Data via Dot and Bracket Notation + Flatten
* Providing Flattened Data to Business Users 

## Hands-On Lab Part 2

### -- [Tasty Bytes Company Overview](https://github.com/sfc-gh-mwies/rogers-hol-202406/blob/main/TB_OVERVIEW.md) --
📢 Review before continuing ☝️

### [02 - Data Governance](https://github.com/sfc-gh-mwies/rogers-hol-202406/tree/main/02%20-%20Governance)
* Dynamic Data Masking and Row-Access Policies

### [03 - Data Engineering](03%20-%20Data%20Engineering)
* Data Engineering Pipelines: Streams, Tasks & Dynamic Tables
* Stored Procedures, User-Defined Functions

### [04 - Collaboration](/04%20-%20Collaboration)
* Quick secure data sharing example (direct share from Snowflake demo account)
* Investigating Days with Zero Sales
* Acquiring Weather Source Data from the Snowflake Marketplace 
* Democratizing Data for Business Users

### [05 - Geospatial](/05%20-%20Geospatial)
* Creating Geography Points from Latitude and Longitude
* Calculating Straight Line Distance between Points
* Collecting Coordinates, Creating a Bounding Polygon & Finding its Center Point
* Finding Locations Furthest Away from our Top Selling Hub
* Geospatial Analysis with H3 (Hexagonal Hierarchical Geospatial Indexing System)

### (Optional) Snowsight Dashboard
- [**snowsight_dashboard_org_city_metrics_build_walkthrough_tb.md**](https://github.com/snowflakecorp/frostbytes/blob/main/Tasty%20Bytes/40%20-%20analytics/Snowsight%20-%20Organization%20and%20City%20Metrics%20Dashboard/snowsight_dashboard_org_city_metrics_build_walkthrough_tb.md)
  - This dashboard covers Organization and City (using a custom filter) metrics related to Tasty Bytes. Tiles within include details on total sales, total orders, hourly sales, top-selling locations and most loyal customers.
