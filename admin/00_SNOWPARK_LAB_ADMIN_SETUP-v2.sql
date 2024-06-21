/***************************************************************************************************
  _______           _            ____          _             
 |__   __|         | |          |  _ \        | |            
    | |  __ _  ___ | |_  _   _  | |_) | _   _ | |_  ___  ___ 
    | | / _` |/ __|| __|| | | | |  _ < | | | || __|/ _ \/ __|
    | || (_| |\__ \| |_ | |_| | | |_) || |_| || |_|  __/\__ \
    |_| \__,_||___/ \__| \__, | |____/  \__, | \__|\___||___/
                          __/ |          __/ |               
                         |___/          |___/            
Demo:         Tasty Bytes Demo
Version:      v2.1
Vignette:     Setup
Script:       setup_step_2_snowflake_account_tb_v2.sql         
Create Date:  2023-06-02
Author:       Jacob Kranzler
Copyright(c): 2023 Snowflake Inc. All rights reserved.
****************************************************************************************************
Description: 
   Setup V2 for Tasty Bytes SE Scale Demo
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2023-10-18        Jacob Kranzler      Added GRANT on PUBLIC to Sysadmin
2023-06-02        Jacob Kranzler      Initial Release
***************************************************************************************************/
 

USE ROLE accountadmin;

USE ROLE sysadmin; 

-- this is here to confirm sysadmin is being used for database creation below.
SELECT CURRENT_ROLE();

-- create frostbyte_tasty_bytes_v2 database
CREATE OR REPLACE DATABASE frostbyte_tasty_bytes_v2;

-- create raw_pos schema
CREATE OR REPLACE SCHEMA frostbyte_tasty_bytes_v2.raw_pos;

-- create raw_ schema
CREATE OR REPLACE SCHEMA frostbyte_tasty_bytes_v2.raw_customer;

-- create raw_customer schema
CREATE OR REPLACE SCHEMA frostbyte_tasty_bytes_v2.raw_supply_chain;

-- create raw_customer schema
CREATE OR REPLACE SCHEMA frostbyte_tasty_bytes_v2.raw_truck;

-- create raw_safegraph schema
CREATE OR REPLACE SCHEMA frostbyte_tasty_bytes_v2.raw_safegraph;

-- create harmonized schema
CREATE OR REPLACE SCHEMA frostbyte_tasty_bytes_v2.harmonized;

-- create analytics schema
CREATE OR REPLACE SCHEMA frostbyte_tasty_bytes_v2.analytics;

-- create warehouses
CREATE WAREHOUSE IF NOT EXISTS demo_build_wh
    WAREHOUSE_SIZE = 'xxxlarge'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'demo build warehouse for frostbyte assets';
    
CREATE WAREHOUSE IF NOT EXISTS tasty_de_wh
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'data engineering warehouse for tasty bytes';

CREATE WAREHOUSE IF NOT EXISTS tasty_ds_wh
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'data science warehouse for tasty bytes';

CREATE WAREHOUSE IF NOT EXISTS tasty_bi_wh
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'business intelligence warehouse for tasty bytes';

CREATE OR REPLACE WAREHOUSE tasty_dev_wh
    WAREHOUSE_SIZE = 'large'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'developer warehouse for tasty bytes';

CREATE WAREHOUSE IF NOT EXISTS tasty_data_app_wh
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'data app warehouse for tasty bytes';

CREATE OR REPLACE WAREHOUSE tasty_snowpark_wh
    WAREHOUSE_TYPE = 'snowpark-optimized' -- create snowpark_optimized_wh
    WAREHOUSE_SIZE = '4X-Large'
    AUTO_RESUME = TRUE
    AUTO_SUSPEND = 60
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'snowpark optimized warehouse for tasty bytes dsci';


-- create roles
USE ROLE securityadmin;

CREATE ROLE IF NOT EXISTS tasty_admin
    COMMENT = 'admin for tasty bytes';
    
CREATE ROLE IF NOT EXISTS tasty_data_engineer
    COMMENT = 'data engineer for tasty bytes';
      
CREATE ROLE IF NOT EXISTS tasty_data_scientist
    COMMENT = 'data scientist for tasty bytes';
    
CREATE ROLE IF NOT EXISTS tasty_bi
    COMMENT = 'business intelligence for tasty bytes';
    
CREATE ROLE IF NOT EXISTS tasty_data_app
    COMMENT = 'data application developer for tasty bytes';
    
CREATE ROLE IF NOT EXISTS tasty_dev
    COMMENT = 'developer for tasty bytes';


-- role hierarchy
GRANT ROLE tasty_admin TO ROLE sysadmin;
GRANT ROLE tasty_data_engineer TO ROLE tasty_admin;
GRANT ROLE tasty_data_scientist TO ROLE tasty_admin;
GRANT ROLE tasty_bi TO ROLE tasty_admin;
GRANT ROLE tasty_data_app TO ROLE tasty_admin;
GRANT ROLE tasty_dev TO ROLE tasty_data_engineer;
GRANT ROLE tasty_dev TO ROLE public; --this is needed for dbt vignette

-- privilege grants
USE ROLE accountadmin;

GRANT EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT TO ROLE tasty_data_scientist;
GRANT EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT TO ROLE tasty_data_engineer;

GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE tasty_data_engineer;

GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE tasty_admin;

USE ROLE securityadmin;

GRANT USAGE ON DATABASE frostbyte_tasty_bytes_v2 TO ROLE tasty_admin;
GRANT USAGE ON DATABASE frostbyte_tasty_bytes_v2 TO ROLE tasty_data_engineer;
GRANT USAGE ON DATABASE frostbyte_tasty_bytes_v2 TO ROLE tasty_data_scientist;
GRANT USAGE ON DATABASE frostbyte_tasty_bytes_v2 TO ROLE tasty_bi;
GRANT USAGE ON DATABASE frostbyte_tasty_bytes_v2 TO ROLE tasty_data_app;
GRANT USAGE ON DATABASE frostbyte_tasty_bytes_v2 TO ROLE tasty_dev;

GRANT USAGE ON ALL SCHEMAS IN DATABASE frostbyte_tasty_bytes_v2 TO ROLE tasty_admin;
GRANT USAGE ON ALL SCHEMAS IN DATABASE frostbyte_tasty_bytes_v2 TO ROLE tasty_data_engineer;
GRANT USAGE ON ALL SCHEMAS IN DATABASE frostbyte_tasty_bytes_v2 TO ROLE tasty_data_scientist;
GRANT USAGE ON ALL SCHEMAS IN DATABASE frostbyte_tasty_bytes_v2 TO ROLE tasty_bi;
GRANT USAGE ON ALL SCHEMAS IN DATABASE frostbyte_tasty_bytes_v2 TO ROLE tasty_data_app;
GRANT USAGE ON ALL SCHEMAS IN DATABASE frostbyte_tasty_bytes_v2 TO ROLE tasty_dev;

GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.public TO ROLE sysadmin;

GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_pos TO ROLE tasty_admin;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_pos TO ROLE tasty_data_engineer;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_pos TO ROLE tasty_data_scientist;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_pos TO ROLE tasty_bi;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_pos TO ROLE tasty_data_app;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_pos TO ROLE tasty_dev;

GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_customer TO ROLE tasty_admin;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_customer TO ROLE tasty_data_engineer;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_customer TO ROLE tasty_data_scientist;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_customer TO ROLE tasty_bi;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_customer TO ROLE tasty_data_app;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_customer TO ROLE tasty_dev;

GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_safegraph TO ROLE tasty_admin;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_safegraph TO ROLE tasty_data_engineer;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_safegraph TO ROLE tasty_data_scientist;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_safegraph TO ROLE tasty_bi;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_safegraph TO ROLE tasty_data_app;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_safegraph TO ROLE tasty_dev;

GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_supply_chain TO ROLE tasty_admin;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_supply_chain TO ROLE tasty_data_engineer;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_supply_chain TO ROLE tasty_data_scientist;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_supply_chain TO ROLE tasty_bi;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_supply_chain TO ROLE tasty_data_app;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_supply_chain TO ROLE tasty_dev;

GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_truck TO ROLE tasty_admin;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_truck TO ROLE tasty_data_engineer;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_truck TO ROLE tasty_data_scientist;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_truck TO ROLE tasty_bi;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_truck TO ROLE tasty_data_app;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.raw_truck TO ROLE tasty_dev;

GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.harmonized TO ROLE tasty_admin;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.harmonized TO ROLE tasty_data_engineer;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.harmonized TO ROLE tasty_data_scientist;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.harmonized TO ROLE tasty_bi;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.harmonized TO ROLE tasty_data_app;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.harmonized TO ROLE tasty_dev;

GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_admin;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_data_engineer;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_data_scientist;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_bi;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_data_app;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_dev;

-- warehouse grants
GRANT ALL ON WAREHOUSE demo_build_wh TO ROLE sysadmin;

GRANT OWNERSHIP ON WAREHOUSE tasty_de_wh TO ROLE tasty_admin COPY CURRENT GRANTS;
GRANT ALL ON WAREHOUSE tasty_de_wh TO ROLE tasty_admin;
GRANT ALL ON WAREHOUSE tasty_de_wh TO ROLE tasty_data_engineer;

GRANT ALL ON WAREHOUSE tasty_ds_wh TO ROLE tasty_admin;
GRANT ALL ON WAREHOUSE tasty_ds_wh TO ROLE tasty_data_scientist;

GRANT ALL ON WAREHOUSE tasty_data_app_wh TO ROLE tasty_admin;
GRANT ALL ON WAREHOUSE tasty_data_app_wh TO ROLE tasty_data_app;

GRANT ALL ON WAREHOUSE tasty_bi_wh TO ROLE tasty_admin;
GRANT ALL ON WAREHOUSE tasty_bi_wh TO ROLE tasty_bi;

GRANT ALL ON WAREHOUSE tasty_dev_wh TO ROLE tasty_admin;
GRANT ALL ON WAREHOUSE tasty_dev_wh TO ROLE tasty_data_engineer;
GRANT ALL ON WAREHOUSE tasty_dev_wh TO ROLE tasty_dev;

GRANT ALL ON WAREHOUSE tasty_snowpark_wh TO ROLE tasty_admin;
GRANT ALL ON WAREHOUSE tasty_snowpark_wh TO ROLE tasty_data_engineer;
GRANT ALL ON WAREHOUSE tasty_snowpark_wh TO ROLE tasty_dev;


-- future grants
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_pos TO ROLE tasty_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_pos TO ROLE tasty_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_pos TO ROLE tasty_data_scientist;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_pos TO ROLE tasty_bi;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_pos TO ROLE tasty_data_app;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_pos TO ROLE tasty_dev;

GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_customer TO ROLE tasty_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_customer TO ROLE tasty_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_customer TO ROLE tasty_data_scientist;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_customer TO ROLE tasty_bi;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_customer TO ROLE tasty_data_app;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_customer TO ROLE tasty_dev;

GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_safegraph TO ROLE tasty_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_safegraph TO ROLE tasty_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_safegraph TO ROLE tasty_data_scientist;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_safegraph TO ROLE tasty_bi;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_safegraph TO ROLE tasty_data_app;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_safegraph TO ROLE tasty_dev;

GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_supply_chain TO ROLE tasty_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_supply_chain TO ROLE tasty_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_supply_chain TO ROLE tasty_data_scientist;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_supply_chain TO ROLE tasty_bi;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_supply_chain TO ROLE tasty_data_app;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_supply_chain TO ROLE tasty_dev;

GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_truck TO ROLE tasty_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_truck TO ROLE tasty_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_truck TO ROLE tasty_data_scientist;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_truck TO ROLE tasty_bi;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_truck TO ROLE tasty_data_app;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.raw_truck TO ROLE tasty_dev;

GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.harmonized TO ROLE tasty_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.harmonized TO ROLE tasty_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.harmonized TO ROLE tasty_data_scientist;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.harmonized TO ROLE tasty_bi;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.harmonized TO ROLE tasty_data_app;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.harmonized TO ROLE tasty_dev;

GRANT ALL ON FUTURE VIEWS IN SCHEMA frostbyte_tasty_bytes_v2.harmonized TO ROLE tasty_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA frostbyte_tasty_bytes_v2.harmonized TO ROLE tasty_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA frostbyte_tasty_bytes_v2.harmonized TO ROLE tasty_data_scientist;
GRANT ALL ON FUTURE VIEWS IN SCHEMA frostbyte_tasty_bytes_v2.harmonized TO ROLE tasty_bi;
GRANT ALL ON FUTURE VIEWS IN SCHEMA frostbyte_tasty_bytes_v2.harmonized TO ROLE tasty_data_app;
GRANT ALL ON FUTURE VIEWS IN SCHEMA frostbyte_tasty_bytes_v2.harmonized TO ROLE tasty_dev;

GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_data_scientist;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_bi;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_data_app;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_dev;

GRANT ALL ON FUTURE VIEWS IN SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_data_scientist;
GRANT ALL ON FUTURE VIEWS IN SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_bi;
GRANT ALL ON FUTURE VIEWS IN SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_data_app;
GRANT ALL ON FUTURE VIEWS IN SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_dev;

GRANT ALL ON FUTURE FUNCTIONS IN SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_data_scientist;

GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_admin;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_data_engineer;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_data_scientist;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_bi;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_data_app;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA frostbyte_tasty_bytes_v2.analytics TO ROLE tasty_dev;

-- Apply Masking Policy Grants
GRANT CREATE TAG ON SCHEMA frostbyte_tasty_bytes_v2.raw_customer TO ROLE tasty_admin;
GRANT CREATE TAG ON SCHEMA frostbyte_tasty_bytes_v2.raw_customer TO ROLE tasty_data_engineer;

USE ROLE accountadmin;
GRANT APPLY TAG ON ACCOUNT TO ROLE tasty_admin;
GRANT APPLY TAG ON ACCOUNT TO ROLE tasty_data_engineer;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE tasty_admin;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE tasty_data_engineer;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE tasty_admin;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE tasty_data_engineer;

USE ROLE sysadmin;
USE WAREHOUSE demo_build_wh;


-- scale warehouse for table creation
ALTER WAREHOUSE demo_build_wh SET warehouse_size = '3X-Large';

/*-----------------------------------------------------------------------------
    ,-.   ,.  ,   . 
    |  ) /  \ | . | 
    |-<  |--| | ) ) 
    |  \ |  | |/|/  
    '  ' '  ' ' '                                                 
-------------------------------------------------------------------------------
    raw build
-----------------------------------------------------------------------------*/

-- raw_pos table build
CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_pos.country
  AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_pos.country;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_pos.franchise
  AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_pos.franchise;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_pos.location
  AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_pos.location;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_pos.menu
  AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_pos.menu;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_pos.truck
  AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_pos.truck;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_pos.order_header
  AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_pos.order_header;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_pos.order_detail
  AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_pos.order_detail;

-- raw_customer table build
CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_customer.customer_loyalty
  AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_customer.customer_loyalty;

-- raw_safegraph table build
CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_safegraph.core_poi_geometry
  AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_safegraph.core_poi_geometry;

-- raw_supply_chain table build
CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_supply_chain.dim_date
    AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_supply_chain.dim_date;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_supply_chain.distribution_detail
    AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_supply_chain.distribution_detail;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_supply_chain.distribution_header
    AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_supply_chain.distribution_header;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_supply_chain.purchase_order_detail
    AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_supply_chain.purchase_order_detail;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_supply_chain.purchase_order_header
    AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_supply_chain.purchase_order_header;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_supply_chain.recipe
    AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_supply_chain.recipe;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_supply_chain.item
    AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_supply_chain.item;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_supply_chain.warehouse
    AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_supply_chain.warehouse;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_supply_chain.vendor
    AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_supply_chain.vendor;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_supply_chain.price_elasticity
    AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_supply_chain.price_elasticity;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_supply_chain.eod_stock_assignment
    AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_supply_chain.eod_stock_assignment;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_supply_chain.menu_prices
    AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_supply_chain.menu_prices;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_supply_chain.item_prices
    AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_supply_chain.item_prices;

-- raw_truck table build
CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_truck.truck_shift
    AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_truck.truck_shift;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_truck.dim_shift
    AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_truck.dim_shift;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.raw_truck.inventory_queue
    AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.raw_truck.inventory_queue;


/*-----------------------------------------------------------------------------
    .  .  ,.  ,-.  .   ,  ,-.  .  . , ,---, ,--. ,-.  
    |  | /  \ |  ) |\ /| /   \ |\ | |    /  |    |  \ 
    |--| |--| |-<  | V | |   | | \| |   /   |-   |  | 
    |  | |  | |  \ |   | \   / |  | |  /    |    |  / 
    '  ' '  ' '  ' '   '  `-'  '  ' ' '---' `--' `-'                                              
-------------------------------------------------------------------------------
    harmonized build
-----------------------------------------------------------------------------*/


CREATE OR REPLACE DYNAMIC TABLE frostbyte_tasty_bytes_v2.harmonized.menu_item_aggregate_dt
LAG = '1 minute'
WAREHOUSE = 'TASTY_DE_WH'
    AS
WITH _point_in_time_cogs AS
(
    SELECT DISTINCT
        r.menu_item_id,
        ip.start_date,
        ip.end_date,
        SUM(ip.unit_price * r.unit_quantity) 
            OVER (PARTITION BY r.menu_item_id, ip.start_date, ip.end_date)
                AS cost_of_menu_item_usd
    FROM frostbyte_tasty_bytes_v2.raw_supply_chain.item i
    JOIN frostbyte_tasty_bytes_v2.raw_supply_chain.recipe r
        ON i.item_id = r.item_id
    JOIN frostbyte_tasty_bytes_v2.raw_supply_chain.item_prices ip
        ON ip.item_id = r.item_id
    ORDER BY r.menu_item_id, ip.start_date
)
SELECT 
    DATE(oh.order_ts) AS date,
    DAYOFWEEK(date) AS day_of_week,
    m.menu_type_id,
    m.truck_brand_name,
    m.menu_item_id,
    m.menu_item_name,
    CASE 
        WHEN pe.price IS NOT NULL THEN pe.price
        ELSE mp.sales_price_usd
    END AS sale_price,
    mp.sales_price_usd  AS base_price,
    ROUND(pitcogs.cost_of_menu_item_usd,2) AS cost_of_goods_usd,
    COUNT(DISTINCT oh.order_id) AS count_orders,
    SUM(od.quantity) AS total_quantity_sold,
    NULL AS competitor_price
FROM frostbyte_tasty_bytes_v2.raw_pos.order_header oh
JOIN frostbyte_tasty_bytes_v2.raw_pos.order_detail od
    ON oh.order_id = od.order_id
JOIN frostbyte_tasty_bytes_v2.raw_pos.menu m
    ON m.menu_item_id = od.menu_item_id
JOIN frostbyte_tasty_bytes_v2.raw_supply_chain.menu_prices mp
    ON mp.menu_item_id = m.menu_item_id
    AND DATE(oh.order_ts) BETWEEN mp.start_date AND mp.end_date
JOIN _point_in_time_cogs pitcogs
    ON pitcogs.menu_item_id = m.menu_item_id
    AND DATE(oh.order_ts) BETWEEN pitcogs.start_date AND pitcogs.end_date
LEFT JOIN frostbyte_tasty_bytes_v2.raw_supply_chain.price_elasticity pe
    ON pe.menu_item_id = m.menu_item_id
    AND pe.from_date <= DATE(oh.order_ts)
    AND pe.through_date >= DATE(oh.order_ts)
    AND pe.day_of_week = DAYOFWEEK(DATE(oh.order_ts))
GROUP BY date, day_of_week, m.menu_type_id, m.truck_brand_name, m.menu_item_id,
m.menu_item_name, sale_price, base_price, pitcogs.cost_of_menu_item_usd, competitor_price
ORDER BY date, m.menu_item_id;


CREATE OR REPLACE VIEW frostbyte_tasty_bytes_v2.harmonized.menu_item_cogs_and_price_v
    AS
SELECT DISTINCT
    r.menu_item_id,
    ip.start_date,
    ip.end_date,
    SUM(ip.unit_price * r.unit_quantity) 
        OVER (PARTITION BY r.menu_item_id, ip.start_date, ip.end_date)
            AS cost_of_menu_item_usd,
    mp.sales_price_usd
FROM frostbyte_tasty_bytes_v2.raw_supply_chain.item i
JOIN frostbyte_tasty_bytes_v2.raw_supply_chain.recipe r
    ON i.item_id = r.item_id
JOIN frostbyte_tasty_bytes_v2.raw_supply_chain.item_prices ip
    ON ip.item_id = r.item_id
JOIN frostbyte_tasty_bytes_v2.raw_supply_chain.menu_prices mp
    ON mp.menu_item_id = r.menu_item_id
    AND mp.start_date = ip.start_date
ORDER BY r.menu_item_id, ip.start_date;


CREATE OR REPLACE VIEW frostbyte_tasty_bytes_v2.harmonized.orders_v
    AS
SELECT 
    oh.order_id,
    oh.truck_id,
    oh.order_ts,
    od.order_detail_id,
    od.line_number,
    m.truck_brand_name,
    m.menu_type,
    t.primary_city,
    t.region,
    t.country,
    t.franchise_flag,
    t.franchise_id,
    f.first_name AS franchisee_first_name,
    f.last_name AS franchisee_last_name,
    l.location_id,
    cpg.placekey,
    cpg.location_name,
    cpg.top_category,
    cpg.sub_category,
    cpg.latitude,
    cpg.longitude,
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.children_count,
    cl.gender,
    cl.marital_status,
    od.menu_item_id,
    m.menu_item_name,
    od.quantity,
    od.unit_price,
    od.price,
    oh.order_amount,
    oh.order_tax_amount,
    oh.order_discount_amount,
    oh.order_total
FROM frostbyte_tasty_bytes_v2.raw_pos.order_detail od
JOIN frostbyte_tasty_bytes_v2.raw_pos.order_header oh
    ON od.order_id = oh.order_id
JOIN frostbyte_tasty_bytes_v2.raw_pos.truck t
    ON oh.truck_id = t.truck_id
JOIN frostbyte_tasty_bytes_v2.raw_pos.menu m
    ON od.menu_item_id = m.menu_item_id
JOIN frostbyte_tasty_bytes_v2.raw_pos.franchise f
    ON t.franchise_id = f.franchise_id
JOIN frostbyte_tasty_bytes_v2.raw_pos.location l
    ON oh.location_id = l.location_id
JOIN frostbyte_tasty_bytes_v2.raw_safegraph.core_poi_geometry cpg
    ON cpg.placekey = l.placekey
LEFT JOIN frostbyte_tasty_bytes_v2.raw_customer.customer_loyalty cl
    ON oh.customer_id = cl.customer_id;

CREATE OR REPLACE VIEW frostbyte_tasty_bytes_v2.harmonized.customer_loyalty_metrics_v
    AS
SELECT 
    cl.customer_id,
    cl.city,
    cl.country,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    SUM(oh.order_total) AS total_sales,
    ARRAY_AGG(DISTINCT oh.location_id) AS visited_location_ids_array
FROM frostbyte_tasty_bytes_v2.raw_customer.customer_loyalty cl
JOIN frostbyte_tasty_bytes_v2.raw_pos.order_header oh
ON cl.customer_id = oh.customer_id
GROUP BY cl.customer_id, cl.city, cl.country, cl.first_name,
cl.last_name, cl.phone_number, cl.e_mail;


CREATE OR REPLACE VIEW frostbyte_tasty_bytes_v2.harmonized.location_detail_v
  AS
WITH _location_point AS
(SELECT 
    cpg.placekey,
    l.location_id,
    cpg.city,
    cpg.country,
    ST_MAKEPOINT(cpg.longitude, cpg.latitude) AS geo_point
FROM frostbyte_tasty_bytes_v2.raw_safegraph.core_poi_geometry cpg
LEFT JOIN frostbyte_tasty_bytes_v2.raw_pos.location l
    ON cpg.placekey = l.placekey),
_distance_between_locations AS
(SELECT 
    a.placekey,
    a.location_id,
    b.placekey AS placekey_2,
    b.location_id AS location_id_2,
    a.city,
    a.country,
    ROUND(ST_DISTANCE(a.geo_point, b.geo_point)/1609,2) AS geography_distance_miles
FROM _location_point a
JOIN _location_point b
    ON a.city = b.city
    AND a.country = b.country
    AND a.placekey <> b.placekey
),
_locations_within_half_mile AS
(
SELECT
    dbl.placekey,
    dbl.location_id,
    COUNT(DISTINCT dbl.placekey_2) AS count_locations_within_half_mile
FROM _distance_between_locations dbl
WHERE dbl.geography_distance_miles <= 0.5
GROUP BY dbl.location_id, dbl.placekey
),
_locations_within_mile AS
(
SELECT
    dbl.placekey,
    dbl.location_id,
    COUNT(DISTINCT dbl.placekey_2) AS count_locations_within_mile
FROM _distance_between_locations dbl
WHERE dbl.geography_distance_miles <= 1
GROUP BY dbl.location_id, dbl.placekey
)
SELECT
    l.location_id,
    ZEROIFNULL(hm.count_locations_within_half_mile) AS count_locations_within_half_mile,
    ZEROIFNULL(m.count_locations_within_mile) AS count_locations_within_mile,
    cpg.placekey,
    cpg.location_name,
    cpg.top_category,
    cpg.sub_category,
    cpg.naics_code,
    cpg.latitude,
    cpg.longitude,
    ST_MAKEPOINT(cpg.longitude, cpg.latitude) AS geo_point,
    cpg.street_address,
    cpg.city,
    c.city_population,
    cpg.region,
    cpg.country,
    cpg.polygon_wkt,
    cpg.geometry_type
FROM frostbyte_tasty_bytes_v2.raw_pos.location l
JOIN frostbyte_tasty_bytes_v2.raw_safegraph.core_poi_geometry cpg
    ON l.placekey = cpg.placekey
JOIN frostbyte_tasty_bytes_v2.raw_pos.country c
    ON l.city = c.city
LEFT JOIN _locations_within_half_mile hm
    ON l.location_id = hm.location_id
LEFT JOIN _locations_within_mile m
    ON l.location_id = m.location_id;


CREATE OR REPLACE VIEW frostbyte_tasty_bytes_v2.harmonized.order_item_cost_v 
    AS
WITH _menu_item_cogs_and_price AS
(
    SELECT DISTINCT
        r.menu_item_id,
        ip.start_date,
        ip.end_date,
        SUM(ip.unit_price * r.unit_quantity) OVER (PARTITION BY r.menu_item_id, ip.start_date, ip.end_date) AS cost_of_goods_usd,
        mp.sales_price_usd AS base_price
    FROM frostbyte_tasty_bytes_v2.raw_supply_chain.item i
    JOIN frostbyte_tasty_bytes_v2.raw_supply_chain.recipe r
        ON i.item_id = r.item_id
    JOIN frostbyte_tasty_bytes_v2.raw_supply_chain.item_prices ip
        ON ip.item_id = r.item_id
    JOIN frostbyte_tasty_bytes_v2.raw_supply_chain.menu_prices mp
        ON mp.menu_item_id = r.menu_item_id
        AND mp.start_date = ip.start_date
    JOIN frostbyte_tasty_bytes_v2.raw_pos.menu m
        ON m.menu_item_id = mp.menu_item_id
    WHERE m.item_category <> 'Extra'
),
_order_item_total AS
( 
    SELECT 
        oh.order_id,
        oh.order_ts,
        od.menu_item_id,
        od.quantity,
        m.base_price AS price,
        m.cost_of_goods_usd,
        m.base_price * od.quantity AS order_item_tot,
        oh.order_amount,
        m.cost_of_goods_usd * od.quantity AS order_item_cog,
        SUM(order_item_cog) OVER (PARTITION BY oh.order_id) AS order_cog
    FROM frostbyte_tasty_bytes_v2.raw_pos.order_header oh
    JOIN frostbyte_tasty_bytes_v2.raw_pos.order_detail od
        ON oh.order_id = od.order_id
    JOIN _menu_item_cogs_and_price m
        ON od.menu_item_id = m.menu_item_id 
        AND DATE(oh.order_ts) BETWEEN m.start_date AND m.end_date
)
SELECT 
        oi.order_id,
        DATE(oi.order_ts) AS date,
        oi.menu_item_id,
        oi.quantity, 
        oi.price,
        oi.cost_of_goods_usd,
        oi.order_item_tot,
        oi.order_item_cog,
        oi.order_amount,
        oi.order_cog,
        oi.order_amount - oi.order_item_tot AS order_amt_wo_item,
        oi.order_cog - oi.order_item_cog AS order_cog_wo_item
FROM _order_item_total oi;

/*-----------------------------------------------------------------------------
     ,.  .  .  ,.  ,    .   , ,---. ,  ,-.  ,-.  
    /  \ |\ | /  \ |     \ /    |   | /    (   ` 
    |--| | \| |--| |      Y     |   | |     `-.  
    |  | |  | |  | |      |     |   | \    .   ) 
    '  ' '  ' '  ' `--'   '     '   '  `-'  `-'  
-------------------------------------------------------------------------------
    analytics build
-----------------------------------------------------------------------------*/

-- snowpark stored procedure creation 
CREATE OR REPLACE PROCEDURE frostbyte_tasty_bytes_v2.analytics.build_ds_table()
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.8'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'create_table'
AS
$$
def create_table(session):
    import snowflake.snowpark.functions as F
    import snowflake.snowpark.types as T
    df = session.table("frostbyte_tasty_bytes_v2.analytics.orders_v") \
            .select("order_id",
                    "truck_id",
                    "location_id",
                    F.col("primary_city").alias("city"),
                    "latitude",
                    "longitude",
                    F.builtin("DATE")(F.col("order_ts")).alias("date"),
                    F.iff(F.builtin("DATE_PART")("HOUR",F.col("order_ts")) < '15',
                          'AM', 'PM').alias("shift"),
                    F.cast(F.col("order_total"), T.FloatType()).alias("order_total"),
                    ) \
            .distinct() \
            .group_by("truck_id", "location_id", "date", 
            "city", "latitude", "longitude", "shift") \
            .agg(F.sum("order_total").alias('shift_sales')) \
            .select("location_id",
                 "city",
                 "date",
                 "shift_sales",
                 "shift",
                 F.month(F.col("date")).alias("month"),
                 F.dayofweek(F.col("date")).alias("day_of_week"),
                 "latitude",
                 "longitude")
                 
    df_poi = session.table("frostbyte_tasty_bytes_v2.analytics.location_detail_v") \
                .select("location_id",
                        "count_locations_within_half_mile",
                        "city_population")
                        
    df = df.join(df_poi, df.location_id == df_poi.location_id, "LEFT") \
            .rename(df.location_id, "location_id") \
            .drop(df_poi.location_id)
    
    max_date = df.select(F.max(F.col("date"))).collect()[0][0]
    df_future = df.select("LOCATION_ID",
                          "CITY", 
                          F.lit(None).astype(T.DoubleType()).alias("SHIFT_SALES"),
                          "SHIFT",
                          "LATITUDE",
                          "LONGITUDE",
                          "COUNT_LOCATIONS_WITHIN_HALF_MILE",
                          "CITY_POPULATION").distinct()

    df_future_dates = df.select(F.dateadd("day", F.lit(7), F.col("DATE")).alias("NEW_DATE"),
                              F.month(F.dateadd("day", F.lit(7), F.col("DATE"))).alias("month"),
                              F.dayofweek(F.dateadd("day", F.lit(7), F.col("DATE"))).alias("day_of_week"),      
                              ) \
                             .rename(F.col("NEW_DATE"), "DATE") \
                             .where(F.col("DATE") > max_date).distinct()
    df_future = df_future.cross_join(df_future_dates)
    
    df = df_future.union_all_by_name(df).select('LOCATION_ID',
                                                         'CITY',
                                                         'DATE',
                                                         'SHIFT_SALES',
                                                         'SHIFT',
                                                         'MONTH',
                                                         'DAY_OF_WEEK',
                                                         'LATITUDE',
                                                         'LONGITUDE',
                                                         'COUNT_LOCATIONS_WITHIN_HALF_MILE',
                                                         'CITY_POPULATION')  
    df.write.mode("overwrite").save_as_table("frostbyte_tasty_bytes_v2.analytics.shift_sales")
                                                                  
    return "SUCCESS"
$$;

CREATE OR REPLACE VIEW frostbyte_tasty_bytes_v2.analytics.orders_v
COMMENT = 'Tasty Bytes Order Detail View'
    AS
SELECT DATE(o.order_ts) AS date, * FROM frostbyte_tasty_bytes_v2.harmonized.orders_v o;

CREATE OR REPLACE VIEW frostbyte_tasty_bytes_v2.analytics.customer_loyalty_metrics_v
COMMENT = 'Tasty Bytes Customer Loyalty Member Metrics View'
    AS
SELECT * FROM frostbyte_tasty_bytes_v2.harmonized.customer_loyalty_metrics_v;

CREATE OR REPLACE VIEW frostbyte_tasty_bytes_v2.analytics.location_detail_v
COMMENT = 'Tasty Bytes Truck Location Details View'
  AS
SELECT * FROM frostbyte_tasty_bytes_v2.harmonized.location_detail_v;

CREATE OR REPLACE VIEW frostbyte_tasty_bytes_v2.analytics.menu_item_cogs_and_price_v
COMMENT = 'Base Menu Item Id - COGS and Sale Price Point-In-Time'
    AS
SELECT * FROM frostbyte_tasty_bytes_v2.harmonized.menu_item_cogs_and_price_v;


CREATE OR REPLACE VIEW frostbyte_tasty_bytes_v2.analytics.order_item_cost_agg_v 
COMMENT = 'Order Item Cost Aggregate View used in Price Elasticity'
    AS
SELECT 
    year, 
    month,
    menu_item_id,
	avg_revenue_wo_item,
    avg_cost_wo_item,
    avg_profit_wo_item,
	LAG(avg_revenue_wo_item,1) OVER (PARTITION BY menu_item_id ORDER BY year,month) AS prev_avg_revenue_wo_item,
    LAG(avg_cost_wo_item,1) OVER (PARTITION BY menu_item_id ORDER BY year,month) AS prev_avg_cost_wo_item,
    LAG(avg_profit_wo_item,1) OVER (PARTITION BY menu_item_id ORDER BY year,month) AS prev_avg_profit_wo_item
FROM 
(SELECT * FROM (
    (
        SELECT   
            oic1.menu_item_id,
            YEAR(oic1.date) AS year,
            MONTH(oic1.date) AS month,
            SUM(oic1.order_amt_wo_item) / SUM(oic1.quantity) AS avg_revenue_wo_item,
            SUM(oic1.order_cog_wo_item) / SUM(oic1.quantity) AS avg_cost_wo_item,
            (SUM(oic1.order_amt_wo_item) - SUM(oic1.order_cog_wo_item)) /SUM(oic1.quantity) AS avg_profit_wo_item
        FROM frostbyte_tasty_bytes_v2.harmonized.order_item_cost_v oic1
        GROUP BY oic1.menu_item_id, YEAR(oic1.date), MONTH(oic1.date)
    )
UNION
    (
    SELECT
            oic2.menu_item_id,
            CASE 
                WHEN max_date.max_month = 12 THEN max_date.max_year + 1
            ELSE max_date.max_year
            END AS year,
            CASE 
                WHEN max_date.max_month = 12 THEN 1
            ELSE max_date.max_month + 1
            END AS month,
            0 AS avg_revenue_wo_item, 
            0 AS avg_cost_wo_item, 
            0 AS avg_profit_wo_item
    FROM (
            SELECT DISTINCT 
                oh.menu_item_id,
                DATE(oh.order_ts) AS date 
            FROM frostbyte_tasty_bytes_v2.harmonized.orders_v oh
        ) oic2
    JOIN
        (
        SELECT 
            MONTH(MAX(DATE(oh.order_ts))) AS max_month,
            YEAR(MAX(DATE(oh.order_ts))) AS max_year
        FROM frostbyte_tasty_bytes_v2.harmonized.orders_v oh
        ) max_date
ON YEAR(oic2.date) = max_date.max_year AND MONTH(oic2.date) = max_date.max_month
    )
) oic
ORDER BY oic.menu_item_id, oic.year, oic.month)avg_r_c_wo_item;


-- create demand_forecast_training_base table
CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.analytics.demand_forecast_training_base
COMMENT = 'Training Staging Table for Demo Purposes'
    AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.analytics_v2.demand_forecast_training_base;

-- create menu_item_forecast table
CREATE OR REPLACE TABLE frostbyte_tasty_bytes_v2.analytics.menu_item_forecast
COMMENT = 'Output of Data Science Vignette - Available immediately in Exchange for Demo Purposes'
    AS
SELECT * FROM frostbyte_tasty_bytes_setup_s.analytics_v2.menu_item_forecast;

-- create menu_item_aggregate_v view
CREATE OR REPLACE VIEW frostbyte_tasty_bytes_v2.analytics.menu_item_aggregate_v
COMMENT = 'Corporate Grain View for Price Elasticity'
    AS
SELECT * RENAME sale_price AS price FROM frostbyte_tasty_bytes_v2.harmonized.menu_item_aggregate_dt;

-- call snowpark stored procedure to generate shift_sales table
CALL frostbyte_tasty_bytes_v2.analytics.build_ds_table();

ALTER DYNAMIC TABLE frostbyte_tasty_bytes_v2.harmonized.menu_item_aggregate_dt SET LAG = '1 days';

-- scale warehouse for table creation
ALTER WAREHOUSE demo_build_wh SET warehouse_size = 'XSmall';


/*-----------------------------------------------------------------------------
    .  . ;-.  ,-.   ,.  ,---. ,--.  ,-.  
    |  | |  ) |  \ /  \   |   |    (   ` 
    |  | |-'  |  | |--|   |   |-    `-.  
    |  | |    |  / |  |   |   |    .   ) 
    `--` '    `-'  '  '   '   `--'  `-'                              
-------------------------------------------------------------------------------
    procedures and tasks for automated transactional data updates
-----------------------------------------------------------------------------*/

-- create raw_pos table update procedure
CREATE OR REPLACE PROCEDURE frostbyte_tasty_bytes_v2.raw_pos.update_raw_pos_tables()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

INSERT INTO frostbyte_tasty_bytes_v2.raw_pos.order_header
(
    SELECT * 
    FROM frostbyte_tasty_bytes_setup_s.raw_pos.order_header
    WHERE DATE(order_ts) >
        (SELECT MAX(order_ts) FROM frostbyte_tasty_bytes_v2.raw_pos.order_header)
);

INSERT INTO frostbyte_tasty_bytes_v2.raw_pos.order_detail
(
    SELECT * 
    FROM frostbyte_tasty_bytes_setup_s.raw_pos.order_detail
    WHERE order_id NOT IN 
        (SELECT DISTINCT order_id FROM frostbyte_tasty_bytes_v2.raw_pos.order_detail)
);
RETURN 'raw_pos tables have been refreshed successfully';
END;
$$;

-- create raw_supply_chain table update procedure
CREATE OR REPLACE PROCEDURE frostbyte_tasty_bytes_v2.raw_supply_chain.update_raw_supply_chain_tables()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

INSERT INTO frostbyte_tasty_bytes_v2.raw_supply_chain.purchase_order_header
(
    SELECT * 
    FROM frostbyte_tasty_bytes_setup_s.raw_supply_chain_v2.purchase_order_header
    WHERE po_date > 
        (SELECT MAX(po_date) FROM frostbyte_tasty_bytes_v2.raw_supply_chain.purchase_order_header)
);

INSERT INTO frostbyte_tasty_bytes_v2.raw_supply_chain.purchase_order_detail
(
    SELECT * 
    FROM frostbyte_tasty_bytes_setup_s.raw_supply_chain_v2.purchase_order_detail
    WHERE po_id NOT IN 
        (SELECT DISTINCT po_id FROM frostbyte_tasty_bytes_v2.raw_supply_chain.purchase_order_detail)
);

INSERT INTO frostbyte_tasty_bytes_v2.raw_supply_chain.distribution_header
(
    SELECT * 
    FROM frostbyte_tasty_bytes_setup_s.raw_supply_chain_v2.distribution_header
    WHERE distribution_date > 
        (SELECT MAX(distribution_date) FROM frostbyte_tasty_bytes_v2.raw_supply_chain.distribution_header)
);

INSERT INTO frostbyte_tasty_bytes_v2.raw_supply_chain.distribution_detail
(
    SELECT * 
    FROM frostbyte_tasty_bytes_setup_s.raw_supply_chain_v2.distribution_detail
    WHERE dh_id NOT IN 
        (SELECT DISTINCT dh_id FROM frostbyte_tasty_bytes_v2.raw_supply_chain.distribution_detail)
);

INSERT INTO frostbyte_tasty_bytes_v2.raw_supply_chain.eod_stock_assignment
(
    SELECT * 
    FROM frostbyte_tasty_bytes_setup_s.raw_supply_chain_v2.eod_stock_assignment
    WHERE assignment_id NOT IN 
        (SELECT DISTINCT assignment_id FROM frostbyte_tasty_bytes_v2.raw_supply_chain.eod_stock_assignment)
);
RETURN 'raw_supply_chain tables have been refreshed successfully';
END;
$$;

-- create raw_truck table update procedure
CREATE OR REPLACE PROCEDURE frostbyte_tasty_bytes_v2.raw_truck.update_raw_truck_tables()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

INSERT INTO frostbyte_tasty_bytes_v2.raw_truck.inventory_queue
(
    SELECT * 
    FROM frostbyte_tasty_bytes_setup_s.raw_truck_v2.inventory_queue 
    WHERE schedule_week NOT IN
        (SELECT DISTINCT schedule_week FROM frostbyte_tasty_bytes_v2.raw_truck.inventory_queue)
);

INSERT INTO frostbyte_tasty_bytes_v2.raw_truck.truck_shift
(
    SELECT * 
    FROM frostbyte_tasty_bytes_setup_s.raw_truck_v2.truck_shift
    WHERE forecast_date > 
        (SELECT MAX(forecast_date) FROM frostbyte_tasty_bytes_v2.raw_truck.truck_shift)
);
RETURN 'raw_truck tables have been refreshed successfully';
END;
$$;


-- task setup
CREATE OR REPLACE TASK frostbyte_tasty_bytes_v2.raw_pos.update_raw_pos_tables_task
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'small'
SCHEDULE = 'USING CRON 0 0 1 * * America/Los_Angeles'
COMMENT = 'Weekly Serverless Task to update the frostbyte_tasty_bytes_v2.raw_pos transaction tables'
    AS
CALL frostbyte_tasty_bytes_v2.raw_pos.update_raw_pos_tables();

CREATE OR REPLACE TASK frostbyte_tasty_bytes_v2.raw_supply_chain.update_raw_supply_chain_tables_task
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'small'
SCHEDULE = 'USING CRON 0 0 1 * * America/Los_Angeles'
COMMENT = 'Weekly Serverless Task to update the frostbyte_tasty_bytes_v2.raw_supply_chain transaction tables'
    AS
CALL frostbyte_tasty_bytes_v2.raw_truck.update_raw_truck_tables();

CREATE OR REPLACE TASK frostbyte_tasty_bytes_v2.raw_truck.update_raw_truck_tables_task
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'small'
SCHEDULE = 'USING CRON 0 0 1 * * America/Los_Angeles'
COMMENT = 'Weekly Serverless Task to update the frostbyte_tasty_bytes_v2.raw_truck transaction tables'
    AS
CALL frostbyte_tasty_bytes_v2.raw_truck.update_raw_truck_tables();

-- resume tasks
ALTER TASK frostbyte_tasty_bytes_v2.raw_pos.update_raw_pos_tables_task RESUME;
ALTER TASK frostbyte_tasty_bytes_v2.raw_supply_chain.update_raw_supply_chain_tables_task RESUME;
ALTER TASK frostbyte_tasty_bytes_v2.raw_truck.update_raw_truck_tables_task RESUME;

use role accountadmin;
drop stream if exists frostbyte_tasty_bytes_v2.raw_pos.order_header_stream;
drop stream if exists frostbyte_tasty_bytes_v2.raw_pos.order_detail_stream;
alter task if exists FROSTBYTE_TASTY_BYTES_V2.ANALYTICS.TB_DEV_CLONE_TASK suspend;
drop task if exists frostbyte_tasty_bytes_v2.analytics.tb_dev_clone_task;
drop task if exists frostbyte_tasty_bytes_v2.analytics.order_header_load_task;
drop task if exists frostbyte_tasty_bytes_v2.analytics.order_detail_load_task;
drop task if exists frostbyte_tasty_bytes_v2.analytics.aggregate_sales_daily_task;


-- setup completion note
SELECT 'frostbyte_tasty_bytes_v2 setup database is now complete' AS note;
