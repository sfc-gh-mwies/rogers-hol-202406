/***************************************************************************************************
  _______           _            ____          _             
 |__   __|         | |          |  _ \        | |            
    | |  __ _  ___ | |_  _   _  | |_) | _   _ | |_  ___  ___ 
    | | / _` |/ __|| __|| | | | |  _ < | | | || __|/ _ \/ __|
    | || (_| |\__ \| |_ | |_| | | |_) || |_| || |_|  __/\__ \
    |_| \__,_||___/ \__| \__, | |____/  \__, | \__|\___||___/
                          __/ |          __/ |               
                         |___/          |___/            
Quickstart:   Tasty Bytes - Zero to Snowflake - Introduction
Version:      v1
Script:       tb_fy25_introduction.sql         
Author:       Jacob Kranzler
Copyright(c): 2024 Snowflake Inc. All rights reserved.
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2024-05-23          Jacob Kranzler      Initial Release
***************************************************************************************************/

USE ROLE sysadmin;

/*--
 • database, schema and warehouse creation
--*/

-- CREATE A DATABASE NAMED "tb_101_share" FROM THE SHARE TITLED "tb_101";

-- build out the tb_101 database and schemas
CREATE OR REPLACE DATABASE TB_101;
CREATE OR REPLACE SCHEMA tb_101.raw_pos;
CREATE OR REPLACE SCHEMA tb_101.raw_customer;
CREATE OR REPLACE SCHEMA tb_101.harmonized;
CREATE OR REPLACE SCHEMA tb_101.analytics;

-- create warehouses
CREATE OR REPLACE WAREHOUSE tb_de_wh
    WAREHOUSE_SIZE = 'large' -- Large for initial data load - scaled down to XSmall at end of this scripts
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    MAX_CLUSTER_COUNT = 5
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'data engineering warehouse for tasty bytes';

CREATE OR REPLACE WAREHOUSE tb_dev_wh
    WAREHOUSE_SIZE = 'medium'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    MAX_CLUSTER_COUNT = 5
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'developer warehouse for tasty bytes';

-- create roles
USE ROLE securityadmin;

-- functional roles
CREATE ROLE IF NOT EXISTS tb_admin
    COMMENT = 'admin for tasty bytes';
    
CREATE ROLE IF NOT EXISTS tb_data_engineer
    COMMENT = 'data engineer for tasty bytes';
    
CREATE ROLE IF NOT EXISTS tb_dev
    COMMENT = 'developer for tasty bytes';
    
-- role hierarchy
GRANT ROLE tb_admin TO ROLE sysadmin;
GRANT ROLE tb_data_engineer TO ROLE tb_admin;
GRANT ROLE tb_dev TO ROLE tb_data_engineer;

-- privilege grants
USE ROLE accountadmin;

GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE tb_data_engineer;

GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE tb_admin;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE tb_admin;

USE ROLE securityadmin;

GRANT USAGE ON DATABASE tb_101 TO ROLE tb_admin;
GRANT USAGE ON DATABASE tb_101 TO ROLE tb_data_engineer;
GRANT USAGE ON DATABASE tb_101 TO ROLE tb_dev;

GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE tb_admin;
GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE tb_data_engineer;
GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE tb_dev;

GRANT ALL ON SCHEMA tb_101.raw_pos TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.raw_pos TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.raw_pos TO ROLE tb_dev;

GRANT ALL ON SCHEMA tb_101.harmonized TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.harmonized TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.harmonized TO ROLE tb_dev;

GRANT ALL ON SCHEMA tb_101.analytics TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.analytics TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.analytics TO ROLE tb_dev;

-- warehouse grants
GRANT OWNERSHIP ON WAREHOUSE tb_de_wh TO ROLE tb_admin COPY CURRENT GRANTS;
GRANT ALL ON WAREHOUSE tb_de_wh TO ROLE tb_admin;
GRANT ALL ON WAREHOUSE tb_de_wh TO ROLE tb_data_engineer;

GRANT ALL ON WAREHOUSE tb_dev_wh TO ROLE tb_admin;
GRANT ALL ON WAREHOUSE tb_dev_wh TO ROLE tb_data_engineer;
GRANT ALL ON WAREHOUSE tb_dev_wh TO ROLE tb_dev;

-- future grants
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_pos TO ROLE tb_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_pos TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_pos TO ROLE tb_dev;

GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_customer TO ROLE tb_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_customer TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_customer TO ROLE tb_dev;

GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.harmonized TO ROLE tb_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.harmonized TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.harmonized TO ROLE tb_dev;

GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.analytics TO ROLE tb_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.analytics TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.analytics TO ROLE tb_dev;

-- Apply Masking Policy Grants
USE ROLE accountadmin;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE tb_admin;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE tb_data_engineer;
  
-- raw_pos table build
USE ROLE sysadmin;
USE WAREHOUSE tb_de_wh;

/*--
 • file format and stage creation
--*/

CREATE OR REPLACE FILE FORMAT tb_101.public.csv_ff 
type = 'csv';

/*--
 raw zone table build 
--*/

-- country table build
CREATE OR REPLACE TABLE tb_101.raw_pos.country
AS SELECT * FROM tb_101_share.raw_pos.country;

-- franchise table build
CREATE OR REPLACE TABLE tb_101.raw_pos.franchise 
AS SELECT * FROM tb_101_share.raw_pos.franchise;

-- location table build
CREATE OR REPLACE TABLE tb_101.raw_pos.location
AS SELECT * FROM tb_101_share.raw_pos.location;

-- menu table build
CREATE OR REPLACE TABLE tb_101.raw_pos.menu
AS SELECT * FROM tb_101_share.raw_pos.menu;

-- truck table build 
CREATE OR REPLACE TABLE tb_101.raw_pos.truck
AS SELECT * FROM tb_101_share.raw_pos.truck;

-- order_header table build
CREATE OR REPLACE TABLE tb_101.raw_pos.order_header
AS SELECT * FROM tb_101_share.raw_pos.order_header;

-- order_detail table build
CREATE OR REPLACE TABLE tb_101.raw_pos.order_detail 
AS SELECT * FROM tb_101_share.raw_pos.order_detail;

-- customer loyalty table build
CREATE OR REPLACE TABLE tb_101.raw_customer.customer_loyalty
AS SELECT * FROM tb_101_share.raw_customer.customer_loyalty;

/*--
 • harmonized view creation
--*/

-- orders_v view
CREATE OR REPLACE VIEW tb_101.harmonized.orders_v
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
FROM tb_101.raw_pos.order_detail od
JOIN tb_101.raw_pos.order_header oh
    ON od.order_id = oh.order_id
JOIN tb_101.raw_pos.truck t
    ON oh.truck_id = t.truck_id
JOIN tb_101.raw_pos.menu m
    ON od.menu_item_id = m.menu_item_id
JOIN tb_101.raw_pos.franchise f
    ON t.franchise_id = f.franchise_id
JOIN tb_101.raw_pos.location l
    ON oh.location_id = l.location_id
LEFT JOIN tb_101.raw_customer.customer_loyalty cl
    ON oh.customer_id = cl.customer_id;

-- loyalty_metrics_v view
CREATE OR REPLACE VIEW tb_101.harmonized.customer_loyalty_metrics_v
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
FROM tb_101.raw_customer.customer_loyalty cl
JOIN tb_101.raw_pos.order_header oh
ON cl.customer_id = oh.customer_id
GROUP BY cl.customer_id, cl.city, cl.country, cl.first_name,
cl.last_name, cl.phone_number, cl.e_mail;

/*--
 • analytics view creation
--*/

-- orders_v view
CREATE OR REPLACE VIEW tb_101.analytics.orders_v
COMMENT = 'Tasty Bytes Order Detail View'
    AS
SELECT DATE(o.order_ts) AS date, * FROM tb_101.harmonized.orders_v o;

-- customer_loyalty_metrics_v view
CREATE OR REPLACE VIEW tb_101.analytics.customer_loyalty_metrics_v
COMMENT = 'Tasty Bytes Customer Loyalty Member Metrics View'
    AS
SELECT * FROM tb_101.harmonized.customer_loyalty_metrics_v;


/* ================ */
/* GOVERNANCE SETUP */
use role accountadmin;
CREATE OR REPLACE ROLE tasty_test_role
COMMENT = 'test role for tasty bytes';
CREATE OR REPLACE ROLE tastybytes_admin;
GRANT ROLE tastybytes_admin TO ROLE SYSADMIN;
GRANT ROLE tasty_test_role TO ROLE tastybytes_admin;

-- first we will grant ALL privileges on the TASTY_DEV_WH to our SYSADMIN
CREATE WAREHOUSE IF NOT EXISTS tasty_dev_wh;
GRANT ALL ON WAREHOUSE tasty_dev_wh TO ROLE sysadmin;

-- next we will grant only OPERATE and USAGE privileges to our TASTY_TEST_ROLE
GRANT OPERATE, USAGE ON WAREHOUSE tasty_dev_wh TO ROLE tasty_test_role;
GRANT ALL ON WAREHOUSE tasty_dev_wh TO ROLE tastybytes_admin;

-- Users will need to create their own DB
GRANT CREATE DATABASE ON ACCOUNT TO ROLE tastybytes_admin;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE tasty_test_role;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE tasty_data_engineer;


-- now we will grant USAGE on our tb_101 database and ALL schemas within it.
GRANT USAGE ON DATABASE tb_101 TO ROLE tasty_test_role;
GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE tasty_test_role;

-- we are going to test Data Governance features as our role, so let's ensure our role can run SELECT statements against our Data Model
GRANT SELECT ON ALL TABLES IN SCHEMA tb_101.raw_customer TO ROLE tasty_test_role;
GRANT SELECT ON ALL TABLES IN SCHEMA tb_101.raw_pos TO ROLE tasty_test_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA tb_101.analytics TO ROLE tasty_test_role;

-- before we proceed, let's SET a SQL VARIABLE to equal our CURRENT_USER()
SET my_user_var  = CURRENT_USER();

-- now we can GRANT our ROLE to the USER we are currently logged in as.
GRANT ROLE tasty_test_role TO USER identifier($my_user_var);
GRANT ROLE tastybytes_admin TO USER identifier($my_user_var);
GRANT CREATE SCHEMA ON DATABASE tb_101 TO ROLE tasty_test_role;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE tasty_test_role;
GRANT APPLY TAG ON ACCOUNT TO ROLE tasty_test_role;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE tastybytes_admin;
GRANT APPLY TAG ON ACCOUNT TO ROLE tastybytes_admin;

/* MUST GRANT tastybytes_admin, tb_data_engineer, tb_admin, tb_dev ROLES TO ALL LAB PARTICIPANTS */
