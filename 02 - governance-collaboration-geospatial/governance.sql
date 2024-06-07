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
Vignette:     Applying Data Governance
Script:       data_governance_step_1_tb.sql
Create Date:  2023-01-13
Author:       Jacob Kranzler
Copyright(c): 2023 Snowflake Inc. All rights reserved.
****************************************************************************************************
Description:
    Data Governance
        3) Column-Level Security and Tagging = Tag-Based Masking
        4) Row-Access Policies + CREATE TABLE & INSERT VALUES
        5) (Optional) Data Classification
        6) Test Objects Cleanup

****************************************************************************************************

/*----------------------------------------------------------------------------------
The Snowflake Access Control Framework is based on:

  • Role-based Access Control (RBAC): Access privileges are assigned to roles, which 
    are in turn assigned to users.
  • Discretionary Access Control (DAC): Each object has an owner, who can in turn 
    grant access to that object.
      • Tip: DAC can be disabled with Managed Access Schemas.

The key concepts to understanding access control in Snowflake are:

  • Securable Object: An entity to which access can be granted. Unless allowed by a 
    grant, access is denied.
      • Securable Objects are owned by a Role (as opposed to a User)
      • Examples: database, schema, table, view, warehouse, function, etc
  • Role: An entity to which privileges can be granted. Roles are in turn assigned 
    to users. Note that roles can also be assigned to other roles, creating a role 
    hierarchy.
  • Privilege: A defined level of access to an object. Multiple distinct privileges 
    may be used to control the granularity of access granted.
  • User: A user identity recognized by Snowflake, whether associated with a person 
    or program.

In Summary
  • In Snowflake, a Role is a container for Privileges to a Securable Object.
  • Privileges can be granted Roles
  • Roles can be granted to Users
  • Roles can be granted to other Roles (which inherit that Roles Privileges)
  • When Users choose a Role, they inherit all the Privileges of the Roles in the 
    hierarchy.
  
----------------------------------------------------------------------------------*/


    /**
      Snowflake System Defined Role Definitions:
       1) ORGADMIN (aka Organization Administrator): Role that manages operations at the organization level.
          More specifically, this role:
          • Can create accounts in the organization.
          • Can view all accounts in the organization using SHOW ORGANIZATION ACCOUNTS as well as all regions
            enabled for the organization using SHOW REGIONS.
          • Can view usage information across the organization.
          • Can be granted to a user or a custom role
          • Can NOT be granted to a system role
       2) ACCOUNTADMIN (aka Account Administrator): Role that encapsulates the SYSADMIN and SECURITYADMIN system-defined roles.
            It is the top-level role in the system and should be granted only to a limited/controlled number of users
            in your account.
       3) SECURITYADMIN (aka Security Administrator): Role that can manage any object grant globally, as well as create, monitor,
          and manage users and roles.
          More specifically, this role:
           • Is granted the MANAGE GRANTS security privilege to be able to modify any grant, including revoking it.
           • Inherits the privileges of the USERADMIN role via the system role hierarchy (i.e. USERADMIN role is granted to SECURITYADMIN).
       4) USERADMIN (aka User and Role Administrator): Role that is dedicated to user and role management only.
          More specifically, this role:
          • Is granted the CREATE USER and CREATE ROLE security privileges.
          • Can create users and roles in the account.
       5) SYSADMIN (aka System Administrator): Role that has privileges to create warehouses and databases in an account.
          If, as recommended, you create a role hierarchy that ultimately assigns all custom roles to the SYSADMIN role, this role also has
          the ability to grant privileges on warehouses, databases, and other objects to other roles.
       6) PUBLIC: Pseudo-role that is automatically granted to every user and every role in your account. The PUBLIC role can own securable
          objects, just like any other role; however, the objects owned by the role are, by definition, available to every other
          user and role in your account.
    **/

/*
            +---------------+
            | ACCOUNTADMIN  |
            +---------------+
              ^    ^     ^
              |    |     |
+-------------+-+  |    ++-------------+
| SECURITYADMIN |  |    |   SYSADMIN   |<------------+
+---------------+  |    +--------------+             |
        ^          |     ^        ^                  |
        |          |     |        |                  |
+-------+-------+  |     |  +-----+-------+  +-------+-----+
|   USERADMIN   |  |     |  | CUSTOM ROLE |  | CUSTOM ROLE |
+---------------+  |     |  +-------------+  +-------------+
        ^          |     |      ^              ^      ^
        |          |     |      |              |      |
        |          |     |      |              |    +-+-----------+
        |          |     |      |              |    | CUSTOM ROLE |
        |          |     |      |              |    +-------------+
        |          |     |      |              |           ^
        |          |     |      |              |           |
        +----------+-----+---+--+--------------+-----------+
                             |
                        +----+-----+
                        |  PUBLIC  |
                        +----------+
 */




/*----------------------------------------------------------------------------------
Step 3 - The first Data Governance feature set we want to deploy and test will be
  Snowflake Tag Based Dynamic Data Masking. This will allow us to mask PII data
  in columns from our test role but not from more privileged roles.
----------------------------------------------------------------------------------*/

-- we can now USE the privileged role and warehouse
USE ROLE tasty_test_role;
USE WAREHOUSE tasty_dev_wh;


-- to begin we will look at our CUSTOMER_LOYALTY table in the raw layer
-- which contains raw data from our Customer Loyalty program
SELECT
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.city,
    cl.country,
    cl.sign_up_date,
    cl.birthday_date
FROM tb_101.raw_customer.customer_loyalty cl 
SAMPLE (10000 ROWS);

-- woah! there is a lot of PII we need to take care of before our users can touch this data.
-- luckily we can use Snowflakes native Tag-Based Masking to do just this

        /*---
         feature note: A tag-based masking policy combines the object tagging and masking policy features
          to allow a masking policy to be set on a tag using an ALTER TAG command. When the data type in
          the masking policy signature and the data type of the column match, the tagged column is
          automatically protected by the conditions in the masking policy.
        ---*/

-- First let's create a database to keep your work separate from the other participants
CREATE DATABASE <firstname>_<lastname>;

-- Next let's create a tags and governance schema to keep ourselves organized and follow best practices

-- create a tags schema to contain our object tags
CREATE OR REPLACE SCHEMA <firstname>_<lastname>.tags
    COMMENT = 'Schema containing object tags';
    
-- we want everyone with access to this table to be able to view the tags 
GRANT USAGE ON SCHEMA <firstname>_<lastname>.tags TO ROLE public;

-- now we will create a governance schema to contain our security policies
CREATE OR REPLACE SCHEMA <firstname>_<lastname>.governance
    COMMENT = 'Schema containing security policies';

GRANT ALL ON SCHEMA <firstname>_<lastname>.governance TO ROLE sysadmin;

-- now we will create one TAG for PII that allows these values: NAME, PHONE_NUMBER, EMAIL, BIRTHDAY
-- not only will this prevent free text values, but will also add selection menu to the GUI
CREATE OR REPLACE TAG <firstname>_<lastname>.tags.tasty_pii
    ALLOWED_VALUES 'NAME', 'PHONE_NUMBER', 'EMAIL', 'BIRTHDAY'
    COMMENT = 'Tag for PII, allowed values are: NAME, PHONE_NUMBER, EMAIL, BIRTHDAY.';


-- with the TAGS created, let's assign them to the relevant columns in our customer loyalty table
-- *Tip: We are doing this programmatically, however you can also do this via GUI

/* FRIST CLONE YOUR OWN VERSION OF THE CUSTOMER_LOYALTY TABLE SO THAT WE'RE NOT STEPPING ON EACH OTHER'S DATA */
CREATE TABLE <firstname>_<lastname>.public.customer_loyalty CLONE tb_101.raw_customer.customer_loyalty;

ALTER TABLE <firstname>_<lastname>.public.customer_loyalty
    MODIFY COLUMN 
    first_name SET TAG <firstname>_<lastname>.tags.tasty_pii = 'NAME',
    last_name  SET TAG <firstname>_<lastname>.tags.tasty_pii = 'NAME',
    phone_number SET TAG <firstname>_<lastname>.tags.tasty_pii = 'PHONE_NUMBER',
    e_mail SET TAG <firstname>_<lastname>.tags.tasty_pii = 'EMAIL',
    birthday_date SET TAG <firstname>_<lastname>.tags.tasty_pii = 'BIRTHDAY';
    
-- now we can use the TAG_REFERENCES_ALL_COLUMNS function to return the tags associated with our customer loyalty table

SELECT
    tag_database,
    tag_schema,
    tag_name,
    column_name,
    tag_value
FROM TABLE(<firstname>_<lastname>.information_schema.tag_references_all_columns
    ('<firstname>_<lastname>.public.customer_loyalty','table'));


-- with our tags in place we can now create our masking policies that will mask data for all but privileged roles.
-- we need to create 1 policy for every datatype where the return datatype can be implicitly cast into the column datatype.
-- we can only assign 1 policy per datatype to an individual tag

--> create our STRING datatype mask
  --> a masking policy is made of standard conditional logic, such a CASE statement
  --> it is validating whether a condition is true, and returning the appropriate expression
CREATE OR REPLACE MASKING POLICY <firstname>_<lastname>.governance.tasty_pii_string_mask AS (val STRING) RETURNS STRING ->
    CASE
        -- these active roles have access to values 
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN','tastybytes_admin')
            THEN val 
        -- if a column is tagged with TASTY_PHI : PHONE_NUMBER    
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('TAGS.TASTY_PII') = 'PHONE_NUMBER'
            THEN CONCAT(LEFT(val,3), '-***-****')
        -- if a column is tagged with TASTY_PHI : EMAIL    
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('TAGS.TASTY_PII') = 'EMAIL'
            THEN CONCAT('**~MASKED~**','@', SPLIT_PART(val, '@', -1))
        -- all other conditions    
    ELSE '**~MASKED~**' 
END;

-- The combination of the city, first 3 digits of a phone number, and birthday might be enough to re-identify 
-- let's play it safe an truncate birthdays into 5 year buckets which will fit the use case of our analyst
--> create our DATE to return the modified date of birth
  --> if a DATE column is not tagged with BIRTHDAY, return null.
CREATE OR REPLACE MASKING POLICY <firstname>_<lastname>.governance.tasty_pii_date_mask AS (val DATE) RETURNS DATE ->
    CASE
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') 
            THEN val
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('TAGS.TASTY_PII') = 'BIRTHDAY'
            THEN DATE_FROM_PARTS(YEAR(val) - (YEAR(val) % 5),1,1)
    ELSE null 
END;


-- now we are able to use an ALTER TAG statement to set the masking policies on the PII tagged columns
ALTER TAG <firstname>_<lastname>.tags.tasty_pii SET
    MASKING POLICY <firstname>_<lastname>.governance.tasty_pii_string_mask,
    MASKING POLICY <firstname>_<lastname>.governance.tasty_pii_date_mask;

    
-- with Tag Based Masking in-place, let's give our work a test
USE ROLE tasty_test_role;
USE WAREHOUSE tasty_dev_wh;

SELECT
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    cl.birthday_date,
    cl.city,
    cl.country
FROM <firstname>_<lastname>.public.customer_loyalty cl
WHERE 1=1
    AND cl.country IN ('United States','Canada','Brazil');


-- the masking is working! But what if we create a view that uses this table?

create or replace view <firstname>_<lastname>.public.CUSTOMER_LOYALTY_METRICS_V(
	CUSTOMER_ID,
	CITY,
	COUNTRY,
	FIRST_NAME,
	LAST_NAME,
	PHONE_NUMBER,
	E_MAIL,
	TOTAL_SALES,
	VISITED_LOCATION_IDS_ARRAY
) as
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
FROM <firstname>_<lastname>.public.customer_loyalty cl
JOIN tb_101.raw_pos.order_header oh
ON cl.customer_id = oh.customer_id
GROUP BY cl.customer_id, cl.city, cl.country, cl.first_name,
cl.last_name, cl.phone_number, cl.e_mail;

SELECT TOP 10
    clm.customer_id,
    clm.first_name,
    clm.last_name,
    clm.phone_number,
    clm.e_mail,
    SUM(clm.total_sales) AS lifetime_sales_usd
FROM <firstname>_<lastname>.public.CUSTOMER_LOYALTY_METRICS_V clm
WHERE 1=1
    AND clm.city = 'San Mateo'
GROUP BY clm.customer_id, clm.first_name, clm.last_name, clm.phone_number, clm.e_mail
ORDER BY lifetime_sales_usd;


-- before moving on, let's quickly check our privileged users are able to see the data unmasked
USE ROLE tastybytes_admin;

SELECT TOP 10
    clm.customer_id,
    clm.first_name,
    clm.last_name,
    clm.phone_number,
    clm.e_mail,
    SUM(clm.total_sales) AS lifetime_sales_usd
FROM <firstname>_<lastname>.PUBLIC.CUSTOMER_LOYALTY_METRICS_V clm
WHERE 1=1
    AND clm.city = 'San Mateo'
GROUP BY clm.customer_id, clm.first_name, clm.last_name, clm.phone_number, clm.e_mail
ORDER BY lifetime_sales_usd;


/*----------------------------------------------------------------------------------
Step 4 -
    Happy with our Tag Based Dynamic Masking controlling masking at the column level,
    we will now look to restrict access at the row level for our test role.

    Within our Customer Loyalty table, our role should only see Customers who are
    based in Tokyo.

    Thankfully, Snowflake has another powerful native Data Governance feature that can
    handle this at scale called Row Access Policies. For our use case, we will leverage
    the mapping table approach.
----------------------------------------------------------------------------------*/

 -- to start, our SYSADMIN will create our mapping table including ROLE and CITY PERMISSIONS columns
 -- we will create this in the governance schema, as we don't want this table to be visible to others.
USE ROLE tasty_test_role;

CREATE OR REPLACE TABLE <firstname>_<lastname>.governance.row_policy_map
    (role STRING, city_permissions STRING);

-- with the table in place, we will now insert the relevant ROLE to CITY_PERMISSIONS mapping
INSERT INTO <firstname>_<lastname>.governance.row_policy_map
    VALUES
        ('TASTY_TEST_ROLE','Tokyo'); -- TASTY_TEST_ROLE should only see Tokyo Customers


-- now that we have our mapping table in place, let's create our Row Access Policy

        /*---
         feature note: Snowflake supports row-level security through the use of Row Access Policies to
          determine which rows to return in the query result. The row access policy can be relatively
          simple to allow one particular role to view rows, or be more complex to include a mapping
          table in the policy definition to determine access to rows in the query result.
        ---*/

CREATE OR REPLACE ROW ACCESS POLICY <firstname>_<lastname>.governance.customer_city_row_policy
    AS (city STRING) RETURNS BOOLEAN ->
       CURRENT_ROLE() IN -- list of roles that will not be subject to the policy
           (
            'ACCOUNTADMIN','SYSADMIN'
           )
        OR EXISTS -- this clause references our mapping table from above to handle the row level filtering
            (
            SELECT rp.role
                FROM <firstname>_<lastname>.governance.row_policy_map rp
            WHERE 1=1
                AND rp.role = CURRENT_ROLE()
                AND rp.city_permissions = city
            )
COMMENT = 'Policy to limit rows returned based on mapping table of ROLE and CITY: <firstname>_<lastname>_governance.row_policy_map';

 -- let's now apply the franchise row policy to our CITY column in the Customer Loyalty dimension table
ALTER TABLE <firstname>_<lastname>.public.customer_loyalty
    ADD ROW ACCESS POLICY <firstname>_<lastname>.governance.customer_city_row_policy ON (city);

-- with the policy successfully applied, let's test it as TASTY_TEST_ROLE
USE ROLE tasty_test_role;

SELECT
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.city,
    cl.marital_status,
    DATEDIFF(year, cl.birthday_date, CURRENT_DATE()) AS age
FROM <firstname>_<lastname>.public.customer_loyalty cl SAMPLE (10000 ROWS)
GROUP BY cl.customer_id, cl.first_name, cl.last_name, cl.city, cl.marital_status, age;

-- wow! we were able to see both Row and Column level security in that result set.
-- let's now check that a privileged user is not impacted

USE ROLE sysadmin;

SELECT
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.city,
    cl.marital_status,
    DATEDIFF(year, cl.birthday_date, CURRENT_DATE()) AS age
FROM <firstname>_<lastname>.public.customer_loyalty cl SAMPLE (10000 ROWS)
GROUP BY cl.customer_id, cl.first_name, cl.last_name, cl.city, cl.marital_status, age;

 -- as we did for our masking, let's double check our row level security is flowing into the downstream analytic views.
USE ROLE tasty_test_role;

SELECT
    clm.city,
    SUM(clm.total_sales) AS total_sales_usd
FROM FROSTBYTE_TASTY_BYTES.HARMONIZED.CUSTOMER_LOYALTY_METRICS_V_<firstname>_<lastname> clm
GROUP BY clm.city;

/*----------------------------------------------------------------------------------
Step 5 - (Optional) Automatic Data Classification - *Enterprise Edition+ required

  In some cases, you may not know if there is sensitive data in a table. Snowflake
  provides the capability to attempt to automatically detect sensitive information 
  and apply relevant Snowflake system defined privacy tags. 

  Classification is a multi-step process that associates Snowflake-defined tags (i.e. 
  system tags) to columns by analyzing the cells and metadata for personal data; this 
  data can now be tracked by a data engineer.

  Let's see it in action!
----------------------------------------------------------------------------------*/

USE ROLE SYSADMIN;
CALL ASSOCIATE_SEMANTIC_CATEGORY_TAGS(
   'tb_101.raw_customer.customer_loyalty',
    EXTRACT_SEMANTIC_CATEGORIES('tb_101.raw_customer.customer_loyalty')
);

-- let's view the new tags Snowflake applied automatically via Data Classification
SELECT *
FROM TABLE(
  frostbyte_tasty_bytes.information_schema.TAG_REFERENCES_ALL_COLUMNS(
    'tb_101.raw_customer.CUSTOMER_LOYALTY',
    'table'
  )
);



/*----------------------------------------------------------------------------------
Bonus! 
    The governance updates we have made are now visible in the Snowsight UI.
    To view them: 
        1) Search for "CUSTOMER_LOYALTY" in the object tree to the left
        2) Under FROSTBYTES_TASTY_BYTES > RAW_CUSTOMER, hover over the 
           CUSTOMER_LOAYALTY table and click the icon in the upper right.
        3) A new tab will open displaying the table. 
            * You will see the Row Access Policy on the "Table Details" tab
            * Click on the "Columns" tab to view the Tags and Masking Policies
              on each column.
              
     If a user has the privileges to do so, they can set tags and policies directly
     from the UI. This enables easy delegation of duties to less technical team 
     members.
----------------------------------------------------------------------------------*/



/*----------------------------------------------------------------------------------
Step 6 -
    Perfect! Everything functioned as we expected and we can now use these skills
    to ensure we are deploying RBAC and Data Governance effectively and early.

    Let's now finish our task by cleaning up all of the objects we created.
----------------------------------------------------------------------------------*/


ALTER TAG <firstname>_<lastname>.tags.tasty_pii UNSET 
    MASKING POLICY <firstname>_<lastname>.governance.tasty_pii_string_mask,
    MASKING POLICY <firstname>_<lastname>.governance.tasty_pii_date_mask;

-- with the masking objects clear, we will now DROP our Row Access Policy from our Table
ALTER TABLE tb_101.raw_customer.customer_loyalty
DROP ROW ACCESS POLICY <firstname>_<lastname>.governance.customer_city_row_policy;

-- Unset the system tags that may have been set by Data Classification
ALTER TABLE <firstname>_<lastname>.public.customer_loyalty MODIFY
    COLUMN first_name UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN last_name UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN e_mail UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN city UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN country UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN gender UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN marital_status UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN birthday_date UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN phone_number UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category,
    COLUMN postal_code UNSET TAG snowflake.core.privacy_category, snowflake.core.semantic_category;

-- next we will DROP our TAGS and GOVERNANCE schemas (and thus everything within)
DROP SCHEMA <firstname>_<lastname>.tags;
DROP SCHEMA <firstname>_<lastname>.governance;