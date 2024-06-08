/*----------------------------------------------------------------------------------
Step 5 - Pipeline Automation
 
 In the near-future Tasty Bytes trucks will be able to accept online orders and the 
 software for this system will insert data directly into the same raw order tables
 that our Truck POS systems will be batch loading into.
 
 With our truck order data accounted for, our Tasty Bytes Data Engineer can now begin
 to work on automating the entire ORDER data pipeline. At a high-level, we want a
 series of tasks that will kick off at midnight  and complete the following:
 
 • Refresh the Development Database via a Production Clone
 • Load any new Order Header and Order Detail files
 • Update our aggregate sales table with a new daily row
----------------------------------------------------------------------------------*/

USE ROLE tasty_data_engineer;
USE WAREHOUSE TASTY_DE_WH;

/* CRITICAL STEP: DO NOT SKIP */
-- FIND AND REPLACE THE STRING "<firstname>_<lastname>" WITH YOUR NAME EX. "SRIDHAR_RAMASWAMY"
CREATE DATABASE frostbyte_tasty_bytes_v2_<firstname>_<lastname> CLONE frostbyte_tasty_bytes_v2;
/* ========================== */

-- so that we can track new inserts to these order tables, let's leverage Snowflake Streams
    --> first, let's create an append-only order_header stream
CREATE OR REPLACE STREAM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.order_header_stream
ON TABLE frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.order_header
    APPEND_ONLY = true -- tracks row inserts only
    SHOW_INITIAL_ROWS = false; -- do not include already inserted records

    --> next, let's create a similar stream on the order_detail table
CREATE OR REPLACE STREAM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.order_detail_stream
ON TABLE frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.order_detail
    APPEND_ONLY = true -- tracks row inserts only
    SHOW_INITIAL_ROWS = false; -- do not include already inserted records

   /*--
     STREAMS: allow developers to track changes (DML) to a table overtime which
     can be coupled with scheduling to process incremental changes as part of a data pipeline
    --*/

-- with the streams in place, let's generate 5 test order_header records
INSERT INTO frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.order_header (order_id, order_ts)
VALUES
(999999995, CURRENT_TIMESTAMP),
(999999996, CURRENT_TIMESTAMP),
(999999997, CURRENT_TIMESTAMP),
(999999998, CURRENT_TIMESTAMP),
(999999999, CURRENT_TIMESTAMP);


-- for the new order_id's, let's now insert 10 test order_detail line item records including price
INSERT INTO frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.order_detail (order_detail_id, order_id, line_number, price)
VALUES
(9999999950, 999999995, 0, 10),
(9999999951, 999999995, 1, 20),
(9999999952, 999999995, 2, 40),
(9999999960, 999999996, 0, 5),
(9999999970, 999999997, 0, 100),
(9999999980, 999999998, 0, 25),
(9999999981, 999999998, 1, 50),
(9999999990, 999999999, 0, 5),
(9999999991, 999999999, 1, 20),
(9999999992, 999999999, 2, 25);


-- with test data inserted, let's query these streams to produce a new daily aggregate sales and order count row
SELECT 
    TO_DATE(ohs.order_ts) AS date,
    COUNT(DISTINCT ohs.order_id) AS daily_orders,
    SUM(ods.price) AS daily_sales
FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.order_header_stream ohs
JOIN frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.order_detail_stream ods
    ON ohs.order_id = ods.order_id
GROUP BY date
ORDER BY date DESC;


/*---------------------------------------------------------------------------------
With all of our building blocks in place, let's now action on our end to end data
pipeline. First, let's review what should be included once again below:
 
 • Refresh the Development Database via a Product Clone
 • Load any new Order Header and Order Detail files
 • Update our aggregate sales table with a new daily row
 
Based on these requirements, let's leverage a Directed Acyclic Graph (DAG) of 
Snowflake Serverless Tasks
----------------------------------------------------------------------------------*/

-- for our root task, let's start with the daily refresh of our developers database by cloning production
CREATE OR REPLACE TASK frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.tb_dev_clone_task
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'x-small' -- serverless task starting with x-small warehouse
SCHEDULE = 'USING CRON 0 0 * * * UTC' -- run at Midnight UTC every day
    AS
CREATE OR REPLACE DATABASE frostbyte_tasty_bytes_v2_<firstname>_<lastname>_dev CLONE frostbyte_tasty_bytes_v2_<firstname>_<lastname>;

    /*---
    DAG OF TASKS: series of tasks composed of a single root task and additional tasks, organized by their dependencies.
     DAGs flow in a single  direction, meaning a task later in the series cannot prompt the run of an earlier task. 
     Each task (except the root task) can have multiple predecessor tasks (dependencies); likewise, each task can
     have multiple subsequent (child) tasks that depend on it. A task runs only after all of its predecessor tasks
     have run successfully to completion.

    SERVERLESS TASK: model for tasks enables you to rely on compute resources managed by Snowflake instead of user-managed
     virtual warehouses. The compute resources are automatically resized and scaled up or down by Snowflake as required
     for each workload. Snowflake determines the ideal size of the compute resources for a given run based on a dynamic
     analysis of statistics for the most recent previous runs of the same task.

    CLONE: creates a copy of a database, schema or table. A snapshot of data present in the source object is taken when
     the clone is created and is made available to the cloned object. The cloned object is writable and is independent
     of the clone source. That is, changes made to either the source object or the clone object are not part of the other.
    ---*/


/*---------------------------------------------------------------------------------
 After our root task, we will want to load both order_header and order_detail using
 our COPY INTO's from earlier but these can both kick-off asynchronously.
----------------------------------------------------------------------------------*/

-- create our order_header task to run AFTER our root task
CREATE OR REPLACE TASK frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.order_header_load_task
AFTER frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.tb_dev_clone_task -- kick off after our Development Clone is complete
    AS
    COPY INTO frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.order_header
    FROM @frostbyte_tasty_bytes_v2_<firstname>_<lastname>.public.order_stage/csv/order_header/
    FILE_FORMAT = 
        (
            FORMAT_NAME = 'frostbyte_tasty_bytes_v2_<firstname>_<lastname>.public.csv_ff'
        );

-- create our order_detail task to also run AFTER our root task
CREATE OR REPLACE TASK frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.order_detail_load_task
AFTER frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.tb_dev_clone_task -- kick off after our Development Clone is complete
    AS
    COPY INTO frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.order_detail
    FROM 
        (
        SELECT 
            $1:"ORDER_DETAIL_ID"::NUMBER(38,0) AS order_detail_id,
            $1:"ORDER_ID"::NUMBER(38,0) AS order_id,
            $1:"MENU_ITEM_ID"::NUMBER(38,0) AS menu_item_id,
            $1:"DISCOUNT_ID"::NUMBER(38,0) AS discount_id,
            $1:"LINE_NUMBER"::NUMBER(5,0) AS line_number,
            $1:"QUANTITY"::NUMBER(5,0) AS quantity,
            $1:"PRICE"::NUMBER(38,6) AS price,
            $1:"UNIT_PRICE"::NUMBER(38,6) AS unit_price,
            $1:"ORDER_ITEM_DISCOUNT_AMOUNT"::NUMBER(34,4) AS order_item_discount_amount
        FROM @frostbyte_tasty_bytes_v2_<firstname>_<lastname>.public.order_stage/json/order_detail/
            (FILE_FORMAT => frostbyte_tasty_bytes_v2_<firstname>_<lastname>.public.json_ff) --reference our file format we created earlier
        );


-- before we can insert the new daily aggregate record we need to make sure both loads are complete
-- so let's make sure to kick off this task only AFTER both load tasks have been
CREATE OR REPLACE TASK frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.aggregate_sales_daily_task
AFTER frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.order_header_load_task,
        frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.order_detail_load_task -- kick off after both of our order tables are finished loading
WHEN SYSTEM$STREAM_HAS_DATA('frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.order_header_stream') -- only run this task if our stream has new header records
    AS
INSERT INTO frostbyte_tasty_bytes_v2_<firstname>_<lastname>.harmonized.aggregate_sales_daily
    SELECT
        TO_DATE(ohs.order_ts) AS date,
        COUNT(DISTINCT ohs.order_id) AS daily_orders,
        SUM(ods.price) AS daily_sales
    FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.order_header_stream ohs
    JOIN frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.order_detail_stream ods
        ON ohs.order_id = ods.order_id
    GROUP BY date
    ORDER BY date DESC;


-- tasks are created in a suspended state, so let's now leverage a system function to enable the entire DAG
SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.tb_dev_clone_task');


-- within Snowflake, task visibility is available at many levels. to start, let's confirm our schedule and dependencies look correct
    --> demo tip: call out WAREHOUSE = NULL indicating Serverless Tasks in place
SELECT 
    name,
    state,
    warehouse,
    schedule,
    predecessors
FROM TABLE(frostbyte_tasty_bytes_v2_<firstname>_<lastname>.information_schema.task_dependents
    (task_name=>'frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.tb_dev_clone_task'));


-- based on what we have seen our DAG of tasks will kick off at midnight UTC everyday moving forward, however as we have inserted some test
-- records let's manually trigger our pipeline run
EXECUTE TASK frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.tb_dev_clone_task;


/*---------------------------------------------------------------------------------
  Moving forward we need to make sure we can monitor our pipelines within Snowflake.
  Before getting into data analysis, let's first observe our scheduled pipeline
  as well as the one-off execution we kicked off.
----------------------------------------------------------------------------------*/

-- let's leverage task_history to confirm our pipeline is scheduled for midnight tonight
SELECT 
    name, 
    state,
    query_start_time,
    completed_time,
    next_scheduled_time,
    scheduled_from,
    query_text
FROM TABLE(frostbyte_tasty_bytes_v2_<firstname>_<lastname>.information_schema.task_history())
WHERE 1=1
    AND scheduled_from = 'SCHEDULE' -- task is from a schedule
ORDER BY query_start_time;


-- now what about our one-off run? let's take a look now searching for those scheduled from manual task execution
SELECT 
    name, 
    state,
    query_start_time,
    completed_time,
    next_scheduled_time,
    scheduled_from,
    query_text
FROM TABLE(frostbyte_tasty_bytes_v2_<firstname>_<lastname>.information_schema.task_history())
WHERE 1=1
    AND scheduled_from = 'EXECUTE TASK' -- task is a one-off run
    AND TO_DATE(query_start_time) = CURRENT_DATE() -- tasks for today
    AND TIMEDIFF(MINUTE,CURRENT_TIMESTAMP,QUERY_START_TIME) > -5
ORDER BY query_start_time;


-- great everything is running as expected, let's now check to confirm the final task finished
SELECT * 
FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.analytics.aggregate_sales_daily_v
ORDER BY DATE DESC;


-- yay! it looks like everything is running as expected. before we move on, let's drop the test
-- rows from our order tables

    --> order_detail
    DELETE FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.order_detail 
    WHERE order_id IN (SELECT DISTINCT order_id FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.order_header WHERE truck_id IS NULL);

    --> order_header
    DELETE FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.raw_pos.order_header WHERE truck_id IS NULL;

    --> aggregate_sales daily
    DELETE FROM frostbyte_tasty_bytes_v2_<firstname>_<lastname>.harmonized.aggregate_sales_daily WHERE daily_sales < 10000;