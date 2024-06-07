![sunlife-snowflake](https://raw.githubusercontent.com/sfc-gh-mwies/sunlife-hol/main/img/sunlife-snowflake.png)
# Getting Started with Snowflake - Zero to Snowflake

## Overview

### What You'll Learn:
- How to create stages, databases, tables, views, and virtual warehouses.
- How to load structured and semi-structured data.
- How to consume Cybersyn data from the [Snowflake Data Marketplace](https://app.snowflake.com/marketplace/listing/GZTSZAS2KF7/).
- How to perform analytical queries on data in Snowflake, including joins between tables.
- How to clone objects.
- How to undo user errors using Time Travel.
- How to securely and easily share data with other accounts.

### Data You'll Use:
**Cybersyn** is a next generation data company creating a real-time view of the world's economy with analytics-ready data exclusively on Snowflake Marketplace. Initially focused on consumer insights, Cybersyn enables you to access external data directly in your Snowflake instance — no ETL required.

This lab will use the following Cybersyn datasets:
- Daily stock price data
- SEC 10-K & 10-Q filings
- Company metadata

## The Snowflake ​User Interface

>  **About the screenshots, sample code, and environment:** Screenshots in this lab depict examples; results may vary slightly from what you see when you complete the exercises.

### Navigating the Snowflake UI

Let's get you acquainted with Snowflake! This section covers the basic components of the user interface.

![snowflake navbar](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/3UIStory_2.png?raw=true)

### Projects > Worksheets

**Let's Get Logged in!** -- Navigate to your hands-on lab web UI and log in using the username and password provided.

Under **Projects** on the left-hand panel, select the ​**Worksheets​** tab. Create a new worksheet by clicking **`+`** on the top right.

![worksheets tab main](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/3UIStory_3.png?raw=true)

### Worksheet Structure 

![worksheets tab details](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/3UIStory_4.png?raw=true)

The top left corner contains the following:
- **Snowflake** icon: Use this to get back to the main console/close the worksheet.
- **Worksheet_name** drop-down: Click the timestamp to edit the worksheet name.
- **Filters** button: Custom filters are special keywords that resolve as a subquery or list of values.

The top right corner contains the following:
- **Context** box: This lets Snowflake know which role and warehouse to use during this session. It can be changed via the UI or SQL commands.
- **Share** button: Open the sharing menu to share to other users or copy the link to the worksheet.

The bottom pane displays the results of queries and other operations. Also includes 4 options (**Object**, **Query**, **Result**, **Chart**) that open/close their respective panels on the UI. **Chart** opens a visualization panel for the returned results. More on this later.

### Projects > Dashboards

Under **Projects** on the left-hand panel, select the ​**Dashboards​** tab. This tab allows you to create flexible displays of one or more charts (in the form of tiles, which can be rearranged). Tiles and widgets are produced by executing SQL queries that return results in a worksheet. Dashboards work at a variety of sizes with minimal configuration.

![dashboards tab](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/3UIStory_5.png?raw=true)

### Data > Databases

Under **Data**, the **Databases**​ tab shows information about the databases you have created or have permission to access. You can create, clone, drop, or transfer ownership of databases, as well as load data in the UI. Notice that a database already exists in your environment.

![databases tab](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/3UIStory_6.png?raw=true)

### Data Products > Marketplace

The **Marketplace** tab is where any Snowflake customer can browse and consume data sets made available by providers. There are two types of shared data: Public and Personalized. Public data is free data sets available for querying instantaneously. Personalized data requires reaching out to the provider of data for approval of sharing data.

![marketplace tab](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/3UIStory_8.png?raw=true)

### Data Products > Private Sharing

Under **Data Products**, the **Private Sharing** tab is where data sharing can be configured to easily and securely share Snowflake tables among separate Snowflake accounts or external users, without having to create a copy of the data.

![shared data tab](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/3UIStory_7.png?raw=true)

### Monitoring > Query History

Under **Monitoring** there are multiple tabs for tracking your usage of your Snowflake account:

- **Query History** is where previous queries are shown, along with filters that can be used to hone results (user, warehouse, status, query tag, etc.). View the details of all queries executed in the last 14 days from your Snowflake account. Click a query ID to drill into it for more information.
- **Copy History** shows the status of copy commands run to ingest data into Snowflake.
- **Task History** allows you to see the execution history for tasks and tasks graphs. (Tasks let you schedule the execution of SQL code. It is associated with a specific database and schema.)
- **Dynamic Tables** is where you can use Snowsight to monitor dynamic table refreshes and examine dynamic tables and dynamic table graphs.
- **Governance** tracks row- and column-level security, object tagging, data classification, access history, and more.

![history tab](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/3UIStory_9.png?raw=true)

---
> **NOTE:** THE ADMIN TAB WILL NOT BE VISIBLE TO NON-ADMIN USERS

### Admin > Warehouses

Under **Admin**, the **​Warehouses​** tab is where you set up and manage compute resources known as virtual warehouses to load or query data in Snowflake. A warehouse called COMPUTE_WH already exists in your environment.

![warehouses tab](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/3UIStory_10.png?raw=true)

### Admin > Cost Management

Under **Admin**, the **Cost Management** tab shows an overview of account consumption and budgets. It also includes details on all the resource monitors that have been created to control the number of credits that virtual warehouses consume. For each resource monitor, it shows the credit quota, type of monitoring, schedule, and actions performed when the virtual warehouse reaches its credit limit.

![resource monitors tab](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/3UIStory_11.png?raw=true)

### Admin > Users & Roles

The **Roles** sub-tab of the **Users & Roles** tab shows a list of the roles and their hierarchies. Roles can be created, reorganized, and granted to users in this tab. The roles can also be displayed in tabular/list format by selecting the **Table** sub-tab.

![roles tab](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/3UIStory_12.png?raw=true)

### Users

The **Users** sub-tab of the **Users & Roles** tab shows a list of users in the account, default roles, and owner of the users. For a new account, no records are shown because no additional roles have been created. Permissions granted through your current role determine the information shown for this tab. To see all the information available on the tab, switch your role to `ACCOUNTADMIN`.

![users tab](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/3UIStory_13.png?raw=true)

---
---

>  **USER MENU**
Clicking on your username in the bottom right of the UI allows you to change your password, roles, and preferences. Snowflake has several system defined roles. You are currently in the default role of `SNOWPARK_HOL_ROLE` and will stay in this role for the majority of the lab.

![user preferences dropdown](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/3UIStory_14.png?raw=true)
 
>  **System Roles**
System Roles like `SYSADMIN`, `ACCOUNTADMIN`, `SECURITYADMIN`, `USERADMIN`, `ORGADMIN` exist out of the box in every Snowflake account. They have specific purposes and cannot be dropped. These are typically reserved for Power Users and Snowfalke Admins.
You can find additional information in the [Snowflake documentation](https://docs.snowflake.net/manuals/user-guide/security-access-control.html).

## Data Lab: Stock Price & SEC Filings Data

### The Lab Scenario
You work at a grocery retailer. You want to understand the performance of major consumer goods (CPG) companies in the US that supply your store. This lab takes a look at daily stock price data and quarterly and annual Securities Exchange Commission (SEC) company filings to understand the performance of the CPG landscape. Public companies are required to submit a quarterly and annual report to the SEC detailing their financial data.

We will start by collecting data from three different sources:
1. Load company metadata from a structured `.csv` file.
2. Load SEC filings from a semi-structured JSON format.
3. Use the Snowflake Marketplace to find free stock price data from Cybersyn.


## 1. Loading Structured Data into Snowflake: CSVs

Let's start by preparing to load structured `.csv` data into Snowflake.

We are using company metadata developed from the Securities and Exchange Commission (SEC) that details the consumer packaged goods (CPG) companies we want to evaluate. The data has been exported and pre-staged for you in an Amazon AWS S3 bucket in the US-EAST region. It is in comma-delimited format with a single header line and double quotes enclosing all string values, including the field headings in the header line. This will be important when we configure the Snowflake table to store this data.

**Getting Data into Snowflake**
Data can be ingested into Snowflake from many locations by using the `COPY` command, Snowpipe auto-ingestion, external connectors, or third-party ETL/ELT solutions. For more information on getting data into Snowflake, see the [Snowflake documentation](https://docs.snowflake.net/manuals/user-guide-data-load.html). For the purposes of this lab, we use the `COPY` command and AWS S3 storage to load data manually. In a real-world scenario, you would more likely use an ETL solution or grab data directly from the Snowflake Marketplace!

### Create a Database and Table
Ensure you are using the `SNOWPARK_HOL_ROLE` role by selecting your name at the top left, **Switch Role** > **SNOWPARK_HOL_ROLE**.

Navigate to the **Databases** tab. Click **Create**, name the database `<firstname>_<lastname>_CYBERSYN`, then click **CREATE**.

![worksheet creation](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/4PreLoad_2.png?raw=true)

Now navigate to the **Worksheets** tab. You should see the worksheet we created earlier.

![new worksheet](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/4PreLoad_3.png?raw=true)

We need to set the context appropriately within the worksheet. In the upper right corner of the worksheet, click the box to the left of the **Share** button to show the context menu. Here we control the elements you can see and run from each worksheet. We are using the UI here to set the context. Later in the lab, we will accomplish the same thing via SQL commands within the worksheet.

Select the following context settings:

**Role:** `SNOWPARK_HOL_ROLE`
**Warehouse:** `SNOWPARK_HOL_VWH`

![context role and warehouse settings](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/4PreLoad_4.png?raw=true)

Next, in the drop-down for the database, select the following context settings:

**Database:** `<firstname>_<lastname>_CYBERSYN`
**Schema:** `PUBLIC`

![context database settings](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/4PreLoad_4b.png?raw=true)

>  **Data Definition Language (DDL) operations are free!**
All the DDL operations we have done so far do not require compute resources, so we can create all our objects for free.

To make working in the worksheet easier, let's rename it. In the top left corner, double-click the worksheet name, which is the timestamp when the worksheet was created, and change it to `ZERO_TO_SNOWFLAKE_WITH_CYBERSYN`.

Next we create a table called `COMPANY_METADATA` to use for loading the comma-delimited data. Instead of using the UI, we use the worksheet to run the DDL that creates the table. Copy the following SQL text into your worksheet:

```SQL
CREATE OR REPLACE TABLE company_metadata
(cybersyn_company_id string,
company_name string,
permid_security_id string,
primary_ticker string,
security_name string,
asset_class string,
primary_exchange_code string,
primary_exchange_name string,
security_status string,
global_tickers variant,
exchange_code variant,
permid_quote_id variant);
```


>  **Many Options to Run Commands.**
SQL commands can be executed via the **Worksheets** tab, using our SnowSQL command line tool, with a SQL editor of your choice, or through our other connectors (Python, Spark, etc.).

Run the query by placing your cursor anywhere in the SQL text and clicking the blue **Play/Run** button in the top right of the worksheet. Or use the keyboard shortcut [Ctrl]/[Cmd]+[Enter].

Verify your `COMPANY_METADATA` table has been created. At the bottom of the worksheet, you should see a Results section displaying a `"Table COMPANY_METADATA successfully created"` message.

![TRIPS confirmation message](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/4PreLoad_5.png?raw=true)

Navigate to the **Databases** tab by clicking the **HOME** icon in the upper left corner of the worksheet. Then click **Data** > **Databases**. In the list of databases, click `<firstname>_<lastname>_CYBERSYN` > `PUBLIC` > **TABLES** to see your newly created `COMPANY_METADATA` table.

![TRIPS table](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/4PreLoad_6.png?raw=true)

Click `COMPANY_METADATA` and the **Columns** tab to see the table structure you just created.

![TRIPS table structure](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/4PreLoad_7.png?raw=true)

### Create an External Stage

We are working with structured, comma-delimited data that has already been staged in a public, external S3 bucket. Before we can use this data, we first need to create a _stage_ that specifies the location of our external bucket.

>  For this lab, we are using an AWS-East bucket. To prevent data egress/transfer costs in the future, you should select a staging location from the same cloud provider and region as your Snowflake account.

From the **Databases** tab, click the `<firstname>_<lastname>_CYBERSYN` database and `PUBLIC` schema. Click the **Create** button, then **Stage** > **Amazon S3**.

![stages create](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/4PreLoad_8.png?raw=true)

In the `Create Securable Object` dialog that opens, replace the following values in the SQL statement:

**Stage Name**: `cybersyn_company_metadata`
**URL**: `s3://sfquickstarts/zero_to_snowflake/cybersyn-consumer-company-metadata-csv/cybersyn_consumer_company_metadata.csv`

**Note:** Make sure to include the final forward slash (`/`) at the end of the URL or you will encounter errors later when loading data from the bucket.
Also ensure you have removed 'credentials = (...)' statement which is not required. You can also comment it out like the picture below by using '--'. The create stage command should resemble the below picture or not include the 3rd line.

>  The S3 bucket for this lab is public so you can leave the credentials options in the statement empty. In a real-world scenario, the bucket used for an external stage would likely require key information.

![create stage settings](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/4PreLoad_9.png?raw=true)

Now let's take a look at the contents of the `cybersyn_company_metadata` stage. Navigate back to the **Worksheets** tab and open the `ZERO_TO_SNOWFLAKE_WITH_CYBERSYN` worksheet we made.

Add the following SQL statement below the previous code and then execute:

```SQL
LIST @cybersyn_company_metadata;
```

In the results in the bottom pane, you should see the list of files in the stage:

![worksheet result](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/4PreLoad_10.png?raw=true)

### Create a File Format

Before we can load the data into Snowflake, we have to create a file format that matches the data structure. In the worksheet, again add the following command below the rest and execute to create the file format:

```SQL
CREATE OR REPLACE FILE FORMAT csv
    TYPE = 'CSV'
    COMPRESSION = 'AUTO'  -- Automatically determines the compression of files
    FIELD_DELIMITER = ','  -- Specifies comma as the field delimiter
    RECORD_DELIMITER = '\n'  -- Specifies newline as the record delimiter
    SKIP_HEADER = 0  -- No headers to skip, starts reading from the first line
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042'  -- Fields are optionally enclosed by double quotes (ASCII code 34)
    TRIM_SPACE = FALSE  -- Spaces are not trimmed from fields
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE  -- Does not raise an error if the number of fields in the data file varies
    ESCAPE = 'NONE'  -- No escape character for special character escaping
    ESCAPE_UNENCLOSED_FIELD = '\134'  -- Backslash is the escape character for unenclosed fields
    DATE_FORMAT = 'AUTO'  -- Automatically detects the date format
    TIMESTAMP_FORMAT = 'AUTO'  -- Automatically detects the timestamp format
    NULL_IF = ('')  -- Treats empty strings as NULL values
    COMMENT = 'File format for ingesting data for zero to snowflake';
```

![create file format](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/4PreLoad_11.png?raw=true)

Verify the file format has been created with the correct settings by executing the following command:

```SQL
SHOW FILE FORMATS IN DATABASE cybersyn;
```

The file format created should be listed in the result:
![create file format settings](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/4PreLoad_12.png?raw=true)


### Load the Data

Now we can run a COPY command to load the data into the `COMPANY_METADATA` table we created earlier.

Navigate back to the `ZERO_TO_SNOWFLAKE_WITH_CYBERSYN` worksheet in the **Worksheets** tab. Make sure the worksheet context is correctly set:

**Role:** `SNOWPARK_HOL_ROLE`
**Warehouse:** `SNOWPARK_HOL_VWH`
**Database:** `<firstname>_<lastname>_CYBERSYN`
**Schema:** `PUBLIC`

![worksheet context](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/5Load_4.png?raw=true)

Execute the following statements in the worksheet to load the staged data into the table. This may take up to 30 seconds.

```SQL
COPY INTO company_metadata FROM @cybersyn_company_metadata file_format=csv PATTERN = '.*csv.*' ON_ERROR = 'CONTINUE';
```

In the result pane, you should see the status of each file that was loaded. Once the load is done, in the **Query Details** pane on the bottom right, you can scroll through the various statuses, error statistics, and visualizations for the last statement executed.

Next, navigate to the **Query History** tab by clicking the **Home** icon and then **Activity** > **Query History**. Select the query at the top of the list, which should be the COPY INTO statement that was last executed. Select the **Query Profile** tab and note the steps taken by the query to execute, query details, most expensive nodes, and additional statistics.

![history and duration](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/5Load_6.png?raw=true)


## 2.Loading Semi-Structured Data into Snowflake: JSONs

>  This section requires loading additional data and, therefore, provides a review of data loading while also introducing loading semi-structured data.

Going back to the lab's example, our company's analytics team wants to evaluate the performance of CPG companies through the lens of their reported metrics in SEC filings. To do this, in this section, we will:

- Load SEC filing data in semi-structured JSON format held in a public S3 bucket.
- Create a view and query the JSON data using SQL dot notation.

The JSON data consists of SEC filings provided by *Cybersyn*, detailing the historical performance of consumer-packaged goods companies from 2019-2023. It is also staged on AWS S3. If viewed in a text editor, the raw JSON in the GZ files looks like:

![raw JSON sample](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/7SemiStruct_1_1.png?raw=true)

_(The full dataset available [**for free**](https://app.snowflake.com/marketplace/listing/GZTSZAS2KH9/) in Snowflake Marketplace from Cybersyn -- no ETL required. For the purposes of this demo, we will focus on working with the semi-structured JSON file to learn how to load structured data into Snowflake.)_

>  **SEMI-STRUCTURED DATA**
Snowflake can easily load and query semi-structured data such as JSON, Parquet, or Avro without transformation. This is a key Snowflake feature because an increasing amount of business-relevant data being generated today is semi-structured, and many traditional data warehouses cannot easily load and query such data. Snowflake makes it easy!

### Create a New Table for the Data

>  **Executing Multiple Commands** Remember that you need to execute each command individually. However, you can execute them in sequence together by selecting all of the commands and then clicking the **Play/Run** button (or using the keyboard shortcut).

Next, let's create two tables, `SEC_FILINGS_INDEX` and `SEC_FILINGS_ATTRIBUTES` to use for loading JSON data. In the worksheet, execute the following `CREATE TABLE` commands:

```SQL
CREATE TABLE sec_filings_index (v variant);

CREATE TABLE sec_filings_attributes (v variant);
```

Note that Snowflake has a special data type called `VARIANT` that allows storing the entire JSON object as a single row and querying the object directly.

>  **Semi-Structured Data Magic**
The `VARIANT` data type allows Snowflake to ingest semi-structured data without having to predefine the schema.

In the results pane at the bottom of the worksheet, verify that your tables, `SEC_FILINGS_INDEX` and `SEC_FILINGS_ATTRIBUTES`, were created:

![success message](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/7SemiStruct_2_1.png?raw=true)

### Create Another External Stage

In the `ZERO_TO_SNOWFLAKE_WITH_CYBERSYN` worksheet, use the following command to create a stage that points to the bucket where the semi-structured JSON data is stored on AWS S3:

```SQL
CREATE STAGE cybersyn_sec_filings
url = 's3://sfquickstarts/zero_to_snowflake/cybersyn_cpg_sec_filings/';
```

Now let's take a look at the contents of the `cybersyn_sec_filings` stage. Execute the following `LIST` command to display the list of files:

```SQL
LIST @cybersyn_sec_filings;
```

In the results pane, you should see a list of `.gz` files from S3:
![results output](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/7SemiStruct_3_1.png?raw=true)

### Load and Verify the Semi-structured Data

We will now use a warehouse to load the data from an S3 bucket into the tables we created earlier. In the `ZERO_TO_SNOWFLAKE_WITH_CYBERSYN` worksheet, execute the `COPY` command below to load the data.

Note that you can specify a `FILE FORMAT` object inline in the command. In the previous section where we loaded structured data in CSV format, we had to define a file format to support the CSV structure. Because the JSON data here is well-formed, we are able to simply specify the JSON type and use all the default settings:

```SQL
COPY INTO sec_filings_index
FROM @cybersyn_sec_filings/cybersyn_sec_report_index.json.gz
    file_format = (type = json strip_outer_array = true);

COPY INTO sec_filings_attributes
FROM @cybersyn_sec_filings/cybersyn_sec_report_attributes.json.gz
    file_format = (type = json strip_outer_array = true);
```

Verify that each file has a status of `LOADED`:
![query result](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/7SemiStruct_4_1.png?raw=true)

Now, let's take a look at the data that was loaded:
```SQL
SELECT * FROM sec_filings_index LIMIT 10;
SELECT * FROM sec_filings_attributes LIMIT 10;
```

Click any of the rows to display the formatted JSON in the right panel:

![JSON data snippet](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/7SemiStruct_5_1.png?raw=true)

To close the display in the panel and display the query details again, click the **X** (Close) button that appears when you hover your mouse in the right corner of the panel.

### Create a View and Query Semi-Structured Data

Next, let's look at how Snowflake allows us to create a view and also query the JSON data directly using SQL.

>  **Views & Materialized Views**
A view allows the result of a query to be accessed as if it were a table. Views can help present data to end users in a cleaner manner, limit what end users can view in a source table, and write more modular SQL.

Snowflake also supports materialized views in which the query results are stored as though the results are a table. This allows faster access, but requires storage space. Materialized views can be created and queried if you are using Snowflake Enterprise Edition (or higher).

Run the following command to create a columnar view of the semi-structured JSON SEC filing data, so it is easier for analysts to understand and query. The CIK corresponds to the Central Index Key, or unique identifier that SEC gives to each filing entity. The ADSH is the document number for any filing submitted to the SEC.

```SQL
CREATE OR REPLACE VIEW sec_filings_index_view AS
SELECT
    v:CIK::string                   AS cik,
    v:COMPANY_NAME::string          AS company_name,
    v:EIN::int                      AS ein,
    v:ADSH::string                  AS adsh,
    v:TIMESTAMP_ACCEPTED::timestamp AS timestamp_accepted,
    v:FILED_DATE::date              AS filed_date,
    v:FORM_TYPE::string             AS form_type,
    v:FISCAL_PERIOD::string         AS fiscal_period,
    v:FISCAL_YEAR::string           AS fiscal_year
FROM sec_filings_index;

CREATE OR REPLACE VIEW sec_filings_attributes_view AS
SELECT
    v:VARIABLE::string            AS variable,
    v:CIK::string                 AS cik,
    v:ADSH::string                AS adsh,
    v:MEASURE_DESCRIPTION::string AS measure_description,
    v:TAG::string                 AS tag,
    v:TAG_VERSION::string         AS tag_version,
    v:UNIT_OF_MEASURE::string     AS unit_of_measure,
    v:VALUE::string               AS value,
    v:REPORT::int                 AS report,
    v:STATEMENT::string           AS statement,
    v:PERIOD_START_DATE::date     AS period_start_date,
    v:PERIOD_END_DATE::date       AS period_end_date,
    v:COVERED_QTRS::int           AS covered_qtrs,
    TRY_PARSE_JSON(v:METADATA)    AS metadata
FROM sec_filings_attributes;
```

SQL dot notation `v:VARIABLE` is used in this command to pull out values at lower levels within the JSON object hierarchy. This allows us to treat each field as if it were a column in a relational table.

The new view should appear as `SEC_FILINGS_INDEX_VIEW` under `<firstname>_<lastname>_CYBERSYN` > `PUBLIC` > **Views** in the object browser on the left. You may need to expand or refresh the objects browser in order to see it.

![JSON_WEATHER_DATA _VIEW in dropdown](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/7SemiStruct_6_1.png?raw=true)

Notice the results look just like a regular structured data source: 

```SQL
SELECT *
FROM sec_filings_index_view
LIMIT 20;
```

## 3. Getting Data from Snowflake Marketplace

### Snowflake Data Marketplace

This section was designed for users with the ACCOUNTADMIN role. Because we're using a custom lab role, you will be able to browse the marketplace, but the data set has already been provisioned for you.

#### Find a listing

Type `stock prices` in the search box at the top, scroll through the results, and select [**Financial & Economic Essentials**](https://app.snowflake.com/marketplace/listing/GZTSZAS2KF7/) (provided by Cybersyn).

![health tab](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/10Share_8.png?raw=true)  

In the **Cybersyn Financial & Economic Essentials** page, you can learn more about the contents of the data listing, explore data dictionaries, and see some sample queries. You will also see links to documentation and the dataset's cloud region availability.

If the data set had not yet been provisioned, you would see a "Get" button (see screenshot) -- For now you can click the "Open" button

![get data fields](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/10Share_cybersyn_get_data.png?raw=true)

When you choose "Open", a new worksheet opens in a new browser tab/window:
1. Set your context 
2. Select the query you want to run (or place your cursor in the query text).
3. Click the **Run/Play** button (or use the keyboard shortcut).
4. You can view the data results in the bottom pane.
5. When you are done running the sample queries, click the **Home** icon in the upper left corner.

![get data fields](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/10Share_cybersyn_query_data2.png?raw=true)

Next:
1. Click **Data** > **Databases**.
2. Click the `Financial__Economic_Essentials` database.
3. You can see details about the schemas, tables, and views that are available to query.

![covid19 databases](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/10Share_cybersyn_db_info.png?raw=true)

That's it! You have now successfully subscribed to the Financial & Economic Essentials datasets from Cybersyn, which are updated daily with global financial data. Notice we didn't have to create databases, tables, views, or an ETL process. We simply searched for and accessed shared data from the Snowflake Data Marketplace.

## 4. Querying, the Results Cache, & Cloning

In the previous exercises, we loaded data into two tables using Snowflake's `COPY` bulk loader command and the `SNOWPARK_HOL_VWH` virtual warehouse. Typically, we would now take on a different role for the analytics users at our company who need to query data in those tables using the worksheet and a second warehouse. This allows each group to work independently without "noisy neighbors" hogging their compute power.

>  **Real World Roles and Querying**
Within a real company, analytics users would likely have a different role than `SNOWPARK_HOL_ROLE`. To keep the lab simple, we are going to stay with the `SNOWPARK_HOL_ROLE` role for this section. Additionally, querying would typically be done with a business intelligence product like Tableau, Looker, PowerBI, etc. For more advanced analytics, data science tools like Datarobot, Dataiku, AWS Sagemaker or many others can query Snowflake. Any technology that leverages JDBC/ODBC, Spark, Python, or any of the other supported programmatic interfaces can run analytics on the data in Snowflake. To keep this lab simple, all queries are being executed via the Snowflake worksheet.

### Execute Some Queries

Go to the **ZERO_TO_SNOWFLAKE_WITH_CYBERSYN** worksheet and be sure to use the HOL warehouse. Your worksheet context should be the following:

**Role:** `SNOWPARK_HOL_ROLE`
**Warehouse:** `SNOWPARK_HOL_VWH`
**Database:** `CYBERSYN_<firstname_lastname>`
**Schema:** `PUBLIC`

![sample data query results](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/6Query_1.png?raw=true)

Run the following query to see a sample of the `company_metadata` data:

```SQL
SELECT * FROM company_metadata;
```

![sample data query results](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/6Query_2.png?raw=true)

Now, let's look at the performance of these companies in the stock market. Run the queries below in the worksheet. 

**Closing Price Statistics:** First, calculate the daily return of a stock (the percent change in the stock price from the close of the previous day to the close of the current day) and 5-day moving average from closing prices (which helps smooth out daily price fluctuations to identify trends).

```SQL
SELECT
    meta.primary_ticker,
    meta.company_name,
    ts.date,
    ts.value AS post_market_close,
    (ts.value / LAG(ts.value, 1) OVER (PARTITION BY meta.primary_ticker ORDER BY ts.date) - 1)::DOUBLE AS daily_return,
    AVG(ts.value) OVER (PARTITION BY meta.primary_ticker ORDER BY ts.date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS five_day_moving_avg_price
FROM Financial__Economic_Essentials.cybersyn.stock_price_timeseries ts
INNER JOIN company_metadata meta
ON ts.ticker = meta.primary_ticker
WHERE ts.variable_name = 'Post-Market Close';
```

![post-market close query results](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/6Query_3.png?raw=true)

>  If you have defined a particular database in the worksheet and want to use a table from a different database, you must fully qualify the reference to the other table by providing its database and schema name.

**Trading Volume Statistics:** Then, calculate the trading volume change from one day to the next to see if there's an increase or decrease in trading activity. This can be a sign of increasing or decreasing interest in a stock.

```SQL
SELECT
    meta.primary_ticker,
    meta.company_name,
    ts.date,
    ts.value AS nasdaq_volume,
    (ts.value / LAG(ts.value, 1) OVER (PARTITION BY meta.primary_ticker ORDER BY ts.date) - 1)::DOUBLE AS volume_change
FROM Financial__Economic_Essentials.cybersyn.stock_price_timeseries ts
INNER JOIN company_metadata meta
ON ts.ticker = meta.primary_ticker
WHERE ts.variable_name = 'Nasdaq Volume';
```

![volume query results](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/6Query_3b.png?raw=true)

### Use the Result Cache

Snowflake has a result cache that holds the results of every query executed in the past 24 hours. These are available across warehouses, so query results returned to one user are available to any other user on the system who executes the same query, provided the underlying data has not changed. Not only do these repeated queries return extremely fast, but they also use no compute credits.

Let's see the result cache in action by running the exact same query again.

```SQL
SELECT
    meta.primary_ticker,
    meta.company_name,
    ts.date,
    ts.value AS post_market_close,
    (ts.value / LAG(ts.value, 1) OVER (PARTITION BY primary_ticker ORDER BY date) - 1)::DOUBLE AS daily_return,
    AVG(ts.value) OVER (PARTITION BY primary_ticker ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS five_day_moving_avg_price
FROM cybersyn.stock_price_timeseries ts
INNER JOIN company_metadata meta
ON ts.ticker = meta.primary_ticker
WHERE variable_name = 'Post-Market Close';
```

In the **Query Details** pane on the right, note that the query runs significantly faster because the results have been cached.
![cached query duration](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/6Query_4.png?raw=true)

### Clone a Table

Snowflake allows you to create clones, also known as "zero-copy clones" of tables, schemas, and databases in seconds. When a clone is created, Snowflake takes a snapshot of data present in the source object and makes it available to the cloned object. The cloned object is writable and independent of the clone source. Therefore, changes made to either the source object or the clone object are not included in the other.

_A popular use case for zero-copy cloning is to clone a production environment for use by Development & Testing teams to test and experiment without adversely impacting the production environment and eliminating the need to set up and manage two separate environments._

>  **Zero-Copy Cloning**
A massive benefit of zero-copy cloning is that the underlying data is not copied. Only the metadata and pointers to the underlying data change. Hence, clones are “zero-copy" and storage requirements are not doubled when the data is cloned. Most data warehouses cannot do this, but for Snowflake it is easy!

Run the following command in the worksheet to create a development (dev) table clone of the `company_metadata` table:

```SQL
CREATE TABLE company_metadata_dev CLONE company_metadata;
```

Click the three dots (**...**) in the left pane and select **Refresh**. Expand the object tree under the `<firstname>_<lastname>_CYBERSYN` database and verify that you see a new table named `company_metadata_dev`. Your Development team now can do whatever they want with this table, including updating or deleting it, without impacting the `company_metadata` table or any other object.

![trips_dev table](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/6Query_6.png?raw=true)

### Joining Tables

We will now join the JSON SEC filing datasets together to investigate the revenue of one CPG company, Kraft Heinz. Run the query below to join `SEC_FILINGS_INDEX` to `SEC_FILINGS_ATTRIBUTES` to see how Kraft Heinz (KHC) business segments have performed over time:

```SQL
WITH data_prep AS (
    SELECT 
        idx.cik,
        idx.company_name,
        idx.adsh,
        idx.form_type,
        att.measure_description,
        CAST(att.value AS DOUBLE) AS value,
        att.period_start_date,
        att.period_end_date,
        att.covered_qtrs,
        TRIM(att.metadata:"ProductOrService"::STRING) AS product
    FROM Financial__Economic_Essentials.cybersyn.sec_filings_attributes_view att
    JOIN sec_filings_index_view idx
        ON idx.cik = att.cik AND idx.adsh = att.adsh
    WHERE idx.cik = '0001637459'
        AND idx.form_type IN ('10-K', '10-Q')
        AND LOWER(att.measure_description) = 'net sales'
        AND (att.metadata IS NULL OR OBJECT_KEYS(att.metadata) = ARRAY_CONSTRUCT('ProductOrService'))
        AND att.covered_qtrs IN (1, 4)
        AND value > 0
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY idx.cik, idx.company_name, att.measure_description, att.period_start_date, att.period_end_date, att.covered_qtrs, product
        ORDER BY idx.filed_date DESC
    ) = 1
)

SELECT
    company_name,
    measure_description,
    product,
    period_end_date,
    CASE
        WHEN covered_qtrs = 1 THEN value
        WHEN covered_qtrs = 4 THEN value - SUM(value) OVER (
            PARTITION BY cik, measure_description, product, YEAR(period_end_date)
            ORDER BY period_end_date
            ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
        )
    END AS quarterly_value
FROM data_prep
ORDER BY product, period_end_date;
```
![weather results](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/7SemiStruct_8_1.png?raw=true)

## 5. Using Time Travel

Snowflake's powerful Time Travel feature enables accessing historical data, as well as the objects storing the data, at any point within a period of time. The default window is 24 hours and, if you are using Snowflake Enterprise Edition, can be increased up to 90 days. Most data warehouses cannot offer this functionality, but - you guessed it - Snowflake makes it easy!

Some useful applications include:

- Restoring data-related objects such as tables, schemas, and databases that may have been deleted.
- Duplicating and backing up data from key points in the past.
- Analyzing data usage and manipulation over specified periods of time.

### Drop and Undrop a Table

First let's see how we can restore data objects that have been accidentally or intentionally deleted.

In the `ZERO_TO_SNOWFLAKE_WITH_CYBERSYN` worksheet, run the following DROP command to remove the `SEC_FILINGS_INDEX` table:

```SQL
DROP TABLE sec_filings_index;

-- Run a query on the table:
SELECT * FROM sec_filings_index LIMIT 10;
```

In the results pane at the bottom, you should see an error because the underlying table has been dropped:
![table dropped error](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/8Time_1.png?raw=true)

Now, restore the table:
```SQL
UNDROP TABLE sec_filings_index;
```

The SEC filing index table should be restored. Verify by running the following query:

```SQL 
SELECT * FROM sec_filings_index LIMIT 10;
```

![restored table result](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/8Time_2.png?raw=true)

### Roll Back a Table

Let's roll back the `COMPANY_METADATA` table in the `<firstname>_<lastname>_CYBERSYN` database to a previous state to fix an unintentional DML error that replaces all the company names in the table with the word "oops".

First, run the following SQL statements to switch your worksheet to the proper context:

```SQL
USE ROLE SNOWPARK_HOL_ROLE;

USE WAREHOUSE SNOWPARK_HOL_VWH;

USE DATABASE <firstname>_<lastname>_CYBERSYN;

USE SCHEMA public;
```

Run the following command to replace all of the company names in the table with the word "oops":

```SQL
UPDATE company_metadata SET company_name = 'oops';
```

Now, run a query that returns the companies. Notice that the company names are all the same:

```SQL
SELECT *
FROM company_metadata;
```

![one row result](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/8Time_3.png?raw=true)

Normally we would need to scramble and hope we have a backup lying around. In Snowflake, we can simply run a command to find the query ID of the last `UPDATE` command and store it in a variable named `$QUERY_ID`.

Use Time Travel to recreate the table with the correct company names and verify the company names have been restored:
```SQL
-- Set the session variable for the query_id
SET query_id = (
  SELECT query_id
  FROM TABLE(information_schema.query_history_by_session(result_limit=>5))
  WHERE query_text LIKE 'UPDATE%'
  ORDER BY start_time DESC
  LIMIT 1
);

-- Use the session variable with the identifier syntax (e.g., $query_id)
CREATE OR REPLACE TABLE company_metadata AS
SELECT *
FROM company_metadata
BEFORE (STATEMENT => $query_id);

-- Verify the company names have been restored
SELECT *
FROM company_metadata;
```

![restored names result](https://github.com/Snowflake-Labs/sfquickstarts/blob/master/site/sfguides/src/getting_started_with_snowflake/assets/8Time_4.png?raw=true)

## Conclusion & Next Steps

Congratulations on completing this introductory lab exercise! You've mastered the Snowflake basics and are ready to apply these fundamentals to your own data. Be sure to reference this guide if you ever need a refresher.

We encourage you to continue with your free trial by loading your own sample or production data and by using some of the more advanced capabilities of Snowflake not covered in this lab.

### Additional Resources:

- Learn more about the [Snowsight](https://docs.snowflake.com/en/user-guide/ui-snowsight.html#using-snowsight) docs.
- Read the [Definitive Guide to Maximizing Your Free Trial](https://www.snowflake.com/test-driving-snowflake-the-definitive-guide-to-maximizing-your-free-trial/) document.
- Attend a [Snowflake virtual or in-person event](https://www.snowflake.com/about/events/) to learn more about our capabilities and customers.
- Join the [Snowflake Community](https://community.snowflake.com/s/topic/0TO0Z000000wmFQWAY/getting-started-with-snowflake).
- Sign up for [Snowflake University](https://community.snowflake.com/s/article/Getting-Access-to-Snowflake-University).
- Contact our [Sales Team](https://www.snowflake.com/free-trial-contact-sales/) to learn more.
- Access Cybersyn's analytics-ready data on [Snowflake Marketplace](https://app.snowflake.com/marketplace/providers/GZTSZAS2KCS/Cybersyn).
- Explore the [60+ public domain sources](https://app.cybersyn.com/data_catalog/?utm_source=Snowflake+Quickstart&utm_medium=organic&utm_campaign=Snowflake+Quickstart) Cybersyn makes available on Snowflake Marketplace.

### What we've covered:

- How to create stages, databases, tables, views, and virtual warehouses.
- How to load structured and semi-structured data.
- How to consume Cybersyn data from the [Snowflake Data Marketplace](https://app.snowflake.com/marketplace/listing/GZTSZAS2KF7/).
- How to perform analytical queries on data in Snowflake, including joins between tables.
- How to clone objects.
- How to undo user errors using Time Travel.
- How to create roles and users, and grant them privileges.
- How to securely and easily share data with other accounts.
