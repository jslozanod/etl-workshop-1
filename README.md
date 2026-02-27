ETL Workshop — Data Warehouse Project
1. Project Objective

The objective of this project was to design and implement a complete ETL process and a Data Warehouse using a Star Schema model.

Starting from a raw CSV file (candidates.csv), I transformed the data into a structured analytical model in PostgreSQL. The goal was not only to store the data, but to organize it properly so that business KPIs could be calculated directly from the Data Warehouse and not from the raw file.

This project helped me understand how raw operational data can be transformed into a clean analytical model ready for reporting.

2. Star Schema Design

For the dimensional model, I designed a Star Schema composed of one fact table and multiple dimension tables.

Fact Table

fact_application

This table contains the measurable information about each candidate application, such as:

Code challenge score

Technical interview score

Hiring decision

Foreign keys to each dimension

Dimension Tables

dim_candidate

dim_date

dim_country

dim_seniority

dim_technology

Each dimension has its own surrogate key (generated with SERIAL).
I did not use natural keys from the CSV file as primary keys in the Data Warehouse, because in dimensional modeling it is better to use surrogate keys to maintain consistency and flexibility.

3. Grain Definition

The grain of the fact table is:

One row per candidate application.

This means that each row in fact_application represents one single application event.

Each record includes:

One candidate

One technology

One country

One seniority level

One application date

The evaluation scores

The hiring result

Defining the grain clearly was important to avoid double counting and to make sure that aggregations like “hires by technology” or “hires by year” work correctly.

4. ETL Logic

The ETL process was implemented in Python and divided into three stages: Extract, Transform and Load.

Extract

In this step, I:

Loaded the CSV file.

Checked the data types.

Explored the dataset using a Jupyter notebook.

Identified possible null values or inconsistencies.

Transform

During the transformation phase, I:

Applied the HIRED rule to create the is_hired column.

Checked that scores were within the expected range (0–10).

Handled null values when necessary.

Created all dimension tables.

Generated surrogate keys for each dimension.

Built the fact table aligned with the defined grain.

The fact table was built only after all dimensions were ready, so that foreign key relationships could be created correctly.

Load

In the load phase, I:

Inserted the dimension tables into PostgreSQL.

Inserted the fact table.

Created foreign key relationships.

Verified row counts after loading.

Confirmed referential integrity between fact and dimension tables.

All KPI queries were executed directly from the Data Warehouse.

5. Data Quality Assumptions

During the transformation process, I made the following assumptions:

Each row in the CSV represents one candidate application.

Scores must be between 0 and 10.

There are no duplicate applications.

The hiring decision follows the defined evaluation rule.

Technology, country and seniority are valid categorical values.

Data Quality Validations Performed

Checked for null values in important columns.

Verified that score values are within the correct range.

Ensured that all foreign keys match valid dimension records.

Confirmed that there were no missing relationships before loading.

These validations were necessary to make sure the Data Warehouse is reliable for analysis.

6. KPIs and Visualizations

All KPIs were calculated using SQL queries executed directly against the Data Warehouse.

The required KPIs implemented were:

Hires by Technology

Hires by Year

Hires by Seniority

Hires by Country over Years (USA, Brazil, Colombia, Ecuador)

Additionally, I implemented two extra KPIs:

Hire Rate (%)

Average Scores of Hired Candidates

The visualizations were generated using Python and exported as images.

Example results obtained:

Hire Rate: 13.40%

Average Code Score (Hired): 8.50

Average Interview Score (Hired): 8.48

All charts are available in the visualizations/ folder.