## Udacity - Data Engineering - 6
# Capstone Project

## About / Synopsis

In the data engineering capstone project I combine what I've learned throughout the program. I'm starting with the Udacity provided project of four datasets. The main dataset includes data on immigration to the United States, and supplementary datasets include data on airport codes, US city demographics, and temperature data. The resulting dataset can be used for analysis of trends in travel and immigration to the US.

## Table of Contents
* [Project Scope](#project-scope)
    - [Project Datasets](#project-datasets)
* [Data Exploration](#data-exploration)
* [Data Model](#data-model)
    - [Data Dictionary](#data-dictionary)
    - [Complete Project Write Up](#complete-project-write-up)
* [Files in the Project](#files-in-the-project)
* [Running the Project](#running-the-project)
* [What I Have Learned](#what-i-have-learned)
* [Author](#author)

## Project Scope

I will investigate four datasets being the US immigration data, US city demographics, weather information and airport data. The resulting dataset can be used for analysis of trends in travel and immigration to the US. For example, the dataset can help data analysts in travel and hospitality companies to evaluate seasonal, regional, and demographical factors in travel to the US.

I'm using local Spark instance to process the data in the local folders. The project code can be upgraded to work on a standalone Amazon EMR Spark cluster with data on Amazon S3. I have this experience from [Project 4](https://github.com/adzugaiev/Udacity-Data-Engineering-4) so I focused more on the data investigation rather than building the production level ETL pipeline.

### Project Datasets

* **I94 Immigration Data**: This data comes from the [US National Tourism and Trade Office](https://travel.trade.gov/research/reports/i94/historical/2016.html). A data dictionary is included in the workspace. I'm using this dataset to populate the fact table and dimensions `countries` and `date`
* **U.S. City Demographic Data**: This data comes [from OpenSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/). I'm aggregating it up to the state level to populate dimensions `states` and `state_race_counts`
* **Airport Code Table**: This is a simple table of airport codes and corresponding cities. It comes [from here](https://datahub.io/core/airport-codes#data). I'm using this dataset to populate dimension `airports`
* **World Temperature Data**: This dataset came [from Kaggle](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data). After exploring the state-level data I've decided to skip it as not aligned in time with the immigration fact data.

## Data Exploration

The notebook `explore_data.ipynb` is exploring step-by-step all project datasets, performs data wrangling, transformations and investigates quality issues. Finally, it models the fact and dimension tables and justifies the choice for the data model.

## Data Model

Using the project datasets, I will create a star schema optimized for queries on immigration data analysis. This includes the following tables.

![Schema ERD](../main/schema/schema.png?raw=true)

The fact table, `i94data` is partitioned by year and month. The `date` dimension table is following this partitioning, however since it's grain is one day it can be as well not partitioned.

### Data Dictionary

Data dictionary is in the separate file `data_dictionary.md`

### Complete Project Write Up
* The rationale for the choice of tools and technologies for the project.
    - At project start, I needed to explore four different datasets available in multiple formats and sizes, up to millions of records;
    - I therefore used Apache Spark in combination with Jupyter Notebook to accommodate with multiple data formats, record volumes, and to document my exploration steps and their outcomes;
    - I was missing some visualizations for my findings, so I added reusable data plotting functions based on Seaborn & Matplotlib for some of my repeating Spark queries, in `hepler.py`;
    - I continued using Spark for the ETL pipeline to reuse many of my exploration-stage code, and I tested the ETL steps in another Jupyter Notebook to check their results interactively and tune them where necessary;
    - Finally, Spark helped me to save the resulting dataset into binary parquet files that can be shared and imported to e.g. Amazon Redshift for the dataset users.
* Propose how often the data should be updated and why.
    - The immigration data source is updated every month, therefore our DWH needs to be updated monthly.
* Write a description of how you would approach the problem differently under the following scenarios:
    - _The data was increased by 100x._ Use standalone Spark cluster on AWS EMR, save data to S3, scale the cluster appropriately and watch your AWS bill increase.
    - _The data populates a dashboard that must be updated on a daily basis by 7am every day._ Implement and schedule the refresh jobs with Apache Airflow.
    - _The database needed to be accessed by 100+ people._ Create Amazon Redshift cluster, understand usage patterns and scale the cluster appropriately. Import the output data produced by ETL to Redshift.

## Files in the Project

- `explore_data.ipynb` explores all project datasets, performs data wrangling, transformations and investigates quality issues.
- `etl_test.ipynb` tests all ETL queries and quality checks following the procedure step by step.
- `etl.py` reads and processes files from `i94_data` and `dimension_data` and loads them into DWH tables in `output_data`.
- `helper.py` defines reusable functions, and is imported into the files above.
- `data_dictionary.md` contains dictionary for the data model.
- `README.md` provides the project description you are now reading.

## Running the Project

1) Unzip `dimension_data/WTD-By-State.csv` if you wish to explore the world temperature dataset.
1) You can run `explore_data.ipynb` for step-by-step exploration of all project datasets.
1) You can either run `etl.py` to create and populate the dataset into `output_data`, or
1) You can run `etl_test.ipynb` to test the ETL procedure step by step.

## What I Have Learned

Through the implementation of this project I've learned:

1) Discovering, wrangling, transforming and understanding the unknown datasets with Spark.
1) Making decisions on the data value and data modeling to uncover this value.
1) Built some graphical tools for the dataset exploration.

## Author

Andrii Dzugaiev, [in:dzugaev](https://www.linkedin.com/in/dzugaev/)