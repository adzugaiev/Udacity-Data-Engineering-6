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
* [Files in the Project](#files-in-the-project)
* [Running the Project](#running-the-project)
* [What I Have Learned](#what-i-have-learned)
* [Author](#author)

## Project Scope

I will investigate four datasets being the US immigration data, US city demographics, weather information and airport data. The resulting dataset can be used for analysis of trends in travel and immigration to the US. I'm using local Spark instance to process the data in the local folders. The project code can be upgraded to work on a standalone Amazon EMR Spark cluster with data on Amazon S3. I have this experience from [Project 4](https://github.com/adzugaiev/Udacity-Data-Engineering-4) so I focused more on the data investigation rather than building the production level ETL pipeline.

### Project Datasets

* **I94 Immigration Data**: This data comes from the [US National Tourism and Trade Office](https://travel.trade.gov/research/reports/i94/historical/2016.html). A data dictionary is included in the workspace. I'm using this dataset to populate the fact table and dimensions `countries` and `date`.
* **U.S. City Demographic Data**: This data comes [from OpenSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/). I'm aggregating it up to the state level to populate dimensions `states` and `state_race_counts`.
* **Airport Code Table**: This is a simple table of airport codes and corresponding cities. It comes [from here](https://datahub.io/core/airport-codes#data). I'm using this dataset to populate dimension `airports`.
* **World Temperature Data**: This dataset came [from Kaggle](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data). After exploring the state-level data I've decided to skip it as not aligned in time with the immigration fact data.

## Data Exploration

The notebook `explore_data.ipynb` is exploring step-by-step all project datasets, performs data wrangling, transformations and investigates quality issues. Finally, it models the fact and dimension tables and justifies the choice for the data model.

## Data Model

Using the project datasets, I will create a star schema optimized for queries on immigration data analysis. This includes the following tables.

![Schema ERD](../main/schema/schema.png?raw=true)

The fact table, `i94data` is partitioned by year and month. The `date` dimension table is following this partitioning, however since it's grain is one day it can be as well not partitioned.

### Data Dictionary

Data dictionary is in the separate file `schema/data_dictionary.md`.

## Files in the Project

- `explore_data.ipynb` explores all project datasets, performs data wrangling, transformations and investigates quality issues.
- `etl_test.ipynb` tests all ETL queries following the procedure step by step.
- `etl.py` reads and processes files from `i94_data` and `dimension_data` and loads them into DWH tables in `output_data`.
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