## Udacity - Data Engineering - 6
# Capstone Project

This file contains dictionary for the data model. For the full project description, see `README.md`

## Table of Contents

* [Data Model](#data-model)
* [Data Dictionary](#data-dictionary)
    - [Fact: Immigration](#fact-immigration)
    - [Dimension: Date](#dimension-date)
    - [Dimension: Countries](#dimension-countries)
    - [Dimension: States](#dimension-states)
    - [Dimension: State Race Counts](#dimension-state-race-counts)
    - [Dimension: Airports](#dimension-airports)

## Data Model

Using the project datasets, I will create a star schema optimized for queries on immigration data analysis. This includes the following tables.

![Schema ERD](../main/schema/schema.png?raw=true)

The fact table, `i94data` is partitioned by year and month. The `date` dimension table is following this partitioning, however since it's grain is one day it can be as well not partitioned.

## Data Dictionary

Legend first column (K):  
**⚷** primary key  
&#126; distribution key  
● not null

### Fact: Immigration
Table `i94data`
|K|Attribute|Type|Description|
|:---:|:---|:---:|:---|
|**⚷**|i94_id|integer|Original i94 record id|
| |arrive_airline|string|Arrival airline|
| |arrive_by|string|Arrival mode (Air, Land, Sea, ...)|
|●|arrive_date|date|Arrival [date](#dimension-date), ref. by `date`|
| |arrive_flag|string|Arrival flag|
| |arrive_flight|string|Arrival flight number|
|&#126;|arrive_month|integer|Arrival month number|
| |arrive_port|string|Port (incl. [Air](#dimension-airports) of arrival, ref. by `local_code`)|
| |arrive_to_state|string|Destination [state](#dimension-states) upon arrival, ref. by `state_code`|
|&#126;|arrive_year|integer|Arrival year|
| |depart_date|date|Departure [date](#dimension-date), ref. by `date`|
| |depart_flag|string|Departure flag|
| |pers_age|integer|Person's age|
| |pers_birth_year|integer|Person's year of birth|
| |pers_country_birth|integer|Person's [country](#dimension-countries) of birth, ref. by `id`|
| |pers_country_resid|integer|Person's [country](#dimension-countries) of residence, ref. by `id`|
| |pers_gender|string|Person's gender (F/M)|
| |visa|string|Visa type|
| |visa_issued|string|Visa issuing authority|
| |visa_type|string|Visa admission class|
| |admission_number|double|Admission number|
| |allow_stay_until|string|Date until stay in the U.S. is allowed|
| |match_flag|string|Whether the arrival & departure events are matching|

### Dimension: Date
Table `date`
|K|Attribute|Type|Description|
|:---:|:---|:---:|:---|
|**⚷**|date|date|Date, with no time|
|&#126;|year|integer|Year from date|
|&#126;|month|integer|Month number from date|
|●|week|integer|Week number in year|
|●|day|integer|Day from date|
|●|weekday|integer|Weekday number, 1 is Sunday|

### Dimension: Countries
Table `countries`
|K|Attribute|Type|Description|
|:---:|:---|:---:|:---|
|**⚷**|id|integer|Original i94 country id|
|●|name|string|Country name|

### Dimension: States
Table `states`
|K|Attribute|Type|Description|
|:---:|:---|:---:|:---|
|**⚷**|state_code|integer|Original state code|
|●|state|string|State name|
|●|pop_male|ineger|Quantity of male population|
|●|pop_female|ineger|Quantity of female population|
|●|pop_total|ineger|Total quantity of population|
|●|pop_veteran|ineger|Quantity of veteran population|
|●|pop_foreign_born|ineger|Quantity of foreign born population|

### Dimension: State Race Counts
Table `state-race-counts`
|K|Attribute|Type|Description|
|:---:|:---|:---:|:---|
|**⚷**|state_code|integer|Original state code, ref. [state](#dimension-states)|
|●|state|string|State name|
|**⚷**|race|string|Race name|
|●|race_count|string|Quantity of race population in state|

### Dimension: Airports
Table `airports`
|K|Attribute|Type|Description|
|:---:|:---|:---:|:---|
|**⚷**|id|integer|Original airport id|
|●|name|string|Airport name|
| |type|string|Airport type|
| |elevation_ft|integer|Elevation above the sea level, feet|
| |city|string|Closest city|
| |gps_code|string|GPS code|
| |local_code|string|Local code, ref. [immigration](#fact-immigration) by `arrive_port`|
| |state_code|string|State code, ref. [state](#dimension-states)|
| |lat|float|Latitude|
| |long|float|Longitude|
