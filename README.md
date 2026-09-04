
# API Data Integration Project

## Project Overview

This project implements an API-based data integration pipeline using Python and Databricks. The pipeline extracts weather data from the Open-Meteo API, transforms and validates the data, and loads the processed records into a Delta table.

## Technologies Used

* Python
* REST API
* Open-Meteo API
* Databricks
* Apache Spark / PySpark
* Delta Lake
* SQL

## API Used

**Open-Meteo Weather API**

The pipeline retrieves current weather information for multiple cities, including:

* Visakhapatnam
* Hyderabad
* Chennai
* Bangalore
* Mumbai
* Delhi

## Pipeline Workflow

```text
REST API
   ↓
Data Extraction
   ↓
Data Transformation
   ↓
Data Quality Validation
   ↓
PySpark DataFrame
   ↓
Delta Table
   ↓
Weather Database
```

## Data Collected

The pipeline collects:

* City
* Observation Time
* Temperature
* Relative Humidity
* Wind Speed
* Weather Code
* Timezone

## Data Quality Checks

The pipeline performs validation checks for:

* Duplicate records
* Invalid temperature values
* Invalid humidity values
* Invalid wind speed values

## Pipeline Result

The final pipeline execution successfully:

* Extracted 6 records
* Loaded 6 records
* Completed with `SUCCESS` status
* Found 0 duplicate records
* Found 0 invalid temperature records
* Found 0 invalid humidity records
* Found 0 invalid wind speed records

## Project Files

* `API_Data_Integration_Pipeline.py` — Main pipeline code
* `Task_5_Documentation.pdf` — Project documentation and execution screenshots
* `README.md` — Project description and documentation
