# Databricks notebook source
import requests

print("Requests library imported successfully")

# COMMAND ----------

def fetch_weather_data(city, latitude, longitude):
    
    API_URL = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code",
        "timezone": "Asia/Kolkata"
    }

    try:
        response = requests.get(
            API_URL,
            params=params,
            timeout=30
        )

        response.raise_for_status()

        data = response.json()
        current = data["current"]

        weather_record = {
            "city": city,
            "country": "India",
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "observation_time": current["time"],
            "temperature_c": current["temperature_2m"],
            "humidity_percent": current["relative_humidity_2m"],
            "wind_speed_kmh": current["wind_speed_10m"],
            "weather_code": current["weather_code"]
        }

        return weather_record

    except requests.exceptions.RequestException as e:
        print(f"API request failed for {city}: {e}")
        return None

    except Exception as e:
        print(f"Processing failed for {city}: {e}")
        return None

# COMMAND ----------

cities = [
    {
        "city": "Visakhapatnam",
        "latitude": 17.68014,
        "longitude": 83.204254
    },
    {
        "city": "Hyderabad",
        "latitude": 17.3850,
        "longitude": 78.4867
    },
    {
        "city": "Chennai",
        "latitude": 13.0827,
        "longitude": 80.2707
    },
    {
        "city": "Bangalore",
        "latitude": 12.9716,
        "longitude": 77.5946
    },
    {
        "city": "Mumbai",
        "latitude": 19.0760,
        "longitude": 72.8777
    },
    {
        "city": "Delhi",
        "latitude": 28.6139,
        "longitude": 77.2090
    }
]

print("Total cities:", len(cities))

# COMMAND ----------

weather_records = []

for city_info in cities:

    record = fetch_weather_data(
        city_info["city"],
        city_info["latitude"],
        city_info["longitude"]
    )

    if record is not None:
        weather_records.append(record)

print("Total cities:", len(cities))
print("Total records fetched:", len(weather_records))

if len(weather_records) == 0:
    raise ValueError("No weather records were fetched from API")

# COMMAND ----------

import pandas as pd
from datetime import datetime

records_df = pd.DataFrame(weather_records)

# Add pipeline load timestamp
records_df["load_timestamp"] = datetime.now()

# Convert observation time
records_df["observation_time"] = pd.to_datetime(
    records_df["observation_time"]
)

# Create observation date
records_df["observation_date"] = (
    records_df["observation_time"].dt.date
)

# Create readable weather category
records_df["weather_category"] = records_df["weather_code"].apply(
    lambda x:
        "Clear Sky" if x == 0 else
        "Cloudy" if x in [1, 2, 3] else
        "Fog" if x in [45, 48] else
        "Drizzle" if x in [51, 53, 55, 56, 57] else
        "Rain" if x in [61, 63, 65, 66, 67] else
        "Snow" if x in [71, 73, 75, 77] else
        "Rain Showers" if x in [80, 81, 82] else
        "Snow Showers" if x in [85, 86] else
        "Thunderstorm" if x in [95, 96, 99] else
        "Unknown"
)

print("Pandas transformation successful")

# COMMAND ----------

pipeline_spark_df = spark.createDataFrame(records_df)

print("Spark DataFrame conversion successful")

# COMMAND ----------

from pyspark.sql.functions import lit

pipeline_spark_df = pipeline_spark_df.withColumn(
    "source",
    lit("Open-Meteo")
)

# COMMAND ----------

from pyspark.sql.functions import col

quality_summary = {
    "total_records": pipeline_spark_df.count(),

    "null_city": pipeline_spark_df.filter(
        col("city").isNull()
    ).count(),

    "null_temperature": pipeline_spark_df.filter(
        col("temperature_c").isNull()
    ).count(),

    "null_humidity": pipeline_spark_df.filter(
        col("humidity_percent").isNull()
    ).count(),

    "null_observation_time": pipeline_spark_df.filter(
        col("observation_time").isNull()
    ).count(),

    "invalid_temperature": pipeline_spark_df.filter(
        (col("temperature_c") < -80) |
        (col("temperature_c") > 60)
    ).count(),

    "invalid_humidity": pipeline_spark_df.filter(
        (col("humidity_percent") < 0) |
        (col("humidity_percent") > 100)
    ).count(),

    "invalid_wind_speed": pipeline_spark_df.filter(
        col("wind_speed_kmh") < 0
    ).count()
}

for check, result in quality_summary.items():
    print(f"{check}: {result}")

# COMMAND ----------

duplicate_records = (
    pipeline_spark_df
    .groupBy("city", "observation_time")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate records:", duplicate_records)

# COMMAND ----------

from datetime import datetime

pipeline_log = {
    "pipeline_name": "weather_api_pipeline",
    "run_timestamp": datetime.now(),
    "records_extracted": len(weather_records),
    "records_loaded": 6,
    "status": "SUCCESS"
}

print(pipeline_log)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS weather_db;

# COMMAND ----------

pipeline_spark_df.createOrReplaceTempView("weather_pipeline_batch")

print("Temporary weather view created successfully")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO weather_db.weather_data AS target
# MAGIC USING weather_pipeline_batch AS src
# MAGIC ON target.city = src.city
# MAGIC AND target.observation_time = src.observation_time
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.country = src.country,
# MAGIC     target.latitude = src.latitude,
# MAGIC     target.longitude = src.longitude,
# MAGIC     target.temperature_c = src.temperature_c,
# MAGIC     target.humidity_percent = src.humidity_percent,
# MAGIC     target.wind_speed_kmh = src.wind_speed_kmh,
# MAGIC     target.weather_code = src.weather_code,
# MAGIC     target.weather_category = src.weather_category,
# MAGIC     target.`source` = src.`source`,
# MAGIC     target.load_timestamp = src.load_timestamp,
# MAGIC     target.observation_date = src.observation_date
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     city,
# MAGIC     country,
# MAGIC     latitude,
# MAGIC     longitude,
# MAGIC     observation_time,
# MAGIC     temperature_c,
# MAGIC     humidity_percent,
# MAGIC     wind_speed_kmh,
# MAGIC     weather_code,
# MAGIC     weather_category,
# MAGIC     `source`,
# MAGIC     load_timestamp,
# MAGIC     observation_date
# MAGIC )
# MAGIC VALUES (
# MAGIC     src.city,
# MAGIC     src.country,
# MAGIC     src.latitude,
# MAGIC     src.longitude,
# MAGIC     src.observation_time,
# MAGIC     src.temperature_c,
# MAGIC     src.humidity_percent,
# MAGIC     src.wind_speed_kmh,
# MAGIC     src.weather_code,
# MAGIC     src.weather_category,
# MAGIC     src.`source`,
# MAGIC     src.load_timestamp,
# MAGIC     src.observation_date
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM weather_db.weather_data;

# COMMAND ----------

records_loaded = pipeline_spark_df.count()

print("Records loaded:", records_loaded)

# COMMAND ----------

from datetime import datetime

pipeline_log = {
    "pipeline_name": "weather_api_pipeline",
    "run_timestamp": datetime.now(),
    "records_extracted": len(weather_records),
    "records_loaded": records_loaded,
    "status": "SUCCESS"
}

log_df = spark.createDataFrame([pipeline_log])

display(log_df)

# COMMAND ----------

log_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("weather_db.pipeline_logs")

print("Pipeline log saved successfully")