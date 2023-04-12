# Sparkify ETL Pipeline

This project is an ETL pipeline for Sparkify, a music streaming startup. The pipeline processes data from JSON files stored in S3 buckets, and extracts data to create dimensional tables in parquet format using PySpark. 

## Getting Started

### Prerequisites
- PySpark 2.4.3 or later
- AWS S3 access

### Installing
Clone the project repository from GitHub to your local machine.

### Usage
1. Configure `dl.cfg` with your AWS credentials.
2. Launch a Spark session using `python etl.py`.
3. The ETL pipeline will extract data from S3 buckets, transform the data into tables, and load them back into S3 buckets in parquet format.

## Files
- `etl.py`: This script processes the JSON data from S3 buckets, and extracts the data into dimensional tables in parquet format.
- `dl.cfg`: This configuration file stores the AWS credentials required for S3 access.

## Dimensional Tables
- `songs_table`: Contains information about each song, including song ID, title, artist ID, year, and duration.
- `artists_table`: Contains information about each artist, including artist ID, name, location, latitude, and longitude.
- `users_table`: Contains information about each user, including user ID, first name, last name, gender, and level.
- `time_table`: Contains information about each timestamp, including start time, hour, day, week, month, year, and weekday.
- `songplays_table`: Contains information about each song play, including songplay ID, start time, user ID, level, song ID, artist ID, session ID, location, and user agent.

