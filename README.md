# AWS Data Warehouse Project


## Project Overview

The purpose of this project is to build an ETL pipeline for a music streaming application. The code in this project will take two sources of application data from Amazon S3 buckets, song information and song streaming information. Using python, the source data is extracted from Amazon S3 buckets and transformed and loaded into Amazon Redshift fact and dimension tables, where it is then ready for easy querying and analytics.

## Source Data and Data Warehouse Design

### Source Data

#### Song Data

- Song data is stored in JSON format and contains metadata about a song and the artist of that song.

- S3 Bucket File Path: s3://udacity-dend/song_data

- Sample of Song Data & data types:

```
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR(MAX),
    title VARCHAR,
    duration REAL,
    year INT
```

#### Log/Event Data

- Log data is stored in JSON format and contains information pertaining to user activity within the music application, such as song plays.

- S3 Bucket File Path: s3://udacity-dend/log_data

- Sample of Log Data & data types:

```
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INT SORTKEY,
    song VARCHAR,
    status INT,
    ts BIGINT,
    userAgent VARCHAR,
    userId INT
```

### Data Warehouse Design

#### Fact Tables

Song Plays - contains information about song play activity within the music application
```
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP,
    user_id INT,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR
```

#### Dimension Tables

Users - contains information about users of the music streaming application
```
    user_id VARCHAR PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender CHAR(1),
    level VARCHAR
```

Songs - contains information about songs in the music streaming application
```
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INT NOT NULL,
    duration DECIMAL NOT NULL
```

Artists - contains information about the artists of the songs
```
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude DECIMAL,
    longitude DECIMAL
```

Time - contains unique timestamp data from the log data converted into date parts for easy analytics
```
    start_time TIMESTAMP PRIMARY KEY,
    hour NUMERIC NOT NULL,
    day NUMERIC NOT NULL,
    week NUMERIC NOT NULL,
    month NUMERIC NOT NULL,
    year NUMERIC NOT NULL,
    weekday NUMERIC NOT NULL
```


## File & Script Information

### cluster_setup.cfg
- Contains necessary parameters to create a Redshift cluster and IAM roles in cluster_setup_and_delete.py

### cluster_setup_and_delete.py

- Creates Redshift cluster and IAM roles using an infrastructure as code (IAC) approach. Redshift clusters are created using the data warehouse parameters defined in cluster_setup.cfg
- Cluster created in this script will be used for future transformation and storage of the music streaming application data from S3 buckets 
- Contains functions for collapse and deletion of the AWS infrastructure
- Writes newly created cluster endpoint and role information to dwh.cfg

### dwh.cfg
- Contains necessary information about previously created redshift clusters as well as S3 file paths to application source data
- Used in following scripts to connect to the newly created data warehouse

### sql_queries.py

- Establishes queries for the following actions:
    - Creation of staging tables to store application data from S3 Buckets
    - Creation of data warehouse fact and dimension tables
    - Insertion of staging data into data warehouse tables


### create_tables.py

- Creates tables in data warehouse using queries from sql_queries.py

### etl.py

- Copies data from S3 buckets into staging tables and inserts data into fact and dimension tables

### test_tables.ipynb

- Jupyter Notebook showing sample queries on new data warehouse using combination of fact and dimension tables




