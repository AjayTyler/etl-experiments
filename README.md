# README

This project creates a database to facilitate analytics for Sparkify as regards songplay information. Marketing personnel, for instance, might glean insights from the listening patterns of users at different levels (identifying high-engagement users that might respond well to discounts on a membership upsell). Artists and other stakeholders could benefit from time series analytics to see whether their own marketing efforts resulted in attracting a larger audience.

Exploratory analytics could also produce market insights that could be transformed easily into whitepapers, webinar presentations, and other marketing collateral to help build and maintain relationships with a variety of interested parties. For example, examining the location of artists and songplays can help identify artists that have achieved local popularity, those with a broad reach, and those that manage to cross from one such cohort to another over time.

This document expounds on the contents of this repository and outlines its use.

## Running the Scripts

When beginning this process for the first time, start by running the `create_tables.py` script from the command line (PowerShell, bash, or whatever your preference may be). This will create the tables required for the database, dropping any previous version that exists. So, make sure that there's no old info that you want to keep before you kick off the script.

If you get an error similar to `connection to server at "127.0.0.1", port 5432 failed: FATAL:  password authentication failed for user "student"`, check to see whether you have multiple instances of Postgres installed. If so, you may need to adjust some of the connection settings. In my case, I needed to add `port=5433` to the connection strings on line 12 and line 24 in `create_tables.py` to make it work, since port 5432 (the default port for Postgres) was in use by another installation. Your case may differ.

```bash

python create_tables.py

```

Next, run the `etl.py` script. This will kick off the ETL process that ingests data and uploads it to the database. Likewise, if you get an error, you may need to add the port to parameters for the connection (line 183 in `etl.py`).

```bash

python etl.py

```

And that's it! When you have more data, simply add it to the appropriate folder (`data/song_data` or `data/log_data`) and run `etl.py` again.

## Files in This Repository

- `data/` contains the data used to design this ETL process.
- `create_tables.py` will, as the name suggests, create the database tables required for this ETL process.
- `etl.ipynb` is a Jupyter notebook that walks the user through the basics of figuring out the ETL process.
- `README.md` is this document that you are reading right now.
- `sql_queries.py` contains all the SQL queries, statements, and commands required to prepare and perform this ETL process.
- `test.ipynb` is the 'Sanity Check' Jupyter notebook that does some preliminary checks of the user's work to help sidestep common issues.

## Database Schema

### Facts

The needs for this particular process are serviced by one fact table: `songplays`. It is derived from application log data and includes only activity affiliated with 'songplays' (i.e. starting or progressing to the next song). In technical terms, this means data where `page == 'NextSong'`.

It includes the following columns:

- songplay_id (primary key)
- start_time
- user_id
- level
- song_id
- artist_id
- session_id
- location
- user_agent

One item of note concerns the `songplay_id` field. The value is derived from a combination of fields in the raw log data: `userId`, `sessionId`, and `itemInSession` concatenated with hyphens. This produces a unique ID that makes it possible to enrich or maintain songplay data in the future, should the need arise.

### Dimensions

Four dimension tables are used in conjunction with `songplays` to provide some useful options for analysis:

- `artists`
- `songs`
- `time`
- `users`

#### Artists

The `artists` dimension table includes the following columns:

- artist_id (primary key)
- name
- location
- latitude
- longitude

#### Songs

The `songs` dimension table includes the following columns:

- song_id (primary key)
- title
- artist_id
- year
- duration

#### Time

The `time` dimension table includes the following columns:

- datetime (primary key)
- hour
- day
- month
- year
- weekday

#### Users

The `users` dimension table includes the following columns:

- user_id (primary key)
- first_name
- last_name
- gender
- level

## ETL Pipeline

The script assumes that a folder named "data" is in the same directory as the script, and that it contains subfolders named "log_data" and "song_data" that each have JSON files located within them in various subdirectories.

```
data
|__ log_data
|   |__ ..
|       |__ *.json
|
|__ song_data
    |__ ..
        |__ *.json
```

The ETL script will read in the data from each source, apply any required transformations, and then save some CSV files to `/staging`. These will be used to copy the data into their respective staging tables in the database. From there, they will be inserted to the appropriate fact and dimension tables.

This allows for an efficient upload of large datasets instead of writing a loop to insert the values iteratively. Additionally, some conditions have been added to each table so that conflicts on primary key columns result in updates instead of duplicates or errors (with the exception of the `time` table, in which case duplicate datetimes are ignored).

In the case of the `songplays` table, updates are focused on the `artist_id` and `song_id` fields as these seemed to be what would be the highest point of failure that would require enrichment after upload.
