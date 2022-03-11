# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

songplay_staging_table_drop = "DROP TABLE IF EXISTS songplays_staging"
time_staging_table_drop = "DROP TABLE IF EXISTS time_staging"
user_staging_table_drop = "DROP TABLE IF EXISTS users_staging"
song_staging_table_drop = "DROP TABLE IF EXISTS songs_staging"
artist_staging_table_drop = "DROP TABLE IF EXISTS artists_staging"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id text PRIMARY KEY,
        start_time timestamp NOT NULL,
        user_id int NOT NULL,
        level text,
        song_id text,
        artist_id text,
        session_id int,
        location text,
        user_agent text
    )
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id int PRIMARY KEY,
        first_name text,
        last_name text,
        gender text,
        level text
    )
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id text PRIMARY KEY,
        title text NOT NULL,
        artist_id text NOT NULL,
        year int,
        duration float NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id text PRIMARY KEY,
        name text NOT NULL,
        location text,
        latitude double precision,
        longitude double precision
    )
""")

time_table_create = ("""
    CREATE TABLE time (
        datetime timestamp PRIMARY KEY,
        hour int NOT NULL,
        day int NOT NULL,
        week int NOT NULL,
        month int NOT NULL,
        year int NOT NULL,
        weekday int NOT NULL
    )
""")

# CREATE STAGING TABLES

songplay_staging_table_create = ("""
    CREATE TABLE songplays_staging (
        songplay_id text,
        start_time timestamp,
        user_id int,
        level text,
        song_id text,
        artist_id text,
        session_id int,
        location text,
        user_agent text,
        artist text,
        title text,
        duration float
    )
""")

songs_staging_table_create = ("""
    CREATE TABLE songs_staging (
        song_id text,
        title text,
        artist_id text,
        year int,
        duration float
    )
""")

artists_staging_table_create = ("""
    CREATE TABLE artists_staging (
        artist_id text,
        name text,
        location text,
        latitude double precision,
        longitude double precision
    )
""")

users_staging_table_create = ("""
    CREATE TABLE users_staging (
        user_id int,
        first_name text,
        last_name text,
        gender text,
        level text
    )
""")

time_staging_table_create = ("""
    CREATE TABLE time_staging (
        datetime timestamp,
        hour int NOT NULL,
        day int NOT NULL,
        week int NOT NULL,
        month int NOT NULL,
        year int NOT NULL,
        weekday int NOT NULL
    )
""")

# INSERT SINGLE RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (
        songplay_id,
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (songplay_id) DO UPDATE
        SET artist_id = excluded.artist_id,
            song_id = excluded.song_id
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE
        SET first_name = excluded.first_name,
            last_name = excluded.last_name,
            gender = excluded.gender,
            level = excluded.level
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO UPDATE
        SET title = excluded.title,
            artist_id = excluded.artist_id,
            year = excluded.year,
            duration = excluded.duration
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO UPDATE
        SET name = excluded.name,
            location = excluded.location,
            latitude = excluded.latitude,
            longitude = excluded.longitude
""")

time_table_insert = ("""
    INSERT INTO time (
        datetime,
        hour,
        day,
        week,
        month,
        year,
        weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (datetime) DO NOTHING
""")

# INSERT FROM STAGING TABLES

insert_from_songplay_staging = ("""
    INSERT INTO songplays
    SELECT
        songplay_id,
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    FROM songplays_staging
    ON CONFLICT (songplay_id) DO UPDATE
        SET artist_id = excluded.artist_id,
            song_id = excluded.song_id
""")

insert_from_song_staging = ("""
    INSERT INTO songs
    SELECT * FROM songs_staging
    ON CONFLICT (song_id) DO UPDATE
        SET title = excluded.title,
            artist_id = excluded.artist_id,
            year = excluded.year,
            duration = excluded.duration
""")

insert_from_artist_staging = ("""
    INSERT INTO artists
    SELECT * FROM artists_staging
    ON CONFLICT (artist_id) DO UPDATE
        SET name = excluded.name,
            location = excluded.location,
            latitude = excluded.latitude,
            longitude = excluded.longitude
""")

insert_from_user_staging = ("""
    INSERT INTO users
    SELECT * FROM users_staging
    ON CONFLICT (user_id) DO UPDATE
        SET first_name = excluded.first_name,
            last_name = excluded.last_name,
            gender = excluded.gender,
            level = excluded.level
""")

insert_from_time_staging = ("""
    INSERT INTO time
    SELECT * FROM time_staging
    ON CONFLICT (datetime) DO NOTHING
""")

# FIND SONGS

song_select = ("""
    SELECT
        a.artist_id,
        s.song_id
    FROM songs as s
    LEFT JOIN artists as a
        on s.artist_id = a.artist_id
    WHERE s.title = %s
      AND a.name = %s
      AND s.duration = %s
""")

update_artist_id = ("""
    UPDATE songplays_staging as sp
    SET artist_id = a.artist_id
    FROM artists as a
    WHERE sp.artist = a.name
""")

update_song_id = ("""
    UPDATE songplays_staging as sp
    SET song_id = s.song_id
    FROM songs as s
    WHERE sp.title = s.title
    AND sp.duration = s.duration
""")

# QUERY LISTS

drop_staging_table_queries = [
    songplay_staging_table_drop, song_staging_table_drop,
    artist_staging_table_drop, user_staging_table_drop,
    time_staging_table_drop]

drop_table_queries = [
    songplay_table_drop, user_table_drop, song_table_drop,
    artist_table_drop, time_table_drop, *drop_staging_table_queries]

create_table_queries = [
    songplay_table_create, user_table_create,
    song_table_create, artist_table_create, time_table_create]

create_staging_tables = [
    songplay_staging_table_create, time_staging_table_create,
    artists_staging_table_create, users_staging_table_create,
    songs_staging_table_create]

insert_dimensions_from_staging_queries = [
    insert_from_song_staging, insert_from_time_staging,
    insert_from_user_staging, insert_from_artist_staging]

insert_facts_from_staging_queries = [insert_from_songplay_staging]
