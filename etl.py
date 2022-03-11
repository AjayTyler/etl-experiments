import os, glob, re
import psycopg2 as pg
import pandas as pd
from sql_queries import *

#########################
# FILE HELPER FUNCTIONS #
#########################

def get_files(root_directory, file_extension):
    """Retrieves all files from all subdirectories of the specified
    root directory and returns the result as a list.
    """

    filepath_list = glob.glob(
        f'{root_directory}/**/*{file_extension}',
        recursive = True)

    return filepath_list

def create_supporting_dirs(filepath):
    """Evaluates whether a directory path exists to the destination filepath
    and creates any that are required.
    """
    dir_path = os.path.split(filepath)[0]
    # Checks if directories exists and creates them if not
    if not os.path.exists(dir_path):
        # Converts any potential relative paths to absolute because makedirs
        # can get a little confused by that sort of thing.
        dirs_to_create = os.path.abspath(dir_path)
        os.makedirs(dirs_to_create)

#######################
# DATA PRE-PROCESSING #
#######################

def combine_json_files(json_filepath_list, as_lines = True):
    """Combines JSON files from a list of filepaths and returns the result as
    a Pandas dataframe. By default, assumes JSON lines format.
    """
    df = pd.concat(
            [pd.read_json(f, lines = as_lines) for f in json_filepath_list],
            ignore_index = True)

    return df

def convert_camel_to_snake(txt):
    """Takes a string and returns it after converting any camelCase formatting
    to snake_case.
    """
    snake_case = re.sub(r'([A-Z])', r'_\1', txt).lower()

    # If column name converted from UpperCamelCase, then drop the initial
    # underscore.
    if snake_case[0] == '_':
        return snake_case[1:]
    else:
        return snake_case

def process_song_columns(dataframe):
    """Removes 'artist_' prefix from columns in the given dataframe."""
    column_mapping = {
        'artist_name': 'name',
        'artist_location': 'location',
        'artist_latitude': 'latitude',
        'artist_longitude': 'longitude'}
    return dataframe.rename(columns = column_mapping)

def process_songplay_columns(dataframe):
    """Converts camelCased column names in the given dataframe to snake_case
    and rename 'ts' to 'timestamp'.
    """
    # Rename columns to match database format
    old_columns = dataframe.columns
    snake_case_columns = [convert_camel_to_snake(c) for c in old_columns]
    dataframe = (
        dataframe
        .rename(columns = dict(zip(old_columns, snake_case_columns)))
        .rename(columns = {
            'ts': 'start_time',
            'length': 'duration',
            'song': 'title'}))

    # Creates songplay_id to make future updates possible
    dataframe['songplay_id'] = (
        dataframe['user_id'].astype(str)
        + dataframe['session_id'].astype(str)
        + dataframe['item_in_session'].astype(str))

    # These columns are added to be filled in later
    dataframe['artist_id'] = None
    dataframe['song_id'] = None

    # Removing superfluous double quotes--doesn't add anything
    dataframe['user_agent'] = dataframe['user_agent'].str.replace('"', '')

    return dataframe

def convert_ms_to_timestamp(dataframe, ms_column):
    """Converts a given column in a dataframe from an integer value that
    represents milliseconds to a datetime object.
    """
    dataframe[ms_column] = pd.to_datetime(dataframe[ms_column], unit = 'ms')
    return dataframe

def convert_to_integer(dataframe, column):
    """Converts specified column to an integer datatype."""
    dataframe[column] = dataframe[column].astype(int)
    return dataframe

def process_time_data(dataframe):
    """Creates a calendar table based on a ms timestamp column from songplays
    dataset."""
    # Rename start_time to datetime in case we want to use this calendar table
    # as a generic solution and not just for the songplays data.
    dataframe = (
        dataframe[['start_time']]
        .rename(
            columns={'start_time': 'datetime'})
        .drop_duplicates())

    # Create derivative fields
    dataframe['hour'] = dataframe['datetime'].dt.hour
    dataframe['day'] = dataframe['datetime'].dt.day
    dataframe['week'] = dataframe['datetime'].dt.week
    dataframe['month'] = dataframe['datetime'].dt.month
    dataframe['year'] = dataframe['datetime'].dt.year
    dataframe['weekday'] =  dataframe['datetime'].dt.weekday

    return dataframe

def write_upload_csv(dataframe, columns, filepath, dedup_key = None):
    """Writes dataframe to a CSV file for upload to sparkifydb."""

    # If the required directories do not exist in filepath, they are created.
    create_supporting_dirs(filepath)

    if dedup_key != None:
        dataframe = dataframe[columns].drop_duplicates(dedup_key)
    else:
        dataframe = dataframe[columns]

    dataframe.to_csv(filepath, index = False)

################
# DATABASE I/O #
################

def connect_to_postgres(host, database, user, password, port = '5432'):
    """Returns a psycopg2 connection object with given parameters."""
    conn = pg.connect(
        f"host={host} dbname={database} user={user} password={password} port = {port}")

    return conn

def execute_query(query, connection):
    """Executes SQL query using specified connection."""
    with connection.cursor() as cur:
        cur.execute(query)
    connection.commit()

def copy_csv_to_table(filepath, table_name, connection, sep = ','):
    """Writes CSV file to target table in server."""

    # Converts any potential relative filepath, since an absolute one is
    # required for the COPY command.
    abs_filepath = os.path.abspath(filepath)

    with connection.cursor() as cur:
        with open(filepath, 'r', encoding = 'utf-8') as f:
            cur.copy_expert(f'COPY {table_name} FROM STDIN WITH CSV HEADER', f)

    connection.commit()

    print(f'Wrote {filepath} to {table_name}')

################
# MAIN PROGRAM #
################

def main():
    # Connection info that we'd never use in a production environment.
    connection_info = ('127.0.0.1', 'sparkifydb', 'student', 'student')

    # Gather paths to data files
    song_files = get_files('data/song_data', '.json')
    log_files = get_files('data/log_data', '.json')

    # Read file contents into dataframes, clean column names, and filter
    # the results if applicable.
    raw_song_data = (
        combine_json_files(song_files)
        .pipe(process_song_columns))

    raw_log_data = (
        combine_json_files(log_files)
        .pipe(process_songplay_columns)
        .pipe(convert_ms_to_timestamp, ms_column = 'start_time')
        .query('page == "NextSong"')
        .pipe(convert_to_integer, column = 'user_id'))

    time_data = process_time_data(raw_log_data)

    # Prepare some parameters to loop through CSV generation instead of doing
    # it by hand.
    datasets = {
        'songs_staging': {
            'data_source': 'song_data',
            'columns': ['song_id', 'title', 'artist_id', 'year', 'duration'],
            'dedup': None
        },
        'artists_staging': {
            'data_source': 'song_data',
            'columns': ['artist_id', 'name', 'location', 'latitude', 'longitude'],
            'dedup': 'artist_id'
        },
        'users_staging': {
            'data_source': 'log_data',
            'columns': ['user_id', 'first_name', 'last_name', 'gender', 'level'],
            'dedup': 'user_id'
        },
        'time_staging': {
            'data_source': 'time_data',
            'columns': ['datetime', 'hour', 'day', 'week', 'month', 'year', 'weekday'],
            'dedup': 'datetime'
        },
        'songplays_staging': {
            'data_source': 'log_data',
            'columns': [
                'songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id',
                'session_id', 'location', 'user_agent', 'artist', 'title',
                'duration'
            ],
            'dedup': None
        }
    }

    # Write .csv files for loading into Postgres
    for k in datasets.keys():
        # Snag some parameters for convenience
        dedup = datasets[k]['dedup']
        columns = datasets[k]['columns']

        # Alias the datasource that we want to use
        if datasets[k]['data_source'] == 'log_data':
            src = raw_log_data
        elif datasets[k]['data_source'] == 'song_data':
            src = raw_song_data
        elif datasets[k]['data_source'] == 'time_data':
            src = time_data

        print(f'Writing {k}_data...')
        write_upload_csv(
            src,
            columns,
            f'staging/{k}_data.csv',
            dedup_key = dedup)

    # Create staging tables
    for q in create_staging_tables:
        with connect_to_postgres(*connection_info) as conn:
            execute_query(q, conn)

    # Load CSVs to their respective staging tables.
    for k in datasets.keys():
        print(f'Writing {k} to database...')
        with connect_to_postgres(*connection_info) as conn:
            copy_csv_to_table(
                f'staging/{k}_data.csv',
                f'{k}',
                conn)

    print('Inserting dimension data from staging tables to actual tables...')
    for q in insert_dimensions_from_staging_queries:
        with connect_to_postgres(*connection_info) as conn:
            execute_query(q, conn)

    print('Update artist_id in songplays_staging table...')
    with connect_to_postgres(*connection_info) as conn:
        execute_query(update_artist_id, conn)

    print('Update song_id in songplays_staging table...')
    with connect_to_postgres(*connection_info) as conn:
        execute_query(update_song_id, conn)

    print('Inserting fact data from staging tables to actual tables...')
    for q in insert_facts_from_staging_queries:
        with connect_to_postgres(*connection_info) as conn:
            execute_query(q, conn)

    print('Dropping staging tables...')
    for q in drop_staging_table_queries:
        with connect_to_postgres(*connection_info) as conn:
            execute_query(q, conn)

    print('All done!')

if __name__ == '__main__':
    main()
