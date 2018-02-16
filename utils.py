import tarfile
import shutil
import gzip
import pandas as pd


# Unzip Tar file
def unzip_file(path_to_zip_file):
    # Unzip lookup directory
    if(path_to_zip_file.endswith('-lookup_data.tar.gz')):
        tar = tarfile.open(path_to_zip_file)
        tar.extractall(path="lookup_tables/")
        tar.close()
    # Unzip data files
    else:
        with gzip.open(path_to_zip_file, 'rb') as f_in:
            output_path = path_to_zip_file.replace('.gz','')
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)


# Import file to DataFrame and apply Column Headers
def file_to_frame(file_name):
    df = pd.read_csv(file_name, sep='\t', index_col=None)

    # Import column headers from lookup table
    col_headers = pd.read_csv('lookup_tables/column_headers.tsv', sep='\t')

    # Apply column headers to DataFrame
    df.columns = col_headers.columns

    print("File imported successfully with {} rows and {} columns.".format(df.shape[0], df.shape[1]))
    return df


# Drop Preprocessing eVar and Prop Columns
def drop_pre_processing_columns(df):
    initial_cols = df.shape[1]

    # Drop eVar Columns
    df = df[df.columns.drop(list(df.filter(regex='^evar')))]
    evars_dropped = initial_cols - df.shape[1]
    print("Removed {} evar columns.".format(evars_dropped))

    # Drop Prop Columns
    df = df[df.columns.drop(list(df.filter(regex='^prop')))]
    props_dropped = initial_cols - evars_dropped - df.shape[1]
    print("Removed {} prop columns.".format(props_dropped))

    return df


def map_lookup_file(df, file_name, column_name):
    # Import lookup table
    # 'referrer_type.tsv' is handled differently because it contains 3 columns
    if (file_name == 'referrer_type.tsv'):
        lookup_df = pd.read_csv('lookup_tables/referrer_type.tsv', sep='\t', index_col=None,
                                names=['short', 'drop', 'long'])
        lookup_df = lookup_df.drop('drop', axis=1)
    else:
        lookup_df = pd.read_csv('lookup_tables/' + file_name, sep='\t', index_col=None, names=['short', 'long'])

    # Convert lookup to dictionary and replace values
    lookup_dict = lookup_df.set_index('short').T.to_dict('records')[0]
    df[column_name] = df[column_name].map(lambda x: lookup_dict.get(x, x))

    print("Mapped values for column '{}'.".format(column_name))
    return df


# Create 'visid_type_map' column to map 'visid_type' to a value for convenience
def create_visitor_id_map(df):
    lookup_dict = {
        0: 'Custom Visitor ID',
        1: 'IP & UA Fallback',
        2: 'Wireless',
        3: 'Adobe',
        4: 'Fallback Cookie',
        5: 'Visitor ID Service'
    }
    df['visid_type_map'] = df['visid_type'].replace(lookup_dict)

    print("Created visid_type_map column.")
    return df


# Create Session ID
def create_session_id(df):
    def getSessionID(row):
        if(row['exclude_hit'] > 0 or row['hit_source'] == 5 or row['hit_source'] == 7 or row['hit_source'] == 8 or row['hit_source'] == 9):
            val = '(not set)'
        else:
            val = str(row['post_visid_high']) + str(row['post_visid_low']) + str(row['visit_num']) + str(row['visit_start_time_gmt'])
        return val

    df['Session_ID'] = df.apply(getSessionID, axis=1)
    return df


# Create User ID
def create_user_id(df):
    def getUserID(row):
        if(row['exclude_hit'] > 0 or row['hit_source'] == 5 or row['hit_source'] == 7 or row['hit_source'] == 8 or row['hit_source'] == 9):
            val = '(not set)'
        else:
            val = str(row['post_visid_high']) + str(row['post_visid_low'])
        return val

    df['User_ID'] = df.apply(getUserID, axis=1)
    return df


def print_validation(df):
    print("Values calculated for this data set:")

    # Unique Visitors
    user_count = df['User_ID'].nunique()
    print("Unique Visitors = {}".format(user_count))

    # Visits
    session_count = df['Session_ID'].nunique()
    print("Visits = {}".format(session_count))


def export_final_file(df, report_suite, date, project_id, private_key, chunksize=10000):
    # Date should be YYYYMMDD for BigQuery

    df.to_gbq(
        'adobe.%s' % report_suite + '_' + date,
        project_id,
        chunksize=chunksize,
        verbose=False,
        reauth=False,
        if_exists='append',
        private_key=private_key
    )
