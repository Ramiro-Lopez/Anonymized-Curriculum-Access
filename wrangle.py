import pandas as pd
import numpy as np
import os
import env

# ==========================================================================

def get_connection(db: str, user: str = env.user, host: str = env.host, password=env.password) -> str: 
    '''
       This function takes in a data base, username, password, and hoast from MySQL
       and returns a my url as a string
    '''
    return f"mysql+pymysql://{user}:{password}@{host}/{db}"

# ==========================================================================

def get_all_cohort_data(file_name="cohort_logs.csv") -> pd.DataFrame: 
    if os.path.isfile(file_name):
        return pd.read_csv(file_name)
    query = """select * 
               from cohorts as c
               right join logs as l
               on l.cohort_id = c.id;
            """
    connection = get_connection("curriculum_logs")
    df = pd.read_sql(query, connection)
    df.to_csv(file_name, index=False)
    return df

# ==========================================================================


def get_curriculum(file_name="anonymized-curriculum-access.txt") -> pd.DataFrame:
    """Takes in one parameter called file name
    and is already defined.
    Because it's a text file I added a space so data is
    tabular and added no header so the first row is maintained"""
    return pd.read_csv(file_name, sep=" ", header=None)


# ==========================================================================


def prep_cohort(df: pd.DataFrame) -> pd.DataFrame:
    '''Takes in a dataframe 
       Creates new column date_time to hold date and time 
       sets date_time as a DateTime value
       set date_time as the index
       drops columns that will not be used 
       fills 1 missing value with the mode of that column
       map program id's for the user 
       creates a copy of the full dataframe as is 
       drops all null values'''
    df['date_time'] = df['date'] + ' ' + df['time']
    df.date_time = pd.to_datetime(df.date_time)
    df= df.set_index('date_time')
    df = df.drop(columns=['deleted_at', 'id', 'date', 'time'])
    df = df.fillna({'path': df['path'].mode()[0]})
    df['program_id'] = df.program_id.map({1.0: 'Full-Stack Web Development – PHP', 
                          2.0: 'Full-Stack Web Development – Java',
                          3.0: 'Data Science',
                          4.0: 'Front-End Web Development'})
    cohort_df_with_nulls = df.copy()
    df = df.dropna()
