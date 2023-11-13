import pandas as pd
import sqlalchemy as db
from sqlalchemy.engine import Connection
from typing import TextIO
import yaml


class RDSDatabaseConnector():
    '''
    This is class created for defining methods to extract data from RDS Database
    '''
    
    def __init__(self):
        pass

    def read_db_creds(self, file: yaml) -> dict:
        '''
        Reads and returns database credentials

        Inputs:
            A file in yaml format containting database credentials

        Returns:
            Database credentials as a dictionary
        '''
        with open(file, 'r') as creds:
            data_creds = yaml.safe_load(creds)
        return data_creds

    def init_db_engine(self) -> Connection:
        '''
        Starts the engine to the database

        Inputs:
            None

        Returns:
            Engine to the database
        '''
        creds_dict = self.read_db_creds('credentials.yaml')
        creds = list(creds_dict.values())
        url = (f"{creds[0]}+{creds[1]}://{creds[2]}:{creds[3]}@{creds[4]}:{creds[5]}/{creds[6]}")
        engine = db.create_engine(url)
        return engine
    
    def read_rds_data(self, engine: Connection, table_name: str) -> pd.DataFrame:
        '''
        Reads table from database into a dataframe

        Inputs:
            Engine connected to database and the table name to read

        Returns:
            A dataframe
        '''
        df = pd.read_sql_table(table_name, engine)
        return df

    def df_to_csv(self, df: pd.DataFrame, csv_name: str) -> TextIO:
        '''
        Converts a dataframe to csv object

        Inputs:
            Dataframe

        Returns:
            Nothing - saves file locally to csv object
        '''
        df.to_csv(csv_name, index=False)

