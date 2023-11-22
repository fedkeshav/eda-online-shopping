import pandas as pd
import sqlalchemy as db
from sqlalchemy.engine import Connection
from typing import TextIO
import yaml
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import boxcox


class DataFrameTransform():
    ''' Class with methods to transform original data'''
    def __init__(self) -> None:
        pass

    def impute_numerical(self, df: pd.DataFrame, column1: str, column2: str) -> pd.DataFrame:
        ''' 
        Imputes missing values based on median value corresponding to the variable correlated with it

        Inputs:
            dataframe, categorical column, numerical column

        Returns:
            Dataframe with imputed values for the null values 
        '''
        df['median'] = df.groupby(column1)[column2].transform('median')
        df[column2] = df[column2].fillna(df['median'])
        df.drop(['median'], axis=1, inplace=True)
        return df
    
    def impute_categorical(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        ''' 
        Imputes missing values for categorical columns using modal value

        Inputs:
            dataframe, categorical column (albeit stored as float)

        Returns:
            Dataframe with imputed values for the null values       
        '''
        mode = df[column].mode().iloc[0]
        condition = (df[column].isna())
        df.loc[condition,column] = mode
        return df
    
    def drop_null(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        '''
        Drops missing values - to be used when percent of missing values are very low

        Inputs:
            Dataframe and column
            
        Returns:
            Dataframe after dropping rows with null values        
        '''
        df = df.dropna(subset=[column])
        return df

    def format_cleaning(self,df: pd.DataFrame,) -> pd.DataFrame:
        '''
        Converts columns into their appropriate formats
        
        Inputs:
            Dataframe
        
        Returns:
            Cleaned Dataframe        
        '''
        for x in ['month','browser','operating_systems','region','traffic_type','visitor_type']:
            df[x] = df[x].astype('category')
        for x in ['administrative','informational','product_related']:
            df[x] = df[x].astype(int)
        return df

    def region_cleaning(self, df: pd.DataFrame,) -> pd.DataFrame:
        '''
        Converts Southern Africa and Northern Africa to Africa
        
        Inputs:
            Dataframe
        
        Returns:
            Cleaned Dataframe        
        '''
        condition = (df['region'] == 'Northern Africa') | (df['region'] == 'Southern Africa')
        df.loc[condition, 'region'] = 'Africa'
        return df
    
    def skewness_log_transformation(self, df: pd.DataFrame, column1: str, column2: str) -> None:
        '''
        Log transformation to reduce skewness
        
        Inputs:
            Original column name and new column name
        
        Returns:
            Dataframe with original and transformed variable       
        '''
        df[column2] = df[column1].map(lambda i: np.log(i) if i > 0 else 0)
    
    def skewness_boxcox_transformation (self, df: pd.DataFrame, column1: str, column2: str, constant: float) -> None:
        '''
        BoxCox transformation to reduce skewness
        
        Inputs:
            Original column name and new column name
        
        Returns:
            Dataframe with original and transformed variable       
        '''        
        df[column2], lambda_value = boxcox(df[column1] + constant)

    def product_related_outliers_transformation(self, df: pd.DataFrame, condition: pd.Series) -> pd.DataFrame:
        '''
        Removes outliers
        
        Inputs:
            Original DataFrame and condition for filtering outliers
        
        Returns:
            Cleaned Dataframe removing outliers from the column       
        '''
        dff = df[condition]
        return dff