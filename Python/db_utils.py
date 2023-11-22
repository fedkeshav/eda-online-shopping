import pandas as pd
import sqlalchemy as db
from sqlalchemy.engine import Connection
from typing import TextIO
import yaml
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from statsmodels.graphics.gofplots import qqplot


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
        path = '/Users/keshavparthasarathy/Documents/AICore_projects/exploratory-data-analysis---online-shopping-in-retail285/'
        creds_dict = self.read_db_creds(path+'credentials.yaml')
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
        raw_df = pd.read_sql_table(table_name, engine)
        return raw_df

    def df_to_csv(self, df: pd.DataFrame, csv_name: str) -> TextIO:
        '''
        Converts a dataframe to csv object

        Inputs:
            Dataframe

        Returns:
            Nothing - saves file locally to csv object
        '''
        df.to_csv(csv_name, index=False)

    
class DataFrameInfo():
    '''
    This is class created for defining methods to find more information about the dataframe
    '''
    def __init__(self) -> None:
        pass

    def display_info(self, df: pd.DataFrame)-> None:
        '''
        Prints basic info about the dataframe

        Inputs:
            DataFrame

        Returns:
            None
        '''
        print('DATAFRAME INFO:')
        print('___________________________________')
        print(df.info())
        print('___________________________________')
        print('')

    def summary_statistics(self, df: pd.DataFrame)-> None:
        '''
        Prints summary statistics of numerical columns

        Inputs:
            DataFrame

        Returns:
            None
        '''
        print('SUMMARY STATISTICS:')
        print('___________________________________')
        print(df.describe())
        print('___________________________________')
        print('')

    def explore_categorical_variable(self,df: pd.DataFrame, column: str)-> None:
        '''
        Explores categorical variables using count plots

        Inputs:
            DataFrame and categorical column

        Returns:
            None
        ''' 
        plt.figure(figsize=(10,6))
        sns.countplot(x=column, data = df)
        plt.title((f'Count values of {column}'))
        plt.show()

    def explore_continuous_variable(self,df: pd.DataFrame, column: str) -> None:
        '''
        Explore continuous variables using histogram

        Inputs:
            DataFrame and continuous column

        Returns:
            None
        '''
        plt.figure(figsize=(10,6))
        sns.histplot(df[column], bins = 'auto', kde=True)
        plt.title((f'Historgram of {column}'))
        plt.show()

    def visualise_missing_data(self, df: pd.DataFrame) -> None:
        '''
        Visualises missing data using heatmap

        Inputs:
            DataFrame

        Returns:
            None        
        '''
        plt.figure(figsize=(10,6))
        sns.heatmap(df.isnull(),cbar=False,cmap='viridis')
        plt.title('Missing data visualisation')
        plt.show()

    def null_percent(self,df: pd.DataFrame, column: str) -> None:
        '''
        Calculate percent of values that are null

        Inputs:
            DataFrame and column

        Returns:
            None        
        '''     
        null_percent = df[column].isnull().sum()/len(df)*100
        null_percent = null_percent.round(1)
        print(f'Percent of null values for {column} is {null_percent}%')
        print('')

    def null_summary(self, df: pd.DataFrame) -> None:
        '''
        Generates summary of null values in percent across columns

        Inputs:
            DataFrame

        Returns:
            None        
        '''     
        null = (df.isnull().sum()/len(df))*100
        null = null.round(1)
        print(f'% OF NULL VALUES: ')
        print('___________________________________')
        print(null)
        print('___________________________________')
        print('')

    def var_skewness(self, df: pd.DataFrame, column: str) -> None:
        '''
        Generates skewness of the variable

        Inputs:
            DataFrame and column

        Returns:
            None        
        '''     
        print(f' Skewness for {column} is {df[column].skew().round(1)}')
        print('')

    def compare_skewness(self, df: pd.DataFrame, column1: str, column2: str) -> None:
        '''
        Compares skewness of two variables

        Inputs:
            DataFrame and two columns for comparison

        Returns:
            None        
        '''    
        print(f' Skewness for {column1} = {df[column1].skew().round(1)}')
        print(f' Skewness for {column2} = {df[column2].skew().round(1)}')
        print('')


class Plotter():
    '''
    This is class created for defining methods to plot charts about the data
    '''
    def __init__(self) -> None:
        pass

    def corr_matrix(self, df: pd.DataFrame) -> None:
        '''
        Displays correlation matrix across variables

        Inputs:
            DataFrame

        Returns:
            None        
        '''   
        numerical_columns = df.select_dtypes(include=['number'])
        corr = numerical_columns.corr()
        mask = np.zeros_like(corr, dtype=np.bool_)
        mask[np.triu_indices_from(mask)] = True
        cmap = sns.diverging_palette(220, 10, as_cmap=True)
        sns.heatmap(corr, mask=mask, 
                    square=True, linewidths=.5, annot=True, cmap=cmap)
        plt.yticks(rotation=0)
        plt.title('Correlation Matrix of Numerical Variables')
        plt.show()

    def corr_scatter(self, df: pd.DataFrame, columns: list) -> None:
        '''
        Displays correlation scatter across a list of numerical variables

        Inputs:
            DataFrame and a list of columns to assess correlation

        Returns:
            None        
        '''   
        numerical_columns = df.select_dtypes(include=['number'])
        sns.pairplot(df[columns])

    def vars_distribution (self, df: pd.DataFrame, columns: list) -> None:
        '''
        Displays frequency distribution for a list of numerical variables

        Inputs:
            DataFrame and a list of columns 

        Returns:
            None        
        '''   
        sns.set(font_scale=0.7)
        f = pd.melt(df, value_vars=columns)
        g = sns.FacetGrid(f, col="variable",  col_wrap=3, sharex=False, sharey=False)
        g = g.map(sns.histplot, "value", kde=True)

    def var_distribution (self, df: pd.DataFrame, column: str) -> None:
        '''
        Displays frequency distribution for a variable

        Inputs:
            DataFrame and a column 

        Returns:
            None        
        '''  
        plt.figure(figsize=(10, 5))
        sns.histplot(df[column], kde=True)

    def qqplot(self, df: pd.DataFrame, column:str) -> None:
        '''
        Displays qq plot for a column to assess if it is normally distributed

        Inputs:
            DataFrame and a column 

        Returns:
            None        
        '''  
        qq_plot = qqplot(df[column] , scale=1 ,line='q', fit=True)
        plt.show()

    def boxplot(self, df: pd.DataFrame, column: str) -> None:
        '''
        Displays boxplot and swarmplot for a column to assess outliers

        Inputs:
            DataFrame and a column 

        Returns:
            None        
        '''  
        sns.set(style="whitegrid")
        plt.figure(figsize=(10,5))
        sns.boxplot(y=column,data=df, color='lightgreen', showfliers=True)
        sns.swarmplot(y=column, data=df, color='black', size=5)
        plt.title(f'Box plot with scatter points for {column}')
        plt.show()

    def scatterplot(self, df: pd.DataFrame, x_column: str, y_column: str) -> None:
        '''
        Displays scatterplot for two columns

        Inputs:
            DataFrame and two columns 

        Returns:
            None        
        '''  
        plt.figure(figsize=(10,5))
        sns.scatterplot(x=df[x_column], y=df[y_column])
        sns.regplot(x=df[x_column], y=df[y_column])