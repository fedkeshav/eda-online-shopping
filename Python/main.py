#%%
#1. IMPORTING NECESSAYR MODULES AND CREATING CLASS INSTANCES
import db_utils
import db_cleaning
import pandas as pd

db_extract = db_utils.RDSDatabaseConnector()
db_info = db_utils.DataFrameInfo()
db_clean = db_cleaning.DataFrameTransform()
db_plotter = db_utils.Plotter()
engine = db_extract.init_db_engine()
conn = engine.connect()

#%%
#2. CREATING FUNCTIONS FOR CLEANING - TO BE USED BELOW
def null_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Imputes or drops null values across columns, and cleans format and other errors

    Inputs:
        Dataframe
    
    Returns:
        Dataframe without nulls
    '''
    # Imputations for more categorical variables (stored as floats though)
    for column in ['administrative','product_related']:
        df = db_clean.impute_categorical(df, column)
    ## Imputations for purely numeric variables
    var_pairs = [['administrative','administrative_duration'],
                ['informational','informational_duration'],
                ['product_related','product_related_duration']]
    for vars in var_pairs:
        df = db_clean.impute_numerical(df,vars[0],vars[1])
    ## Removing null for some of the other variables
    df = db_clean.drop_null(df,'operating_systems')
    df = db_clean.drop_null(df, 'product_related_duration') # Dropping one row for which the impute method did not work
    ## Cleaning datatype format
    df = db_clean.format_cleaning(df)
    df = db_clean.region_cleaning(df)
    print('Dataframe after null cleaning')
    print(df.info())
    return df

def outlier_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Cleans all outlier values

    Inputs:
        Dataframe
    
    Returns:
        Dataframe removing outliers
    '''
    cond = (df['product_related_duration'] < 20000)
    df = db_clean.product_related_outliers_transformation(df,cond)
    print('Dataframe after outlier cleaning')
    print(df.info())
    return df

def skewness_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Transforms skewed variables

    Inputs:
        Dataframe
    
    Returns:
        Dataframe with transformed variables
    ''' 
    ## Transforming variables
    df = df
    db_clean.skewness_log_transformation(df, 'administrative_duration','transformed_administrative_duration')
    db_clean.skewness_log_transformation(df, 'informational_duration', 'transformed_informational_duration')
    db_clean.skewness_boxcox_transformation(df, 'product_related_duration', 'transformed_product_related_duration', 0.01)
    db_clean.skewness_boxcox_transformation(df, 'bounce_rates', 'transformed_bounce_rates', 1e-10)
    db_clean.skewness_boxcox_transformation(df, 'exit_rates','transformed_exit_rates', 1e-10)
    db_clean.skewness_log_transformation(df, 'page_values','transformed_page_values')
    return df

def skewness_pre_post(df: pd.DataFrame) -> None:
    '''
    Summarises skewness of original and transformed variable

    Inputs:
        Dataframe
    
    Returns:
        None
    ''' 
    var_list_pairs = [['administrative_duration','transformed_administrative_duration'],
                    ['informational_duration','transformed_informational_duration' ],
                    ['product_related_duration','transformed_product_related_duration' ],
                    ['bounce_rates','transformed_bounce_rates'],
                    ['exit_rates','transformed_exit_rates'],
                    ['page_values','transformed_page_values'] ]
    for var in var_list_pairs:
        db_info.compare_skewness(df, var[0], var[1])

def collinearity_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Cleans collinear variables

    Inputs:
        Dataframe
    
    Returns:
        Dataframe cleaned for collinearity
    ''' 
    df.drop(['bounce_rates', 'transformed_bounce_rates'], axis = 1, inplace=True)
    return df

def ordering_columns(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Orders dataframe columns consistently

    Inputs:
        Dataframe
    
    Returns:
        Dataframe with columns in desired order
    ''' 
    desired_order =['administrative', 
                'administrative_duration', 
                'transformed_administrative_duration',
                'informational', 
                'informational_duration', 
                'transformed_informational_duration', 
                'product_related', 
                'product_related_duration', 
                'transformed_product_related_duration', 
                'exit_rates', 
                'transformed_exit_rates',
                'page_values', 
                'transformed_page_values',
                'month', 
                'operating_systems', 
                'browser', 
                'region', 
                'traffic_type', 
                'visitor_type', 
                'weekend', 
                'revenue' 
                ]
    df = df[desired_order]
    return df

#%%
#3. EXTRACTING THE DATA FROM THE DATABASE
df = db_extract.read_rds_data(engine, 'customer_activity')
db_extract.df_to_csv(df, 'customer_data.csv')
numeric_features = ['administrative',
                    'administrative_duration',
                    'informational',
                    'informational_duration',
                    'product_related',
                    'product_related_duration',
                    'bounce_rates',
                    'exit_rates',
                    'page_values']
categorical_features = [col for col in df.columns if col not in numeric_features]

#%% 
#4. TRANSFORMING THE DATA
df = null_cleaning(df) #Dealing with Nulls
df = outlier_cleaning(df) #Dealing with Outliers
df = skewness_cleaning(df) #Dealing with Skewness
skewness_pre_post(df) #See skewness pre and post transformation
df = collinearity_cleaning(df) #Dealing with Collinearity
df = ordering_columns(df) #Ordering columns as required
df.info()