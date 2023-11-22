#%%
#1. IMPORTING DATA INTO DATAFRAME
import db_utils
import db_cleaning
db_extract = db_utils.RDSDatabaseConnector()
db_info = db_utils.DataFrameInfo()
db_clean = db_cleaning.DataFrameTransform()
db_plotter = db_utils.Plotter()
engine = db_extract.init_db_engine()
conn = engine.connect()
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
#2. CLEANING THE DATA FOR NULL VALUES
db_info.null_summary(df)
## Imputations of ordinal categorical variables
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
df.info()

#%% 
#3. CLEANING FOR OUTLIERS (BEFORE SKEWNESS)

## Removing 7 outliers in product_related_duration
cond = (df['product_related_duration'] < 20000)
df = db_clean.product_related_outliers_transformation(df,cond)

# %%
#4. CLEANING FOR SKEWNESS
## Transforming variables
db_clean.skewness_log_transformation(df, 'administrative_duration','transformed_administrative_duration')
db_clean.skewness_log_transformation(df, 'informational_duration', 'transformed_informational_duration')
db_clean.skewness_boxcox_transformation(df, 'product_related_duration', 'transformed_product_related_duration', 0.01)
db_clean.skewness_boxcox_transformation(df, 'bounce_rates', 'transformed_bounce_rates', 1e-10)
db_clean.skewness_log_transformation(df, 'exit_rates','transformed_exit_rates')
db_clean.skewness_log_transformation(df, 'page_values','transformed_page_values')
## Summarising changes in skewness after transformation
var_list_pairs = [['administrative_duration','transformed_administrative_duration'],
                  ['informational_duration','transformed_informational_duration' ],
                  ['product_related_duration','transformed_product_related_duration' ],
                  ['bounce_rates','transformed_bounce_rates'],
                  ['exit_rates','transformed_exit_rates'],
                  ['page_values','transformed_page_values'] ]
for var in var_list_pairs:
    db_info.compare_skewness(df, var[0], var[1])

# %%
#5. CHECKING AND CLEANING FOR COLLINEARITY
## Exit and bounce rates seem to be the  only highly co-related variables (91%)
## Dropping bounce rates as it is slightly more skewed
df.drop(['bounce_rates', 'transformed_bounce_rates'], axis = 1, inplace=True)
## Re-ordering columns - keeping original columns for interpretability
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
df.info()