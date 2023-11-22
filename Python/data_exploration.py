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
# %%
#2. EXPLORING THE DATA
db_info.display_info(df)
db_info.summary_statistics(df)
db_info.null_summary(df)
for column in categorical_features:
    db_info.explore_categorical_variable(df,column)
for column in numeric_features:
    db_info.explore_continuous_variable(df,column)
db_info.visualise_missing_data(df)
db_plotter.corr_matrix(df)
db_plotter.corr_scatter(df,numeric_features)

#%%
#3. VISUALISING OUTLIERS
# Admin duration and admin are fine
db_plotter.scatterplot(df,'administrative', 'administrative_duration')
db_plotter.var_distribution(df, 'administrative_duration')
# Info and info_duration are fine
db_plotter.scatterplot(df,'informational', 'informational_duration')
db_plotter.var_distribution(df, 'informational_duration')
# Product_related_duration has outliers  over values of 20k
db_plotter.scatterplot(df,'product_related', 'product_related_duration')
db_plotter.var_distribution(df, 'product_related_duration')
# Bounce rates and exit rates are fine (except potentially 2 data points borderline)
db_plotter.scatterplot(df,'bounce_rates', 'exit_rates')
# Page values are fine
db_plotter.var_distribution(df, 'page_values')
db_plotter.scatterplot(df,'page_values', 'bounce_rates')

#%%
#4. CHECKING AND CLEANING FOR SKEWNESS
db_plotter.vars_distribution(df, numeric_features)
for var in numeric_features:
    print(f' Skewness for {var} = {df[var].skew().round(1)}')
    print('')
## Solving for skewness of all variables excluding more categorical variables - admin, info, product_related
## Analysing each variable
for var in numeric_features:
    db_info.var_skewness(df, var)
    df[var].describe()
    db_plotter.var_distribution(df, var)
    db_plotter.qqplot(df, var)

#%%
#5. CHECKING AND CLEANING FOR COLLINEARITY
db_plotter.corr_matrix(df)
print(df['bounce_rates'].corr(df['exit_rates']))
print(df['product_related'].corr(df['product_related_duration']))