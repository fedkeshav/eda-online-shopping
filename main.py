#%%
# IMPORTING DATA INTO DATAFRAME AND STORING IT AS CSV
import db_utils

db_extract = db_utils.RDSDatabaseConnector()
engine = db_extract.init_db_engine()
conn = engine.connect()

df = db_extract.read_rds_data(engine, 'customer_activity')
db_extract.df_to_csv(df, 'customer_data.csv')


# %%
df.info()
# %%
df.head(30)
# %%
