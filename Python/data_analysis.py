#%%
import db_utils
import db_cleaning
import pandas as pd
from functools import reduce

db_extract = db_utils.RDSDatabaseConnector()
db_info = db_utils.DataFrameInfo()
db_clean = db_cleaning.DataFrameTransform()
db_plotter = db_utils.Plotter()
#%%
df = pd.read_csv('/Users/keshavparthasarathy/Documents/AICore_projects/exploratory-data-analysis---online-shopping-in-retail285/Cleaned_customer_data')
#%%
df.info()
# %%
#1. What are our customers doing?
# (a) Are sales proportionally happening more on weekends?
aggregation = {'month': 'count','revenue': 'sum', 'page_values': 'sum'}
ag_df = df.groupby('weekend').agg(aggregation).reset_index()
ag_df = ag_df.rename(columns={'month': 'sessions'})
ag_df['percent_sessions_generating_revenue'] = ((ag_df['revenue']/ag_df['sessions'])*100).round(1)
ag_df['revenue_per_session'] = (ag_df['page_values'] / ag_df['sessions']).round(1)
ag_df.drop(['revenue','page_values'], axis=1, inplace=True)
ag_df.head()
# %%
# (b) Which regions are generating the most revenue?
aggregation = {'page_values': 'sum'}
ag_df = df.groupby('region').agg(aggregation).reset_index()
ag_df['page_values'] = ag_df['page_values'].astype(int)
ag_df = ag_df.sort_values(by='page_values', ascending=False)
ag_df.head() 
# %%
# (c) Which traffic type is generating most revenue?
aggregation = {'page_values': 'sum'}
ag_df = df.groupby('traffic_type').agg(aggregation).reset_index()
ag_df['page_values'] = ag_df['page_values'].astype(int)
ag_df = ag_df.sort_values(by='page_values', ascending=False)
ag_df.head() 
# %%
# (d) What percentage of time is spent on the website performing administrative/product or informational related tasks?
aggregation = {'informational_duration': 'sum', 'administrative_duration': 'sum', 'product_related_duration': 'sum'}
ag_df = df.agg(aggregation).reset_index()
ag_df.columns = ['task_type', 'duration']
ag_df['percent_duration'] = (ag_df['duration']/(ag_df['duration'].sum())*100).round(1) 
ag_df.drop('duration', axis=1, inplace=True)
ag_df.head() 
# (e) Are there any informational/administrative tasks which users spend time doing most?
aggregation = {'informational_duration': 'sum', 'administrative_duration': 'sum', 'product_related_duration': 'sum'}
ag_df = df.groupby('month').agg(aggregation).reset_index()
ag_df['page_values'] = ag_df['page_values'].astype(int)
ag_df = ag_df.sort_values(by='page_values', ascending=False)
ag_df.head()  
# %%
# (f) What percent of sales comes in different months?
aggregation = {'page_values': 'sum'}
ag_df = df.groupby('month').agg(aggregation).reset_index()
ag_df['page_values'] = ag_df['page_values'].astype(int)
ag_df = ag_df.sort_values(by='page_values', ascending=False)
ag_df.head()  

# %%
#2. What softwares our customers are using?
#(a) What are the operating systems used by customers?
db_info.explore_categorical_variable(df,'operating_systems')
# %%
#(b) How many use mobile vs web operating systems?
temp_df = df
condition = (temp_df['operating_systems']=='Android') | (temp_df['operating_systems']=='iOS')
temp_df['device_type'] ='desktop'
temp_df.loc[condition, 'device_type'] = 'mobile'
db_info.explore_categorical_variable(temp_df,'device_type') 
# %%
#(c) What are the commonly used browsers and their breakdown of mobile vs desktop?
db_info.explore_categorical_variable(temp_df,'browser')
for var in ['mobile', 'desktop']:
    new_df = temp_df[temp_df['device_type']==var]
    print(f' Device type is {var}')
    print('_____________________________')
    db_info.explore_categorical_variable(new_df,'browser')
    print('')
    print('')
# %%
#(d) Do the operating system and browser usage vary by region?
def region_software_table(var: str) -> pd.DataFrame:
    '''
    This function creates a new dataframe with the software types as rows, and percent of use in differernt countries as columns

    Inputs:
        A variable - operating systems, device type or browser

    Returns:    
        A new dataframe   
        '''
    data = []
    regions = temp_df['region'].unique().tolist()
    for region in regions:
        new_df = temp_df[temp_df['region']==region] 
        agg_df = new_df.groupby(var).agg({var: 'count'})
        agg_df['percentage_use'] = ((agg_df[var] / agg_df[var].sum())*100).astype(int)
        agg_df.drop(var, axis=1, inplace=True)
        agg_df.columns = [region]
        data.append(agg_df)
    merged_df = data[0]
    for df in data[1:]:
        merged_df = pd.merge(merged_df, df, on=var)
    return merged_df

os_df = region_software_table('operating_systems')
device_df = region_software_table('device_type')
browser_df = region_software_table('browser')
os_df.head(20)
device_df.head(20)
browser_df.head(20)

# %%
#3. Effective marketing?
## (i) Sales contribution of different traffic channels by region
data = []
traffic_types = df['traffic_type'].unique().tolist()
for traffic in traffic_types: # Creating a list of dataframes, with each element of a list being a sales for a traffic type across regions
    temp_df = df[df['traffic_type']==traffic]
    agg_df = temp_df.groupby('region').agg({'page_values': 'sum'})
    agg_df.columns = [traffic]
    data.append(agg_df)
merged_df = reduce(lambda left, right: pd.merge(left, right, on='region', how='outer'), data)  # Merging list elements into dataframe using the 'region' column  
merged_df['total_sales'] = merged_df.sum(axis=1) #Calculating total sales to estimate percentage sales below
columns = merged_df.columns.tolist()
for column in columns: #Converting absolute sales to percentage of total
    merged_df[column] = ((merged_df[column] / merged_df['total_sales'])*100).round(1)
merged_df['other'] = (merged_df['Newsletter']
               + merged_df['Other'] 
               + merged_df['Yahoo Search'] 
               + merged_df['Pinterest']
               + merged_df['Yandex search'] 
               + merged_df['Tik Tok page'] 
               + merged_df['Facebook page']
               + merged_df['DuckDuckGo search']) #Consolidating small channels to'other'
merged_df.drop(['Newsletter', 'Other', 'Yahoo Search', 'Pinterest', 'Yandex search', 'Tik Tok page', 'Facebook page', 'DuckDuckGo search', 'total_sales'], axis = 1, inplace=True)
# Plotting the graph
xlabel = 'Region'
ylabel= 'Sales Contribution (%)'
title = 'Contribution of different traffic channels to sales'
legend_title = 'Traffic Type'
db_plotter.barplot(merged_df, stack=True, xlabel=xlabel, ylabel=ylabel, title=title, legend_title=legend_title)
# %%
## (ii) Bounce rate by channel
temp_df = df
agg_df = temp_df.groupby('traffic_type').agg({'exit_rates': 'mean'})
agg_df = agg_df.sort_values(by='exit_rates',ascending=False)
agg_df['exit_rates'] = (agg_df['exit_rates']*100).round(1)
xlabel = 'Traffic type'
ylabel= 'Average bounce/exit rates'
title = 'Bounce rates by traffic type'
db_plotter.barplot(agg_df, stack=False,xlabel=xlabel, ylabel=ylabel, title=title, ycolumn='exit_rates')
# %%
## (iii) Which months have generated most sales from ads traffic?
mask = df['traffic_type'].str.contains('ads')
temp_df = df[mask]
agg_df = temp_df.groupby('month').agg({'page_values': 'sum'})
# Mapping dictionary for converting months to numbers
month_to_number = {
    'Jan': 1,
    'Feb': 2,
    'Mar': 3,
    'Apr': 4,
    'May': 5,
    'June': 6,
    'Jul': 7,
    'Aug': 8,
    'Sep': 9,
    'Oct': 10,
    'Nov': 11,
    'Dec': 12
}
agg_df['month_number'] = agg_df.index.map(month_to_number).astype(int)
agg_df = agg_df.sort_values(by='month_number', ascending=True)
xlabel = 'month'
ylabel = 'Sales from ads'
title = 'Sales from ads across months'
db_plotter.barplot(agg_df['page_values'], stack=False,xlabel=xlabel, ylabel=ylabel, title=title)
# %%
#4. Revenue generation
## (a) By country
temp_df = df
agg_df = temp_df.groupby('region').agg({'page_values': 'sum'})
agg_df = agg_df.sort_values(by='page_values', ascending=False)
ylabel = 'Sales'
xlabel = 'Region'
title = 'Sales by region'
db_plotter.barplot(agg_df, stack=False,xlabel=xlabel, ylabel=ylabel, title=title, ycolumn='page_values')
# %%
# (b) By type of day
temp_df = df
agg_df = temp_df.groupby('weekend').agg({'page_values': 'sum'})
ylabel = 'Sales'
xlabel = 'Day type'
title = 'Sales by day type'
db_plotter.barplot(agg_df, stack=False,xlabel=xlabel, ylabel=ylabel, title=title, ycolumn='page_values')
# %%
# (c) By month
temp_df = df
agg_df = temp_df.groupby('month').agg({'page_values': 'sum'})
agg_df['month_number'] = agg_df.index.map(month_to_number).astype(int)
agg_df = agg_df.sort_values(by='month_number')
ylabel = 'Sales'
xlabel = 'Month'
title = 'Sales by month'
db_plotter.barplot(agg_df, stack=False,xlabel=xlabel, ylabel=ylabel, title=title, ycolumn='page_values')
# %%
# (d) By traffic category
temp_df = df
traffic_category = {
    'Twitter': 'Social',
    'Google search': 'Search', 
    'Instagram ads': 'Ads',
    'Youtube channel': 'Social', 
    'Instagram Page': 'Social', 
    'Affiliate marketing': 'Ads',
    'Facebook ads': "Ads" ,
    'Youtube ads': "Ads" ,
    'Tik Tok ads': "Ads" , 
    'Bing search': 'Search', 
    'Direct Traffic': "Direct/Other" ,
    'Newsletter': "Direct/Other" , 
    'Other': "Direct/Other" , 
    'Yahoo Search': 'Search', 
    'Pinterest': 'Social', 
    'Yandex search': 'Search', 
    'Tik Tok page': 'Social', 
    'Facebook page': 'Social', 
    'DuckDuckGo search': 'Search',
}
temp_df['traffic_category'] = temp_df['traffic_type'].map(traffic_category)
agg_df = temp_df.groupby('traffic_category').agg({'page_values': 'sum'})
agg_df = agg_df.sort_values(by='page_values')
ylabel = 'Sales'
xlabel = 'Traffic category'
title = 'Sales by traffic category'
db_plotter.barplot(agg_df, stack=False,xlabel=xlabel, ylabel=ylabel, title=title, ycolumn='page_values')
# %%
# (e) What percent of returning and new customers make a purchase when they visit the site?
temp_df = df
temp_df['mask'] = (temp_df['page_values']>0)
aggregation = {'mask': 'sum', 'traffic_type': 'count' }
agg_df = temp_df.groupby('visitor_type').agg(aggregation)
agg_df['percent_positive_sales'] = ((agg_df['mask'] / agg_df['traffic_type'])*100).round(1)
agg_df.drop(['mask','traffic_type' ], axis=1, inplace=True)
ylabel = 'Percent of visits converting to sale'
xlabel = 'Customer type'
title = 'Percent of visits converting to sale by customer type'
db_plotter.barplot(agg_df, stack=False,xlabel=xlabel, ylabel=ylabel, title=title, ycolumn='percent_positive_sales')