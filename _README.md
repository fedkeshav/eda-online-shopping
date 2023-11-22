# Exploratory data analysis - Online shopping data

## Description
In most real world situations, before any analytics or data science can be applied on the data, we need to explore and understand the current patterns in data. This includes checking and working with null values, checking distributions of data (e.g., are they normal), checking skewness and checking collinearity. In addition, we should check if each variable has the right data format and if there are any typos/other errors in the data. All of this is termed as exploratory data analysis.

This project shows how to do exploratory data analysis on a online retail shopping data.

## Tools and Technologies used
General python packages
- sqlalchemy (for connection to databases)
- pandas
- numpy
- yaml
- typing

Statistical packages
- Matplot
- Seaborn
- Scipy
- statsmodels

## How to import and use the environment
1. Download the 'environment.yml' file to download all the packages needed to run the python files
2. Create the same environment from the downloaded 'environment.yml' file by using this code on your terminal: 'conda env create -f environment.yml'
3. Activate the environment using 'conda activate your_environment_name'

## Folder structure
- The raw data can be found in the file 'customer_data.csv'.
- The packages needed to run the code can be found in 'environment.yml'
- All python files required to explore and analyse the data can be found in the 'Python' folder.
- The output of the exploration and analyses can be found in the 'Outputs' folder.


