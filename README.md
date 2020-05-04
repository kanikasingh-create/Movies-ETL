# Movies-ETL

## Overview
Creating an automated ETL pipeline to:
  * Extract data from multiple sources.
  * Clean and transform the data automatically using Pandas and regular expressions and;
  * Load the new data into a database in PostgreSQL.
  
## Challenge Summary
Using Visual Studio Code, we have created an automated pipeline that takes in new data, performs the appropriate transformations, and loads the data into existing tables. These steps can be seen in the challenge.py script, which documents the steps that were taken to create the pipeline.

1. Initially, we created a function to import data files using pandas. This funtion simultaneously
    (a) imports data from wikipedia json file (wikipedia.movies.json)
    (b) imports data from Kaggle metadata (movies_metadata.csv)
    (c) imports data from MovieLens rating data (ratings.csv)
  
2. We then clean and transform the data to analyze the data, create new dataframes, and load the cleaned data to an SQL database.
* The data transformation is performed with the following assumptions to exclude any bad data:
    (a) Drop columns with >90% null values. This is based on the assumption that 10% of a large dataframe provides inconsequential information.
    (b) Remove outliers that are mismatched in different datasets.
    (c) Creating clean data will cause us to unintentionally drop a few values. Because of this, we will assume that losing this small amount of data won't affect impact our analysis since this is a fairly large dataset.
    (d) We intentionally drop columns that are not necessary for Amazing Prime's analysis, such as video or runtime.
    (e) Since many aspects of movies are similar (i.e. budget, box office revenue, release date, among others), we assum that the 3 different datasets have common columns. This allows us to merge the different datasets and fill in missing or null values in one dataframe from another.

3. We add try-except blocks to account for unforeseen problems that may arise with new data. In our case, this is the IMDB identification number count.

4. We perform formatting, such as renaming column names, leaving only original titles in the cleaned dataset, transforming column datatypes and other data using regular expressions etc. to improve readability and ensure only clean, non-duplicated data is captured.

We check that the function works correctly on the current Wikipedia and Kaggle data, and then pull this new dataframe into an SQL database, thus completing the requirement of creating an automated pipeline that takes in new data, performs the appropriate transformations, and loads the data into existing tables.
