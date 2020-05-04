# Import all dependencies.
import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
import psycopg2
from config import db_password
import time

# Import wiki json file.
file_dir = "/Users/kanikasingh/Documents/UCB/Projects/M8/"
with open(f'{file_dir}/wikipedia-movies.json', mode='r') as file:
    wiki_movies_raw = json.load(file)

# Challenge – Use the code from Jupyter Notebook so that the function performs all of the transformation steps, with no exploratory data analysis or redundant code.
# Create a wikipedia DataFrame with list comprehension.
wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie]
wiki_movies_df = pd.DataFrame(wiki_movies)
wiki_movies_df.head()

# Pull Kaggle data directly into Pandas DataFrames.
kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv')
ratings = pd.read_csv(f'{file_dir}ratings.csv')

# Form all variables.
form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
form_two = r"\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)"
date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
date_form_two = r'\d{4}.[01]\d.[123]\d'
date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
date_form_four = r'\d{4}'

# Clean data.
def clean_movie(movie):
    movie = dict(movie)
    alt_titles = {}
    # Combine alternate titles into one list.
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune–Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:

        # Check if the current key exists in the movie object.
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)

    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles

    # Merge column names.
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name('Directed by', 'Director')
    change_column_name('Distributed by', 'Distributor')
    change_column_name('Edited by', 'Editor(s)')
    change_column_name('Length', 'Running time')
    change_column_name('Original release', 'Release date')
    change_column_name('Music by', 'Composer(s)')
    change_column_name('Produced by', 'Producer(s)')
    change_column_name('Producer', 'Producer(s)')
    change_column_name('Productioncompanies ', 'Production company(s)')
    change_column_name('Productioncompany ', 'Production company(s)')
    change_column_name('Released', 'Release Date')
    change_column_name('Release Date', 'Release date')
    change_column_name('Screen story by', 'Writer(s)')
    change_column_name('Screenplay by', 'Writer(s)')
    change_column_name('Story by', 'Writer(s)')
    change_column_name('Theme music composer', 'Composer(s)')
    change_column_name('Written by', 'Writer(s)')
    
    return movie

# Create list comprehension on clean movies.
clean_movies = [clean_movie(movie) for movie in wiki_movies]

# Set wiki movies DataFrame to the DataFrame created from clean movies.
wiki_movies_df = pd.DataFrame(clean_movies)

# Turn the extracted values to numeric values.
def parse_dollars(s):
    if type(s) != str:
        return np.nan
    # If input is in form of $###.# million
    if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
        s = re.sub('\$|\s|[a-zA-Z]','', s)
        value = float(s) * 10**6
        return value
    # If input is of the form $###.# billion
    elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
        s = re.sub('\$|\s|[a-zA-Z]','', s)
        value = float(s) * 10**9
        return value
    # If input is of the form $###,###,###
    elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
        s = re.sub('\$|,','', s)
        value = float(s)
        return value
    # Otherwise, return NaN.
    else:
        return np.nan

# Challenge - Create ETL function that takes in 3 arguments: Wikipedia data, Kaggle metadata and MovieLens rating data.
def etl(wiki_data, kaggle, rating):
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_data = pd.DataFrame(clean_movies)
    # Drop columns with mostly null values.
    wiki_data = wiki_data[[column for column in wiki_data.columns if wiki_data[column].isnull().sum() < len(wiki_data) * 0.9]]
    # Correct values in IMDB data column.
    wiki_data['imdb_id'] = wiki_data['imdb_link'].str.extract(r'(tt\d{7})')
    wiki_data.drop_duplicates(subset='imdb_id', inplace=True)

    # Convert all data types in wiki data.
    box_office = wiki_data['Box office'].dropna()
    box_office[box_office.map(lambda x: type(x) != str)]
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    wiki_data.drop('Box office', axis=1, inplace=True)
    wiki_data['Box office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

    # Convert budget data.
    budget = wiki_data['Budget'].dropna()
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    budget = budget.str.replace(r'\[\d+\]\s*', '')
    wiki_data.drop('Budget', axis=1, inplace=True)
    wiki_data['Budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

    # Alter release data.
    release_date = wiki_data['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    wiki_data['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)

    # Inspect running time data.
    running_time = wiki_data['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    wiki_data['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    wiki_data.drop('Running time', axis=1, inplace=True)

    # Execute 'to numeric' and 'to datetime' functions.
    kaggle = kaggle[kaggle['adult'] == 'False'].drop('adult',axis='columns')
    kaggle['budget'] = kaggle['budget'].astype(int)
    kaggle['id'] = pd.to_numeric(kaggle['id'], errors='raise')
    kaggle['popularity'] = pd.to_numeric(kaggle['popularity'], errors='raise')
    kaggle['release_date'] = pd.to_datetime(kaggle['release_date'])
    rating['timestamp'] = pd.to_datetime(rating['timestamp'], unit='s')

    # Merge datasets.
    movies_df = pd.merge(wiki_data, kaggle, on='imdb_id', suffixes=['_wiki','_kaggle'])

    # Drop unnecessary columns.
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)', 'video'], inplace=True)

    # Create function to replace values.
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)
        df.drop(columns=wiki_column, inplace=True)

    # Run missing value functions.
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget', 'Budget')
    fill_missing_kaggle_data(movies_df, 'revenue', 'Box office')

    #Formatting
    # Format column names.
    movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]
    movies_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns', inplace=True)
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                .rename({'userId':'count'}, axis=1) \
                .pivot(index='movieId',columns='rating', values='count')
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0) 

    # Import movies database to SQL.
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    engine = create_engine(db_string)
    movies_df.to_sql(name='movies', con=engine)

    # Import ratings to SQL.
    rows_imported = 0

    # Get the start_time from time.time()
    start_time = time.time()
    for data in pd.read_csv(f'{file_dir}/ratings.csv', chunksize=1000000):
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        data.to_sql(name='ratings', con=engine, if_exists='append')
        rows_imported += len(data)

        # Add elapsed time to final print out
        print(f'Done. {time.time() - start_time} total seconds elapsed')
    return wiki_data, movies_df, movies_with_ratings_df
 
print(etl(wiki_movies_df, kaggle_metadata, ratings))