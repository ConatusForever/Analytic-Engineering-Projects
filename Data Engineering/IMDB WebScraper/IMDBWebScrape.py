# importing libraries

import pandas as pd, requests, json, re, datetime, pypyodbc, sys, sql_server_info
from sqlalchemy import create_engine,text
from bs4 import BeautifulSoup

movie_ids = []
movie_ranks = []
movie_titles = []
movie_years = []
movie_ratings = []
movie_durations = []
movie_genres = []
movie_imdb_ratings = []
movie_metascores = []
movie_votes = []
imdb_cols = [movie_ids, movie_ranks, movie_titles, movie_years, movie_ratings, movie_durations, movie_genres, movie_imdb_ratings, movie_metascores, movie_votes]


today = datetime.date.today()
url = [f'https://www.imdb.com/search/title/?title_type=feature&year=2023-01-01,{today}&start={i:d}&ref_=adv_nxt'  for i in (range(1, 5394, 50))]

def get_imdb_data(url):
    ''' This function takes in a url and gets 
    the imdb data from the url'''


    for i in url:
        response = requests.get(i)
        soup = BeautifulSoup(response.text, 'html.parser')
        item_list = soup.find_all('div', class_='lister-item mode-advanced')


        for i in item_list:
            
            movie_id = i.find_all('a')[0].get('href')       #1 get movie ids
            if len(movie_id) == 0:
                movie_id = 'Missing'
            else:
                movie_id = movie_id.split('/')[2]
            movie_ids.append(movie_id)
                
            rank = i.find_all('span', class_='lister-item-index unbold text-primary')  #2 get movie rank
            if len(rank) == 0:
                rank = 'Missing'
            
            else:
                rank = rank[0].text.strip('.')
            movie_ranks.append(rank)

            title = i.find_all('a')[1]      #3 get movie title
            if len(title) == 0:
                title = 'Missing'
            else:
                title = title.text
            movie_titles.append(title)

            year = i.find_all('span', class_='lister-item-year text-muted unbold')      #4 get movie year
            if len(year) == 0:
                year = 'Missing'
            
            else:
                year = year[0].text.strip('()')
                year = re.sub('[^0-9]', '', year)
            movie_years.append(year)

            rating = i.find_all('span', class_ ='certificate')  #5 get rating from paragraph
            if len(rating) == 0:
                rating = 'Missing'
            else:
                rating = rating[0].text
            movie_ratings.append(rating)

            runtime = i.find('span', class_='runtime')    #6 get runtime from paragraph
            if runtime is None:
                runtime = 'Missing'
            else:
                runtime = runtime.text.strip(' min')
            movie_durations.append(runtime)

            try:
                genre = i.find_all('p', class_ = 'text-muted')    #7 get genre from paragraph
                if len(genre) == 0:
                    genre = 'Missing'
                else:
                    genre = genre[0].find_all('span', class_ ='genre')[0].text.strip()
            except:
                genre = 'Missing'
            movie_genres.append(genre)
            
            imdb_ratings = i.find_all('div',class_='inline-block ratings-imdb-rating')        #8 get imdb rating
            if len(imdb_ratings) == 0:
                imdb_ratings = 'Missing'
            else:
                imdb_ratings = imdb_ratings[0].find_all('strong')[0].text
            movie_imdb_ratings.append(imdb_ratings)

            metascores = i.find_all('div',class_='inline-block ratings-metascore')  #9 get metascore
            if len(metascores) == 0:
                metascores = 'Missing'
            else:
                metascores = metascores[0].find_all('span')[0].text
            movie_metascores.append(metascores)
            
            try:
                votes = i.find_all('p', class_='sort-num_votes-visible')        #10 get voting info
                if  len(votes) == 0:
                    votes = 'Missing'
                else:
                    votes = votes[0].find_all('span',attrs={'name':'nv'})[0].text
            except:
                votes = 'Missing'
            movie_votes.append(votes)

print('Getting IMDB data...')
get_imdb_data(url)
print('Done getting IMDB data.')



def create_imdb():
    ''' This function creates a dictionary of all the movies in the IMDB website.'''  
    
    movie_dict = {}

    # Adding all the lists to the dictionary

    movie_dict['movie_id'] = movie_ids
    movie_dict['movie_rank'] = movie_ranks
    movie_dict['movie_title'] = movie_titles
    movie_dict['movie_year'] = movie_years
    movie_dict['movie_rating'] = movie_ratings
    movie_dict['movie_duration'] = movie_durations
    movie_dict['movie_genre'] = movie_genres
    movie_dict['movie_imdb_rating'] = movie_imdb_ratings
    movie_dict['movie_metascore'] = movie_metascores
    movie_dict['movie_votes'] = movie_votes
    
    return pd.DataFrame(movie_dict) # Creating a dataframe from the dictionary
print('Creating IMDB dataframe...')
imdb_df = create_imdb()
print('Done creating IMDB dataframe.')



def write_to_db():
    ''' This funciton writes the dataframe 
    to my sql server database.'''

        
    driver = sql_server_info.driver
    datasource = sql_server_info.datasource
    server_name = sql_server_info.server_name
    database_name = 'IMDB'


    conn_string = f'mssql+pyodbc://@{server_name}/{database_name}?driver={driver}'
    engine = create_engine(conn_string)
    conn = engine.connect()
    imdb_df.to_sql('IMDB', con = conn, if_exists='append', index=False)

print('Writing to database...')
write_to_db()

print('Done writing to database.')