################################################################################
# Description: This DAG is used to ingest raw data from Reddit API and store it in Supabase
# Author:      Ilyes DJERFAF
# Date:        2024-01-14
# Reference:   https://praw.readthedocs.io/en/latest/getting_started/quick_start.html
# ################################################################################

################################################################################
############################## Imports #########################################
################################################################################
from datetime import datetime
from airflow.models import DAG
from pandas import DataFrame
import praw
from supabase import create_client, Client
import pandas as pd


################################################################################
############################## DAG Arguments ###################################
################################################################################

def _job():
    from datetime import datetime
    from airflow.models import DAG
    from pandas import DataFrame
    import praw
    from supabase import create_client, Client
    import pandas as pd

    # authentification
    username = "ml-engineer-id"
    password = "YesWeCan!2024"
    app_client_ID = "-op8tbEm4Lru11GOWHaGvQ"
    app_client_secret = "O7xLPfX-yV1hUyxeQcAvvLaqVx0luA"
    user_agent = "python:ETLApp:v1.1 (by /u/ml-engineer-id)"

    # reddit praw object
    reddit = praw.Reddit(
        client_id=app_client_ID,
        client_secret=app_client_secret,
        password=password,
        user_agent=user_agent,
        username=username,
    )

    # supabase object
    url = "https://sfvirdoealfpgazvpxzt.supabase.co"
    key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNmdmlyZG9lYWxmcGdhenZweHp0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDM4NjI0MjcsImV4cCI6MjAxOTQzODQyN30.Ck39HXTtENYM_m5QjuVP_SR6gt9ipP9YUOBKalYOqrI"
    supabase: Client = create_client(url, key)

    # Create a random user login email and password.
    random_email: str = "machinelearning.engineer.id@gmail.com"
    random_password: str = "YesWeCan!2024"
    user = supabase.auth.sign_up({"email": random_email, "password": random_password})
    user = supabase.auth.sign_in_with_password(
        {"email": random_email, "password": random_password}
    )

    # European countries
    european_countries = {
        "United Kingdom": "unitedkingdom",
        "Ukraine": "Ukrainian",
        "Turkey": "Turkey",
        "Switzerland": "switzerland",
        "Sweden": "sweden",
        "Spain": "es",
        "Slovenia": "Slovenia",
        "Slovakia": "Slovakia",
        "Serbia": "serbia",
        "Romania": "Romania",
        "Portugal": "portugal",
        "Poland": "poland",
        "Norway": "norway",
        "Netherlands": "TheNetherlands",
        "Republic of Moldova": "moldova",
        "Malta": "malta",
        "TFYR Macedonia": "macedonia",
        "Luxembourg": "Luxembourg",
        "Lithuania": "lithuania",
        "Latvia": "Latvia",
        "Italy": "it",
        "Ireland": "ireland",
        "Iceland": "Iceland",
        "Hungary": "hungary",
        "Greece": "greece",
        "Germany": "Germany",
        "France": "france",
        "Finland": "Finland",
        "Estonia": "eesti",
        "Denmark": "Denmark",
        "Czechia": "czech",
        "Cyprus": "cyprus",
        "Croatia": "croatia",
        "Bulgaria": "bulgaria",
        "Bosnia and Herzegovina": "bih",
        "Belgium": "belgium",
        "Belarus": "belarus",
        "Austria": "austria",
        "Armenia": "armenian",
    }

    # Drop the table if it exists
    data, count = supabase.table("hot_posts").delete().gte("id", 0).execute()

    # Create the table of raw data
    for country in european_countries:
        subreddit = reddit.subreddit(european_countries[country])
        # for each country, we are going to extract the hot 10 topics
        for submission in subreddit.hot(limit=10):
            # delete all moreComments
            submission.comments.replace_more(limit=0)
            top_level_comments = list(
                submission.comments
            )  # to have the first level comments only
            # fill the table
            data = (
                supabase.table("hot_posts")
                .insert(
                    {
                        "country": str(country),
                        "title": str(submission.title),
                        "num_comments": int(submission.num_comments),
                        "score": int(submission.score),
                        "saved": submission.saved,
                        "over_18": submission.over_18,
                        "is_original_content": submission.is_original_content,
                        "upvote_ratio": submission.upvote_ratio,
                    }
                )
                .execute()
            )



    _table = supabase.table('hot_posts').select("*").execute()
    fetch = True
    for param in _table:
        if fetch:
            _data = param
            fetch = False

    data = _data[1]
    dataset = pd.DataFrame(data)

    bool_columns = ["over_18", "is_original_content"]
    count_columns = ["num_comments", "score"]
    avg_columns = ["num_comments",	"score", "upvote_ratio"]

    for col in bool_columns:
        dataset[f"count_{col}"] = dataset.groupby('country')[col].transform(lambda x: x.sum())

    for col in count_columns:
        dataset[f"count_{col}"] = dataset.groupby('country')[col].transform('sum')
    for col in avg_columns:
        dataset[f"avg_{col}"] = dataset.groupby('country')[col].transform('mean')

    # Charger le fichier CSV capital_monde_longitude_latitude
    df_capital = pd.read_csv('..\include\dataset\country-capital-lat-long-population.csv')
    df_capital.rename(columns={"Country" : "country"}, inplace=True)
    # Fusionner les deux DataFrames sur la colonne 'country'
    df_out = pd.merge(dataset, df_capital, how='left', on='country')
    # Convert the datetime column to datetime type
    df_out['time'] = pd.to_datetime(df_out['time'], errors='coerce')
    # Create a new column with the formatted date 'year_month_day'
    df_out['time'] = df_out['time'].dt.strftime('%Y-%m-%d')
    usefull_columns = ['time', 'country', 'Capital City', 'Latitude', 'Longitude', 'Population',
                   'count_over_18', 'count_is_original_content', 'count_num_comments', 'count_score',
                   'avg_num_comments', 'avg_score','avg_upvote_ratio']
    df_out = df_out[usefull_columns].drop_duplicates().reset_index()
    print("done")

def _done():
    print("done v2")

from airflow.operators.python import PythonOperator

with DAG(dag_id='reddit_analysis',
         start_date=datetime(2024, 1, 14),
         schedule='@daily',
         catchup=False) as dag:
    
    job = PythonOperator(
        task_id="job",
        python_callable=_job
    )

    done = PythonOperator(
        task_id="done",
        python_callable=_done
    )

    job >> done
