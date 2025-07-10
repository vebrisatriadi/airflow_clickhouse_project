# dags/dag_02_etl_tmdb_to_dwh.py (Final Lengkap)
import os
import json
import pandas as pd
import pendulum
import clickhouse_connect
import glob
from datetime import datetime

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

def get_clickhouse_client():
    conn = BaseHook.get_connection('clickhouse_manual')
    return clickhouse_connect.get_client(host=conn.host, port=conn.port, user=conn.login, password=conn.password)

@dag(dag_id='etl_tmdb_to_dwh', start_date=pendulum.datetime(2025, 7, 10, tz="Asia/Jakarta"), schedule=None, catchup=False)
def etl_tmdb_to_dwh_dag_complete():
    @task
    def create_dwh_tables():
        client = get_clickhouse_client()
        SQLS = [
            "CREATE TABLE IF NOT EXISTS default.fact_movies ( movie_id UInt64, title String, release_date Nullable(Date32), revenue UInt64, budget UInt64, popularity Float32, vote_average Float32, vote_count UInt32, runtime Nullable(UInt16) ) ENGINE = MergeTree() ORDER BY movie_id;",
            "CREATE TABLE IF NOT EXISTS default.dim_genres ( genre_id UInt32, genre_name String ) ENGINE = MergeTree() ORDER BY genre_id;",
            "CREATE TABLE IF NOT EXISTS default.dim_production_companies ( company_id UInt64, company_name String ) ENGINE = MergeTree() ORDER BY company_id;",
            "CREATE TABLE IF NOT EXISTS default.bridge_movie_genres ( movie_id UInt64, genre_id UInt32 ) ENGINE = MergeTree() ORDER BY (movie_id, genre_id);",
            "CREATE TABLE IF NOT EXISTS default.bridge_movie_companies ( movie_id UInt64, company_id UInt64 ) ENGINE = MergeTree() ORDER BY (movie_id, company_id);"
        ]
        for sql in SQLS: client.command(sql)
        print("All DWH tables are ready.")
        return True

    @task
    def extract_transform_load(tables_ready: bool):
        if not tables_ready: raise ValueError("DWH tables are not ready.")
        json_files = glob.glob(os.path.join('/opt/airflow/data', '**', '*.json'), recursive=True)
        
        all_entries, all_genres, all_companies, all_entry_genres, all_entry_companies = [], [], [], [], []
        print(f"Found {len(json_files)} files. Processing up to 3000 files.")

        for file_path in json_files[:3000]:
            try:
                with open(file_path, 'r') as f: data = json.load(f)
                if data.get('id') and (data.get('title') or data.get('name')):
                    # Extract data
                    entry_id = data['id']
                    title = data.get('title') or data.get('name')
                    raw_date = data.get('release_date') or data.get('first_air_date')
                    release_date_obj = None
                    if raw_date:
                        try:
                            release_date_obj = datetime.strptime(raw_date, '%Y-%m-%d').date()
                        except (ValueError, TypeError):
                            pass
                    
                    # Append to lists
                    all_entries.append({'movie_id': entry_id, 
                                        'title': title, 
                                        'release_date': release_date_obj, 
                                        'revenue': data.get('revenue', 0), 
                                        'budget': data.get('budget', 0), 
                                        'popularity': data.get('popularity', 0.0), 
                                        'vote_average': data.get('vote_average', 0.0), 
                                        'vote_count': data.get('vote_count', 0), 
                                        'runtime': data.get('runtime')}
                    )
                    for g in data.get('genres', []):
                        all_genres.append(g)
                        all_entry_genres.append({'movie_id': entry_id, 'genre_id': g['id']})
                    for c in data.get('production_companies', []):
                        all_companies.append(c)
                        all_entry_companies.append({'movie_id': entry_id, 'company_id': c['id']})
            except Exception as e:
                print(f"Skipping file {file_path} due to error: {e}")

        if not all_entries:
            print("No valid entries found to load. Exiting.")
            return

        client = get_clickhouse_client()

        # 1. Load Genre Dimension
        if all_genres:
            dim_genres_df = pd.DataFrame(all_genres).drop_duplicates(subset=['id'])
            dim_genres_df.rename(columns={'id': 'genre_id', 'name': 'genre_name'}, inplace=True)
            final_genres_df = dim_genres_df[['genre_id', 'genre_name']]
            client.insert_df('default.dim_genres', final_genres_df)
            print(f"Loaded {len(final_genres_df)} unique rows into dim_genres.")

        # 2. Load Production Company Dimension
        if all_companies:
            dim_companies_df = pd.DataFrame(all_companies).drop_duplicates(subset=['id'])
            dim_companies_df.rename(columns={'id': 'company_id', 'name': 'company_name'}, inplace=True)
            final_companies_df = dim_companies_df[['company_id', 'company_name']]
            client.insert_df('default.dim_production_companies', final_companies_df)
            print(f"Loaded {len(final_companies_df)} unique rows into dim_production_companies.")
            
        # 3. Load Bridge Table
        if all_entry_genres:
            bridge_genres_df = pd.DataFrame(all_entry_genres).drop_duplicates()
            client.insert_df('default.bridge_movie_genres', bridge_genres_df)
            print(f"Loaded {len(bridge_genres_df)} rows into bridge_movie_genres.")

        if all_entry_companies:
            bridge_companies_df = pd.DataFrame(all_entry_companies).drop_duplicates()
            client.insert_df('default.bridge_movie_companies', bridge_companies_df)
            print(f"Loaded {len(bridge_companies_df)} rows into bridge_movie_companies.")

        # 4. Load Main Fact Table
        entries_df = pd.DataFrame(all_entries).drop_duplicates(subset=['movie_id'])
        client.insert_df('default.fact_movies', entries_df)
        print(f"Loaded {len(entries_df)} rows into fact_movies.")

    create_dwh_tables() >> extract_transform_load(tables_ready=True)

etl_tmdb_to_dwh_dag_complete()