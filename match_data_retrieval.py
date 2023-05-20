import itertools
from web_scraper_fifa import *
import requests
from datetime import datetime
import pandas as pd
import os
from dotenv import load_dotenv
import sqlite3

# Load environment variables from .env file
load_dotenv()

# Get API key from environment variable
API_KEY = os.getenv('API_KEY')

# Use the API key in the headers for the requests module
HEADERS = {'X-RapidAPI-Key': f'{API_KEY}',
           "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"      
          }

BASE_URL = "https://api-football-v1.p.rapidapi.com/v3/"
URL_STATISTICS = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"
URL_LINEUPS = "https://api-football-v1.p.rapidapi.com/v3/fixtures/lineups"

NAME_DB = "football_database"


def connect_db():
    global conn 
    conn = sqlite3.connect(f"Data/{NAME_DB}.db")

def close_db():
    global conn
    conn.close()
    

def get_all_live_matches():
    response = requests.get(BASE_URL + "fixtures?live=all", headers=HEADERS).json()
    live_matches = response["response"]

    return live_matches


def retrieve_competition_season_data(competition_name, competition_api_id, season):
    connect_db()

    response = requests.get(BASE_URL + f"fixtures?league={competition_api_id}&season={season}&timezone=Europe/Berlin", headers=HEADERS).json()
    json_matches = response["response"]

    fixture_ids = save_matches_to_db(json_matches, season, competition_name)
    process_fixtures(fixture_ids)
    
    close_db()


def save_matches_to_db(json_matches, season, competition_name):    
    global conn
    c = conn.cursor()

    c.execute(f'''
                CREATE TABLE IF NOT EXISTS Matches 
                (
                Fixture_ID INTEGER PRIMARY KEY,
                Competition_Name TEXT,
                Season INTEGER,
                Home_Team TEXT,
                Away_Team TEXT,
                Home_Goals INTEGER,
                Away_Goals INTEGER,
                Match_Date TEXT,
                Match_Time TEXT,
                Match_Status TEXT
                )
            ''')
    fixture_ids = []

    # loop through each match and extract the relevant data
    for match in json_matches:
        print(match)
        fixture_id = match["fixture"]["id"]
        home_team = match["teams"]["home"]["name"]
        away_team = match["teams"]["away"]["name"]
        home_goals = match["goals"]["home"]
        away_goals = match["goals"]["away"]
        match_status = match["fixture"]["status"]["short"]
        match_date = match["fixture"]["date"]

        # Split the match_date string into date and time separately and convert date to format %d.%m.%Y
        datetime_obj = datetime.fromisoformat(match_date.replace("Z", "+00:00"))
        match_date = datetime_obj.strftime('%d.%m.%Y')
        match_time = datetime_obj.strftime('%H:%M')

        # if match has not been played yet or data is unavailable for other reasons, skip it
        if(home_goals is None and away_goals is None):
            continue

        # Insert the retrieved data into the database
        c.execute(f'''
            INSERT OR IGNORE INTO Matches (Fixture_ID, Competition_Name, Season, Home_Team, Away_Team, Home_Goals, Away_Goals, Match_Date, Match_Time, Match_Status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (fixture_id, competition_name, season, home_team, away_team, home_goals, away_goals, match_date, match_time, match_status))

        fixture_ids.append(fixture_id)

    conn.commit()

    return fixture_ids


def process_fixtures(fixture_ids):
    for fixture_id in fixture_ids:
        querystring = {"fixture" : fixture_id}
        statistics_response = requests.request("GET", URL_STATISTICS, headers=HEADERS, params=querystring).json()
        statistics = statistics_response["response"]

        # if the api has statistics data regarding the fixture id, preprocess and save it to the database
        if (len(statistics) != 0):
            stat_types, stat_values = extract_statistics(statistics)
            save_match_statistics_to_db(fixture_id, stat_types, stat_values)
        save_match_lineups_to_db(fixture_id)
        return 


def extract_statistics(statistics):
    try:
        home_stats = statistics[0]["statistics"]
        away_stats = statistics[1]["statistics"]

        # Prepare the stats for SQL
        # Convert strings to match python and sql conventions (e.g., replace the special "%" character)
        stat_types = [f"home_{stat['type'].lower().replace(' ', '_').replace('%', 'percent')}" for stat in home_stats]
        stat_types += [f"away_{stat['type'].lower().replace(' ', '_').replace('%', 'percent')}" for stat in away_stats]
        
        # Convert stat_values to appropriate format for SQL
        stat_values = []
        for stat in home_stats + away_stats:
            value = stat['value']
            if isinstance(value, str) and '%' in value:
                value = float(value.strip('%')) / 100  # convert percentage to decimal
            elif value is None:
                value = 0  
            else:
                value = float(value)  # convert other numeric values to float
            stat_values.append(value)

        return stat_types, stat_values
    
    except Exception as e:
        print(e)



def save_match_statistics_to_db(fixture_id, stat_types, stat_values):
    global conn
    c = conn.cursor()

    fixture_id = str(fixture_id)

    # Create table if it doesn't exist already
    columns_definition = ', '.join([f'{stat} REAL' for stat in stat_types])
    query_create_table = f'''
                        CREATE TABLE IF NOT EXISTS Statistics (
                        fixture_id STRING PRIMARY KEY,
                        {columns_definition}
                        )
                        '''

    c.execute(query_create_table)

    # Insert the statistics
    placeholders = ', '.join('?' * len(stat_values))
    query_insert = f'''
                    INSERT OR IGNORE INTO Statistics(fixture_id, {', '.join(stat_types)})
                    VALUES (?, {placeholders})
                    '''
    c.execute(query_insert, [fixture_id] + stat_values)
    conn.commit()


def save_match_lineups_to_db(fixture_id): 
    global conn
    c = conn.cursor()

    fixture_id = str(fixture_id)
    querystring = {"fixture" : fixture_id}

    response = requests.request("GET", URL_LINEUPS, headers=HEADERS, params=querystring).json()
    lineups = response["response"]

    # extract the players who played in the match for each team
    home_players = [player["player"]["name"] for player in lineups[0]["startXI"]]
    away_players = [player["player"]["name"] for player in lineups[1]["startXI"]]

    home_subs = [player["player"]["name"] for player in lineups[0]["substitutes"]]
    away_subs = [player["player"]["name"] for player in lineups[1]["substitutes"]]

    # create Players table with starting lineups
    starting_players_columns_home =  ", ".join([f"home_player_{i} TEXT" for i in range(1, len(home_players) + 1)])
    starting_players_columns_away =  ", ".join([f"away_player_{i} TEXT" for i in range(1, len(away_players) + 1)])

    all_player_columns = starting_players_columns_home + ", " + starting_players_columns_away

    query_create_table_players = f'''
                        CREATE TABLE IF NOT EXISTS Players (
                        fixture_id STRING PRIMARY KEY,
                        {all_player_columns}
                        )
                        '''
    c.execute(query_create_table_players)

    lineup_data_players = home_players + away_players
    placeholders_players = ', '.join(['?'] * len(lineup_data_players))

    all_player_columns_insert_query = starting_players_columns_home + ", " + starting_players_columns_away

    query_insert_players = f'''
                    INSERT OR IGNORE INTO Players (fixture_id, {all_player_columns_insert_query})
                    VALUES (?, {placeholders_players})
                    '''

    c.execute(query_insert_players, [fixture_id] + lineup_data_players)

    # create Substitutes table
    query_create_table_subs = '''
                        CREATE TABLE IF NOT EXISTS Substitutes (
                        id INTEGER PRIMARY KEY,
                        fixture_id STRING,
                        team TEXT,
                        substitute_name TEXT
                        )
                        '''
    c.execute(query_create_table_subs)

    # insert home substitutes
    for sub in home_subs:
        query_insert_home_subs = '''
                            INSERT INTO Substitutes (fixture_id, team, substitute_name)
                            VALUES (?, ?, ?)
                            '''
        c.execute(query_insert_home_subs, [fixture_id, "home", sub])

    # insert away substitutes
    for sub in away_subs:
        query_insert_away_subs = '''
                            INSERT INTO Substitutes (fixture_id, team, substitute_name)
                            VALUES (?, ?, ?)
                            '''
        c.execute(query_insert_away_subs, [fixture_id, "away", sub])

    conn.commit()