import itertools
from web_scraper_fifa import *
import requests
from datetime import datetime
import pandas as pd
import os
from dotenv import load_dotenv
from db_manager import *

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
URL_EVENTS = f"https://api-football-v1.p.rapidapi.com/v3/fixtures/events"


def get_all_live_matches():
    try:
        response = requests.get(BASE_URL + "fixtures?live=all", headers=HEADERS).json()
    except requests.exceptions.RequestException as e:
        print(f"Exception occured when requesting from API: {e}")
    
    live_matches = response["response"]

    return live_matches



def retrieve_competition_season_data(competition_name, competition_api_id, season):
    global conn
    conn = connect_db()

    try:
        response = requests.get(BASE_URL + f"fixtures?league={competition_api_id}&season={season}&timezone=Europe/Berlin", headers=HEADERS).json()
    except requests.exceptions.RequestException as e:
        print(f"Exception occured when requesting from API: {e}")
        return
    
    json_matches = response["response"]

    fixture_ids = save_matches_to_db(json_matches, season, competition_name)
    process_fixtures(fixture_ids)
    
    close_db(conn)


def create_matches_table(db_cursor):  
    db_cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS Matches 
                (
                fixture_id TEXT PRIMARY KEY,
                competition_name TEXT,
                season INTEGER,
                home_team TEXT,
                away_team TEXT,
                home_goals INTEGER,
                away_goals INTEGER,
                match_date TEXT,
                match_time TEXT,
                match_status TEXT,
                home_team_formation TEXT,
                away_team_formation TEXT
                )
            ''')
    global conn
    conn.commit()
    
def create_statistics_table(db_cursor, names_stats):
    query_create_table = f'''
                    CREATE TABLE IF NOT EXISTS Statistics (
                    fixture_id STRING PRIMARY KEY,
                    {names_stats},
                    FOREIGN KEY (fixture_id) REFERENCES Matches(fixture_id)
                    )
                    '''

    db_cursor.execute(query_create_table)
    global conn
    conn.commit()


def create_substitute_table(db_cursor):
    query_create_table_subs = '''
                        CREATE TABLE IF NOT EXISTS Substitutes (
                        id INTEGER PRIMARY KEY,
                        fixture_id TEXT,
                        team TEXT,
                        player_subbed_off TEXT,
                        player_subbed_in TEXT,
                        player_id_subbed_off TEXT,
                        player_id_subbed_in TEXT,
                        minute_subbed_in INTEGER,
                        FOREIGN KEY (fixture_id) REFERENCES Matches(fixture_id)
                        )
                        '''
    db_cursor.execute(query_create_table_subs)
    global conn
    conn.commit()


def create_startingXI_table(db_cursor):
    query_create_table_players = '''
                        CREATE TABLE IF NOT EXISTS Players (
                        id INTEGER PRIMARY KEY,
                        fixture_id TEXT,
                        player_name TEXT,
                        player_id TEXT,
                        position TEXT,
                        position_on_grid TEXT,
                        team TEXT,
                        FOREIGN KEY(fixture_id) REFERENCES Matches(fixture_id)
                        )
                        '''
    db_cursor.execute(query_create_table_players)

def save_matches_to_db(json_matches, season, competition_name):    
    global conn
    db_cursor = conn.cursor()

    create_matches_table(db_cursor)

    fixture_ids = []

    # loop through each match and extract the relevant data
    for match in json_matches:
        fixture_id = str(match["fixture"]["id"])
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
        if (home_goals is None and away_goals is None):
            continue

        # Insert the retrieved data into the database
        db_cursor.execute(f'''
            INSERT OR IGNORE INTO Matches (Fixture_ID, Competition_Name, Season, Home_Team, Away_Team, Home_Goals, Away_Goals, Match_Date, Match_Time, Match_Status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (fixture_id, competition_name, season, home_team, away_team, home_goals, away_goals, match_date, match_time, match_status))

        fixture_ids.append(fixture_id)

    conn.commit()

    return fixture_ids


def process_fixtures(fixture_ids):
    count = 0
    for fixture_id in fixture_ids:
        querystring = {"fixture" : fixture_id}
        try:
            statistics_response = requests.request("GET", URL_STATISTICS, headers=HEADERS, params=querystring).json()
        except requests.exceptions.RequestException as e:
            print(f"Exception occured when requesting from API: {e}")

        statistics = statistics_response["response"]
        # if the api has statistics data regarding the fixture id, preprocess and save it to the database
        if (len(statistics) != 0):
            stat_types, stat_values = extract_statistics(statistics)
            save_match_statistics_to_db(fixture_id, stat_types, stat_values)
        save_match_lineups_to_db(fixture_id)
        count += 1

        if(count == 20):
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


def update_statistic_columns(db_cursor, stat_types):
    db_cursor.execute("PRAGMA table_info(Statistics)")
    existing_columns = [column[1] for column in db_cursor.fetchall()]  

    new_columns = [stat for stat in stat_types if stat not in existing_columns]

    if new_columns:
        for column in new_columns:
            db_cursor.execute(f"ALTER TABLE Statistics ADD COLUMN {column} REAL")


def save_match_statistics_to_db(fixture_id, stat_types, stat_values):
    global conn
    db_cursor = conn.cursor()

    fixture_id = str(fixture_id)

    # Create table if it doesn't exist already
    stats_columns = ', '.join([f'{stat} REAL' for stat in stat_types])
    create_statistics_table(db_cursor, stats_columns)

    # Add new stat columns to the table if api has introduced new stats for new seasons
    update_statistic_columns(db_cursor, stat_types)

    # Insert the statistics
    placeholders = ', '.join('?' * len(stat_values))
    query_insert = f'''
                    INSERT OR IGNORE INTO Statistics(fixture_id, {', '.join(stat_types)})
                    VALUES (?, {placeholders})
                    '''
    db_cursor.execute(query_insert, [fixture_id] + stat_values)

    conn.commit()


def save_match_lineups_to_db(fixture_id): 
    fixture_id = str(fixture_id)
    querystring = {"fixture" : fixture_id}

    try:
        response = requests.request("GET", URL_LINEUPS, headers=HEADERS, params=querystring).json()
    except requests.exceptions.RequestException as e:
        print(f"Exception occured when requesting from API: {e}")
        return
    
    lineups = response["response"]

    save_formations_to_matches_table(lineups, fixture_id)
    save_startingXI_to_db(lineups, fixture_id)
    save_substitutes_to_db(fixture_id)


def save_formations_to_matches_table(lineups, fixture_id):
    global conn

    home_formation = lineups[0]["formation"]
    away_formation = lineups[1]["formation"]
    # Update the formations in the Matches table
    db_cursor = conn.cursor()
    db_cursor.execute('''
        UPDATE Matches 
        SET home_team_formation = ?, away_team_formation = ? 
        WHERE fixture_id = ?
    ''', (home_formation, away_formation, fixture_id))
    conn.commit()


def save_startingXI_to_db(lineups, fixture_id):
    global conn

    db_cursor = conn.cursor()

    create_startingXI_table(db_cursor)

    # check if fixture_id already exists
    db_cursor.execute('SELECT * FROM Players WHERE fixture_id=?', (fixture_id,))
    fixture_data = db_cursor.fetchone()
    # if fixture is already present in the table, do not insert it again
    if fixture_data is not None:
        return

    # extract the players who played in the match for each team
    for i, team in enumerate(["home", "away"]):
        for player in lineups[i]["startXI"]:
            player_id = player["player"]["id"]
            player_name = player["player"]["name"]
            position = player["player"]["pos"]
            position_on_grid = player["player"]["grid"]

            query_insert_player = '''
                            INSERT OR IGNORE INTO Players (fixture_id, player_name, player_id, position, position_on_grid, team)
                            VALUES (?, ?, ?, ?, ?, ?)
                            '''
            db_cursor.execute(query_insert_player, [fixture_id, player_name, player_id, position, position_on_grid, team])

    conn.commit()


def save_substitutes_to_db(fixture_id):
    global conn
    db_cursor = conn.cursor()

    create_substitute_table(db_cursor)

    # check if fixture_id already exists
    db_cursor.execute('SELECT * FROM Substitutes WHERE fixture_id=?', (fixture_id,))
    fixture_data = db_cursor.fetchone()
    # if fixture is already present in the table, do not insert it again
    if fixture_data is not None:
        return

    querystring = {"fixture" : fixture_id}

    try:
        response =  requests.request("GET", URL_EVENTS, headers=HEADERS, params=querystring).json()
    except requests.exceptions.RequestException as e:
        print(f"Exception occured: {e}")
        return

    for event in response["response"]:
        if (event['type'] == 'subst'):
            player_subbed_off = event["player"]["name"]
            player_subbed_in = event["assist"]["name"]
            player_id_subbed_off = event["player"]["id"]
            player_id_subbed_in = event["assist"]["id"]
            team_name = event["team"]["name"]
            time_of_sub = event["time"]["elapsed"]
            query_insert_home_subs = '''
                            INSERT OR IGNORE INTO Substitutes (fixture_id, team, player_subbed_off, player_subbed_in, player_id_subbed_off, player_id_subbed_in, minute_subbed_in)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            '''
            db_cursor.execute(query_insert_home_subs, [fixture_id, team_name, player_subbed_off, player_subbed_in, player_id_subbed_off, player_id_subbed_in, time_of_sub])

    conn.commit()