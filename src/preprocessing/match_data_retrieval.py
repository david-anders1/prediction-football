import itertools
from web_scraper_fifa import *
import requests
from datetime import datetime
import pandas as pd
import os
from dotenv import load_dotenv
from db.db_manager import *

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
URL_PLAYERS = f"https://api-football-v1.p.rapidapi.com/v3/players/"



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

    # collect all players that played in this season 
    # to get the player stats in the national and internation competitions after collecting all the match data of the season
    global all_players_in_season
    all_players_in_season = set()

    fixture_ids = save_matches_to_db(json_matches, season, competition_name)
    process_fixtures(fixture_ids)

    for player in all_players_in_season:
        if (is_player_already_in_table(player) == False):
            save_player(player, season)
        save_player_stats_for_season(player, season)
    
    close_db(conn)


def save_matches_to_db(json_matches, season, competition_name):    
    global conn
    db_cursor = conn.cursor()

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


def update_match_statistic_columns(db_cursor, stat_types):
    db_cursor.execute("PRAGMA table_info(Match_Statistics)")
    existing_columns = [column[1] for column in db_cursor.fetchall()]  
    new_columns = [stat for stat in stat_types if stat not in existing_columns]

    if new_columns:
        for column in new_columns:
            try:
                db_cursor.execute(f"ALTER TABLE Match_Statistics ADD COLUMN {column} REAL")
            except sqlite3.OperationalError as e:
                print(e)


def save_match_statistics_to_db(fixture_id, stat_types, stat_values):
    global conn
    db_cursor = conn.cursor()

    fixture_id = str(fixture_id)

    # Create table if it doesn't exist already
    stats_columns = ', '.join([f'{stat} REAL' for stat in stat_types])
    create_match_statistics_table(conn, stats_columns)

    # Add new stat columns to the table if api has introduced new stats for new seasons
    update_match_statistic_columns(db_cursor, stat_types)

    # Insert the statistics
    placeholders = ', '.join('?' * len(stat_values))
    query_insert = f'''
                    INSERT OR IGNORE INTO Match_Statistics(fixture_id, {', '.join(stat_types)})
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
    if (lineups):
        save_formations_to_matches_table(lineups, fixture_id)
        save_startingXI_to_db(lineups, fixture_id)
        save_substitutes_to_db(fixture_id)


# Update the formations in the Matches table
def save_formations_to_matches_table(lineups, fixture_id):
    try:
        home_formation = lineups[0]["formation"]
        away_formation = lineups[1]["formation"]
    except IndexError as e:
        return

    global conn
    db_cursor = conn.cursor()
    db_cursor.execute('''
        UPDATE Matches 
        SET home_team_formation = ?, away_team_formation = ? 
        WHERE fixture_id = ?
    ''', (home_formation, away_formation, fixture_id))
    conn.commit()


def is_player_already_in_table(player_id):
    global conn
    db_cursor = conn.cursor()

    # check if player_id already exists
    db_cursor.execute('SELECT * FROM Players WHERE player_id=?', (player_id,))
    fixture_data = db_cursor.fetchone()

    if fixture_data:
        return True
    else:
        return False
    

def save_player(player_id, season):
    querystring = {"id" : player_id, "season": season}

    try:
        response = requests.request("GET", URL_PLAYERS, headers=HEADERS, params=querystring).json()
    except requests.exceptions.RequestException as e:
        print(f"Exception occured when requesting from API: {e}")
        return

    try:
        player_data = response["response"][0]["player"]
    except IndexError as e:
        return
    
    player_data = response["response"][0]["player"]
    player_id = player_data["id"]
    first_name = player_data["firstname"]
    last_name = player_data["lastname"]
    nationality = player_data["nationality"]
    height = player_data["height"]
    weight = player_data["weight"]
    birth_date = player_data["birth"]["date"]
    try:
        date_obj = datetime.strptime(birth_date, "%Y-%m-%d")
        birth_date = date_obj.strftime("%d.%m.%Y")
    # no birth date found in data
    except TypeError as e:
        birth_date = None
    # sometimes there's an inconsistency in the api football with a different format for the birth date
    except ValueError as e:
        date_obj = datetime.strptime(birth_date, "%Y-%d-%m")
        birth_date = date_obj.strftime("%d.%m.%Y")

    global conn
    db_cursor = conn.cursor()
    db_cursor.execute("INSERT OR IGNORE INTO Players (player_id, first_name, last_name, nationality, height, weight, birth_date) VALUES (?, ?, ?, ?, ?, ?, ?)", 
            (player_id, first_name, last_name, nationality, height, weight, birth_date))

    conn.commit()


def save_startingXI_to_db(lineups, fixture_id):
    global conn
    db_cursor = conn.cursor()

    # check if fixture_id already exists
    db_cursor.execute('SELECT * FROM StartingXI WHERE fixture_id=?', (fixture_id,))
    fixture_data = db_cursor.fetchone()
    # if fixture is already present in the table, do not insert it again
    if fixture_data is not None:
        return

    global all_players_in_season
    # extract the players who played in the match for each team
    for i, team in enumerate(["home", "away"]):
        try:
            for player in lineups[i]["startXI"]:
                player_id = player["player"]["id"]
                position = player["player"]["pos"]
                position_on_grid = player["player"]["grid"]
                all_players_in_season.add(player_id)

                query_insert_player = '''
                                INSERT OR IGNORE INTO StartingXI (fixture_id, player_id, position, position_on_grid, team)
                                VALUES (?, ?, ?, ?, ?)
                                '''
                db_cursor.execute(query_insert_player, [fixture_id, player_id, position, position_on_grid, team])
        except IndexError as e:
            continue

    conn.commit()


def save_substitutes_to_db(fixture_id):
    global conn
    db_cursor = conn.cursor()

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


    global all_players_in_season
    for event in response["response"]:
        try:
            if (event['type'] == 'subst'):
                player_id_subbed_off = event["player"]["id"]
                player_id_subbed_in = event["assist"]["id"]
                all_players_in_season.add(player_id_subbed_in)
                all_players_in_season.add(player_id_subbed_off)

                team_name = event["team"]["name"]
                time_of_sub = event["time"]["elapsed"]
                query_insert_home_subs = '''
                                INSERT OR IGNORE INTO Substitutes (fixture_id, team, player_id_subbed_off, player_id_subbed_in, minute_subbed_in)
                                VALUES (?, ?, ?, ?, ?)
                                '''
                db_cursor.execute(query_insert_home_subs, [fixture_id, team_name, player_id_subbed_off, player_id_subbed_in, time_of_sub])
        except IndexError as e:
            continue

    conn.commit()



def save_player_stats_for_season(player_id, season):
    querystring = {"id" : player_id, "season": season}

    try:
        response = requests.request("GET", URL_PLAYERS, headers=HEADERS, params=querystring).json()
    except requests.exceptions.RequestException as e:
        print(f"Exception occured when requesting from API: {e}")

    try:
        player_stats = response["response"][0]["statistics"]
    except IndexError as e:
        return
    
    for competition_player_data in player_stats:
        # general comp data
        competition = competition_player_data["league"]["name"]
        appeareances = competition_player_data["games"]["appearences"]
        lineups = competition_player_data["games"]["lineups"]
        minutes_played = competition_player_data["games"]["minutes"]
        position = competition_player_data["games"]["position"]
        rating = competition_player_data["games"]["rating"]
        captain = competition_player_data["games"]["captain"]

        # sub data
        substitutes_in = competition_player_data["substitutes"]["in"]
        substitutes_out = competition_player_data["substitutes"]["out"]
        substitutes_bench = competition_player_data["substitutes"]["bench"]

        # shots
        shots_total =  competition_player_data["shots"]["total"]
        shots_on_goal = competition_player_data["shots"]["on"]

        # goals
        goals = competition_player_data["goals"]["total"]
        goals_conceded = competition_player_data["goals"]["conceded"]
        assists = competition_player_data["goals"]["assists"]
        saves = competition_player_data["goals"]["saves"]

        # passes
        passes_total = competition_player_data["passes"]["total"]
        key_passes = competition_player_data["passes"]["key"]
        passes_accuracy = competition_player_data["passes"]["accuracy"]

        # tackles
        tackles_total = competition_player_data["tackles"]["total"]
        blocks = competition_player_data["tackles"]["blocks"]
        interceptions = competition_player_data["tackles"]["interceptions"]

        # duels 
        duels_total = competition_player_data["duels"]["total"]
        duels_won = competition_player_data["duels"]["won"]
        duels_winrate = None
        if (duels_total is not None and duels_won is not None and duels_total != 0):
            duels_winrate = duels_won / duels_total

        # dribbles
        dribble_attempts = competition_player_data["dribbles"]["attempts"]
        dribble_success = competition_player_data["dribbles"]["success"]
        dribble_past = competition_player_data["dribbles"]["past"]

        # fouls
        fouls_drawn = competition_player_data["fouls"]["drawn"]
        fouls_commited = competition_player_data["fouls"]["committed"]

        # cards
        cards_yellow = competition_player_data["cards"]["yellow"]
        cards_yellowred = competition_player_data["cards"]["yellowred"]
        cards_red = competition_player_data["cards"]["red"]

        # penalty
        penalties_won = competition_player_data["penalty"]["won"]
        penalties_commited = competition_player_data["penalty"]["commited"]
        penalties_scored = competition_player_data["penalty"]["scored"]
        penalties_missed = competition_player_data["penalty"]["missed"]
        penalties_saved = competition_player_data["penalty"]["saved"]

        global conn
        db_cursor = conn.cursor()
        # Insert data into Statistics table
        db_cursor.execute("""INSERT OR IGNORE INTO Player_Statistics (
            player_id, season, competition_name, appeareances, lineups, minutes_played, position, rating, captain, substitutes_in, substitutes_out,
            substitutes_bench, shots_total, shots_on_goal, goals_total, goals_conceded, assists, saves, passes_total, key_passes,
            passes_accuracy, tackles_total, blocks, interceptions, duels_total, duels_won, duels_winrate, dribble_attempts,
            dribble_success, dribble_past, fouls_drawn, fouls_committed, cards_yellow, cards_yellowred, cards_red,
            penalties_won, penalties_committed, penalties_scored, penalties_missed, penalties_saved)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (player_id, season, competition, appeareances, lineups, minutes_played, position, rating, captain, substitutes_in, substitutes_out,
            substitutes_bench, shots_total, shots_on_goal, goals, goals_conceded, assists, saves, passes_total, key_passes,
            passes_accuracy, tackles_total, blocks, interceptions, duels_total, duels_won, duels_winrate, dribble_attempts,
            dribble_success, dribble_past, fouls_drawn, fouls_commited, cards_yellow, cards_yellowred, cards_red,
            penalties_won, penalties_commited, penalties_scored, penalties_missed, penalties_saved))

        conn.commit()
        