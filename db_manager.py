import sqlite3
import pandas as pd
from fuzzywuzzy import fuzz

DATABASE_PATH = 'Data/football_database.db'


# Here the conversion of team names between the oddsportal naming convention and the api football naming convention can be listed
NAMECONVERSION_ODDSPORTAL_API = {
                                    "Dortmund" : "Borussia Dortmund",
                                    "Wolfsburg" : "VfL Wolfsburg",
                                    "Mainz" : "FSV Mainz 05",
                                    "Schalke" : "FC Schalke 04",
                                    "B. Monchengladbach" : "Borussia Monchengladbach",
                                    "Stuttgart" : "VfB Stuttgart",
                                    "Augsburg" : "FC Augsburg"
                                }


def connect_db():
    try:
        conn = sqlite3.connect(DATABASE_PATH)

        create_matches_table(conn)
        create_substitute_table(conn)
        create_startingXI_table(conn)
        create_players_table(conn)
        create_fifa_players_table(conn)
        create_statistics_player_table(conn)

        return conn
    except sqlite3.Error as e:
        print(f"An error occurred while connecting to the database: {e}")

def fetch_data_from_db(table_name):
    with sqlite3.connect(DATABASE_PATH) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    return df

def close_db(conn):
    try:
        conn.close()
    except sqlite3.Error as e:
        print(f"An error occurred while closing the database: {e}")


def create_matches_table(conn):  
    db_cursor = conn.cursor()
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
    conn.commit()
    

def create_match_statistics_table(conn, names_stats):
    db_cursor = conn.cursor()
    query_create_table = f'''
                    CREATE TABLE IF NOT EXISTS Match_Statistics (
                    fixture_id STRING PRIMARY KEY,
                    {names_stats},
                    FOREIGN KEY (fixture_id) REFERENCES Matches(fixture_id)
                    )
                    '''

    db_cursor.execute(query_create_table)
    conn.commit()


def create_substitute_table(conn):
    query_create_table_subs = '''
                        CREATE TABLE IF NOT EXISTS Substitutes (
                        id INTEGER PRIMARY KEY,
                        fixture_id TEXT,
                        team TEXT,
                        player_id_subbed_off TEXT,
                        player_id_subbed_in TEXT,
                        minute_subbed_in INTEGER,
                        FOREIGN KEY (fixture_id) REFERENCES Matches(fixture_id)
                        )
                        '''
    db_cursor = conn.cursor()
    db_cursor.execute(query_create_table_subs)
    conn.commit()


def create_startingXI_table(conn):
    query_create_table_startingXI = '''
                        CREATE TABLE IF NOT EXISTS StartingXI (
                        id INTEGER PRIMARY KEY,
                        fixture_id TEXT,
                        player_id TEXT,
                        position TEXT,
                        position_on_grid TEXT,
                        team TEXT,
                        FOREIGN KEY(fixture_id) REFERENCES Matches(fixture_id)
                        )
                        '''
    db_cursor = conn.cursor()
    db_cursor.execute(query_create_table_startingXI)


def create_players_table(conn):
    query_create_table_players = '''
                        CREATE TABLE IF NOT EXISTS Players (
                        player_id TEXT PRIMARY KEY,
                        first_name TEXT,
                        last_name TEXT,
                        nationality TEXT,
                        birth_date TEXT,
                        height INTEGER,
                        weight INTEGER
                        )
                        '''
    db_cursor = conn.cursor()
    db_cursor.execute(query_create_table_players)


def create_fifa_players_table(conn):
    query_create_table_players = '''
                        CREATE TABLE IF NOT EXISTS FIFA_Player_Statistics (
                        player_id TEXT,
                        overall_rating INTEGER,
                        potential INTEGER,
                        preferred_foot TEXT,
                        weak_foot INTEGER,
                        skill_moves INTEGER,
                        work_rate TEXT,
                        body_type TEXT,
                        best_position TEXT,
                        best_overall_rating INTEGER,
                        positions TEXT,
                        stats TEXT,
                        fifa_version TEXT,
                        date_fifa_card TEXT,
                        PRIMARY KEY(player_id, date_fifa_card)
                        )
                        '''
    db_cursor = conn.cursor()
    db_cursor.execute(query_create_table_players)


def check_player_exists(conn, player_id):
    db_cursor = conn.cursor()
    db_cursor.execute(f"SELECT 1 FROM FIFA_Player_Statistics WHERE player_id = ?", (player_id,))
    result = db_cursor.fetchone()
    
    return result is not None


def create_statistics_player_table(conn):
    query_create_statistics_players = '''
                                    CREATE TABLE IF NOT EXISTS Player_Statistics (
                                    player_id INTEGER NOT NULL,
                                    season INTEGER NOT NULL,
                                    competition_name TEXT NOT NULL,
                                    appeareances INTEGER,
                                    lineups INTEGER,
                                    minutes_played INTEGER,
                                    position TEXT,
                                    rating REAL,
                                    captain INTEGER,
                                    substitutes_in INTEGER,
                                    substitutes_out INTEGER,
                                    substitutes_bench INTEGER,
                                    shots_total INTEGER,
                                    shots_on_goal INTEGER,
                                    goals_total INTEGER,
                                    goals_conceded INTEGER,
                                    assists INTEGER,
                                    saves INTEGER,
                                    passes_total INTEGER,
                                    key_passes INTEGER,
                                    passes_accuracy INTEGER,
                                    tackles_total INTEGER,
                                    blocks INTEGER,
                                    interceptions INTEGER,
                                    duels_total INTEGER,
                                    duels_won INTEGER,
                                    duels_winrate REAL,
                                    dribble_attempts INTEGER,
                                    dribble_success REAL,
                                    dribble_past INTEGER,
                                    fouls_drawn INTEGER,
                                    fouls_committed INTEGER,
                                    cards_yellow INTEGER,
                                    cards_yellowred INTEGER,
                                    cards_red INTEGER,
                                    penalties_won INTEGER,
                                    penalties_committed INTEGER,
                                    penalties_scored INTEGER,
                                    penalties_missed INTEGER,
                                    penalties_saved INTEGER,
                                    FOREIGN KEY(player_id) REFERENCES Players(player_id),
                                    PRIMARY KEY(player_id, season, competition_name)
                                    );
                                    '''
    db_cursor = conn.cursor()
    db_cursor.execute(query_create_statistics_players)


def get_fixture_id(conn, match_date, home_team, away_team):
    conn = sqlite3.connect('Data/football_database.db')
    cur = conn.cursor()

    fixture_id = match_fixture_teams_by_name(cur, match_date, home_team, away_team)

    if fixture_id:
        return fixture_id
    else:
        fixture_id = fuzzy_match_team_names(cur, match_date, home_team, away_team)
        return fixture_id


def update_team_name(conn, old_team_name, new_team_name):  
    db_cursor = conn.cursor()
    
    # Update the home_team column
    db_cursor.execute(f'''
                UPDATE Matches
                SET home_team = ?
                WHERE home_team = ?
            ''', (new_team_name, old_team_name))
    
    # Update the away_team column
    db_cursor.execute(f'''
                UPDATE Matches
                SET away_team = ?
                WHERE away_team = ?
            ''', (new_team_name, old_team_name))

    conn.commit()

# update team names manually to be in line with the naming on sofifa.com (such that webscraping can work correctly)
def update_german_team_names():
    conn = connect_db()
    update_team_name(conn, "Bayern Munich", "FC Bayern München")
    update_team_name(conn, "FC Koln", "FC Köln")
    update_team_name(conn, "Fortuna Dusseldorf", "Fortuna Düsseldorf")
    update_team_name(conn, "Borussia Monchengladbach", "Borussia Mönchengladbach")
    update_team_name(conn, "SpVgg Greuther Furth", "SpVgg Greuther Fürth")
    update_team_name(conn, "VfL BOCHUM", "VfL Bochum")
    update_team_name(conn, "Bayer Leverkusen", "Bayer 04 Leverkusen")
    update_team_name(conn, "SC Paderborn 07", "Paderborn")
    update_team_name(conn, "FC Heidenheim", "Heidenheim")
    update_team_name(conn, "FC Ingolstadt 04", "Ingolstadt")

    conn.close()


def match_fixture_teams_by_name(cur, match_date, home_team, away_team):
    if home_team in NAMECONVERSION_ODDSPORTAL_API.keys():
        home_team = NAMECONVERSION_ODDSPORTAL_API.get(home_team)
    if away_team in NAMECONVERSION_ODDSPORTAL_API.keys():
        away_team = NAMECONVERSION_ODDSPORTAL_API.get(away_team)

    query = f"""
    SELECT fixture_id
    FROM Matches
    WHERE match_date = ? AND (home_team = ? or away_team = ?)
    """
    cur.execute(query, (match_date, home_team, away_team))

    result = cur.fetchone()
    if result:
        fixture_id = result[0]
        return fixture_id
    else:
        return None
    

# Do fuzzy string matching to solve the problem of different team names in the API and on oddsportal
def fuzzy_match_team_names(cur, match_date, home_team, away_team):
    query = f"""
    SELECT fixture_id, home_team, away_team
    FROM Matches
    WHERE match_date = ?
    """
    cur.execute(query, (match_date,))

    results = cur.fetchall()
    if results:
        for result in results:
            home_team_similarity = fuzz.ratio(home_team, result[1])
            away_team_similarity = fuzz.ratio(away_team, result[2])
            if home_team_similarity > 65 or away_team_similarity > 65:  
                return result[0]

        print(f"No match found with provided date {match_date} and team names {home_team} and {away_team}.")
        return None
    