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
    """
    Connect to the SQLite database and create the necessary tables if they do not exist.
    
    Returns
    -------
    Connection
        a SQLite connection object if successful, else prints an error message.
    """
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
    """
    Fetch all rows from a specified table in the SQLite database and return them in a DataFrame.
    
    Parameters
    ----------
    table_name : str
        The name of the table from which to fetch the data.
    
    Returns
    -------
    DataFrame
        a DataFrame containing all rows from the specified table.
    """
    with sqlite3.connect(DATABASE_PATH) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    return df

def close_db(conn):
    """
    Close the connection to the SQLite database.
    
    Parameters
    ----------
    conn : Connection
        The SQLite connection object to be closed.
    """
    try:
        conn.close()
    except sqlite3.Error as e:
        print(f"An error occurred while closing the database: {e}")


def create_matches_table(conn): 
    """
    Create the 'Matches' table in the SQLite database if it does not exist.
    
    Parameters
    ----------
    conn : Connection
        The SQLite connection object.
    """ 
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
    """
    Create the 'Match_Statistics' table in the SQLite database if it does not exist.
    
    Parameters
    ----------
    conn : Connection
        The SQLite connection object.
    names_stats : str
        A string representing the names of the columns in the 'Match_Statistics' table.
    """
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
    """
    Create the 'Substitutes' table in the SQLite database if it does not exist.
    
    Parameters
    ----------
    conn : Connection
        The SQLite connection object.
    """
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
    """
    Create the 'StartingXI' table in the SQLite database if it does not exist.
    
    Parameters
    ----------
    conn : Connection
        The SQLite connection object.
    """
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
    """
    Create the 'Players' table in the SQLite database if it does not exist.
    
    Parameters
    ----------
    conn : Connection
        The SQLite connection object.
    """
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
    """
    Create the 'FIFA_Player_Statistics' table in the SQLite database if it does not exist.
    
    Parameters
    ----------
    conn : Connection
        The SQLite connection object.
    """
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
    """
    Check if a player exists in the 'FIFA_Player_Statistics' table.
    
    Parameters
    ----------
    conn : Connection
        The SQLite connection object.
    player_id : str
        The ID of the player to check.
    
    Returns
    -------
    bool
        True if the player exists, False otherwise.
    """
    db_cursor = conn.cursor()
    db_cursor.execute(f"SELECT 1 FROM FIFA_Player_Statistics WHERE player_id = ?", (player_id,))
    result = db_cursor.fetchone()
    
    return result is not None


def create_statistics_player_table(conn):
    """
    Create the 'Player_Statistics' table in the SQLite database if it does not exist.
    
    Parameters
    ----------
    conn : Connection
        The SQLite connection object.
    """
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
    """
    Retrieve the fixture_id for a match using match_date and team names.
    
    Parameters
    ----------
    conn : Connection
        The SQLite connection object.
    match_date : str
        The date of the match.
    home_team : str
        The name of the home team.
    away_team : str
        The name of the away team.
    
    Returns
    -------
    str or None
        The fixture_id if found, None otherwise.
    """
    conn = sqlite3.connect('Data/football_database.db')
    cur = conn.cursor()

    fixture_id = match_fixture_teams_by_name(cur, match_date, home_team, away_team)

    if fixture_id:
        return fixture_id
    else:
        fixture_id = fuzzy_match_team_names(cur, match_date, home_team, away_team)
        return fixture_id


def update_team_name(conn, old_team_name, new_team_name):  
    """
    Update team names in the 'Matches' table in the SQLite database.
    
    Parameters
    ----------
    conn : Connection
        The SQLite connection object.
    old_team_name : str
        The old name of the team to be updated.
    new_team_name : str
        The new name of the team to be updated.
    """
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

def update_german_team_names():
    """
    Update the names of German teams in the 'Matches' table to align with the naming on sofifa.com (such that webscraping can work correctly).
    """
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
    """
    Retrieve the fixture_id for a match using match_date and team names by matching exact names or converted names.
    
    Parameters
    ----------
    cur : Cursor
        The SQLite cursor object.
    match_date : str
        The date of the match.
    home_team : str
        The name of the home team.
    away_team : str
        The name of the away team.
    
    Returns
    -------
    str or None
        The fixture_id if found, None otherwise.
    """
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
    

def fuzzy_match_team_names(cur, match_date, home_team, away_team):
    """
    This function performs fuzzy string matching to resolve discrepancies between team names
    in the API and on oddsportal, and returns the fixture_id of the matched team if found.

    Parameters
    ----------
    cur : sqlite3.Cursor
        Cursor object to execute SQLite commands.
    match_date : str
        The date of the match in a string format.
    home_team : str
        The name of the home team.
    away_team : str
        The name of the away team.

    Returns
    -------
    str or None
        Returns the fixture_id of the matched team if found, else returns None.

    Notes
    -----
    The function uses fuzzy string matching from the `fuzzywuzzy` package to compare team names. 
    A similarity ratio above 65 is considered as a match.
    """
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
    