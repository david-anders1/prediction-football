import sqlite3
from fuzzywuzzy import fuzz

NAME_DB = "football_database"

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
        conn = sqlite3.connect(f"Data/{NAME_DB}.db")
        return conn
    except sqlite3.Error as e:
        print(f"An error occurred while connecting to the database: {e}")


def close_db(conn):
    try:
        conn.close()
    except sqlite3.Error as e:
        print(f"An error occurred while closing the database: {e}")


def get_fixture_id(conn, match_date, home_team, away_team):
    conn = sqlite3.connect('Data/football_database.db')
    cur = conn.cursor()

    fixture_id = match_fixture_teams_by_name(cur, match_date, home_team, away_team)

    if fixture_id:
        return fixture_id
    else:
        fixture_id = fuzzy_match_team_names(cur, match_date, home_team, away_team)
        return fixture_id


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
    