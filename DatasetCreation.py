from WebScraper import *
import requests
import time
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get API key from environment variable
api_key = os.getenv('API_KEY')

# Use the API key in the headers for the requests module
headers = {'X-RapidAPI-Key': f'{api_key}',
           "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"      
          }

BASE_URL = "https://api-football-v1.p.rapidapi.com/v3/"


def get_all_live_matches():
    response = requests.get(BASE_URL + "fixtures?live=all", headers=headers).json()
    live_matches = response["response"]
    return live_matches


def check_red_card_in_live_matches(live_matches):
    for match in live_matches:
        home_team = match["teams"]["home"]["name"]
        away_team = match["teams"]["away"]["name"]
        match_id = match["fixture"]["id"]

        # get the events for this match
        match_response = requests.get(BASE_URL + f"fixtures?id={match_id}", headers=headers).json()
        events = match_response["response"][0]['events']

        # check for red card events
        for event in events:
            if event["type"] == "Card" and event["detail"] == "Red Card":
                team_name = event["team"]["name"]
                player_name = event["player"]["name"]
                print(f"{player_name} received a red card for {team_name} in the match between {home_team} and {away_team}")
                print(match)
                odds_response = requests.get(BASE_URL + f"odds?fixture={match_id}", headers=headers).json()
                bookmakers = odds_response["response"][0]["bookmakers"]
                for bookmaker in bookmakers:
                    bookmaker_name = bookmaker["name"]
                    odds = bookmaker["bets"][0]["values"]
                    print(f"{bookmaker_name} has odds of {odds} for the match with fixture ID {match_id} where a red card was received")

    # wait for 5 seconds before checking the next match
    time.sleep(5)


def create_matches_df(json_matches, season):
    match_data_list = []

    # loop through each match and extract the relevant data
    for match in json_matches:
        fixture_id = match["fixture"]["id"]
        home_team = match["teams"]["home"]["name"]
        away_team = match["teams"]["away"]["name"]
        home_score = match["goals"]["home"]
        away_score = match["goals"]["away"]
        match_date = match["fixture"]["date"]
        match_status = match["fixture"]["status"]["short"]

        match_data_list.append(
            {
                "Fixture ID": fixture_id,
                "Home Team": home_team,
                "Away Team": away_team,
                "Home Score": home_score,
                "Away Score": away_score,
                "Match Date": match_date,
                "Match Status": match_status,
                "Season": season,
            }
        )
    df_match_data = pd.DataFrame(match_data_list)
        
    return df_match_data


def retrieve_league_season_data(league_id, season):
    response = requests.get(BASE_URL + f"fixtures?league={league_id}&season={season}&timezone=Europe/Berlin", headers=headers).json()
    print(response)
    json_matches = response["response"]
    
    df_matches = create_matches_df(json_matches, season)
    df_matches = add_statistics_to_df_matches(df_matches)
    compile_players_per_fixture(df_matches)

    return df_matches



def add_statistics_to_df_matches(df_matches):
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"
    counter = 0

    for fixture_id in df_matches['Fixture ID'].values:
        print(fixture_id)
        df_matches.to_csv("match_data_season_x")

        querystring = {"fixture" : fixture_id}

        statistics_response =  requests.request("GET", url, headers=headers, params=querystring).json()
        statistics = statistics_response["response"]
        df_matches["Fixture ID"] = df_matches["Fixture ID"].astype(str)

        row = df_matches[df_matches["Fixture ID"] == fixture_id]
        statistics_home_team = statistics[0]["statistics"]
        for stat in statistics_home_team:
            df_matches.loc[row.index, f"Home_{stat['type']}"] = stat['value']

        statistics_away_team = statistics[1]["statistics"]
        for stat in statistics_away_team:
            df_matches.loc[row.index, f"Away_{stat['type']}"] = stat['value']

    return df_matches


def compile_players_per_fixture(df_matches):
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/lineups"
    all_lineups = pd.DataFrame()

    for fixture_id in df_matches['Fixture ID'].values:
        all_lineups.to_csv("lineups.csv")

        querystring = {"fixture" : fixture_id}

        response = requests.request("GET", url, headers=headers, params=querystring).json()
        lineups = response["response"]

        # extract the players who played in the match for each team
        home_players = [player["player"]["name"] for player in lineups[0]["startXI"]]
        away_players = [player["player"]["name"] for player in lineups[1]["startXI"]]

        home_subs = [player["player"]["name"] for player in lineups[0]["substitutes"]]
        away_subs = [player["player"]["name"] for player in lineups[1]["substitutes"]]

        lineup_data = {"fixture_id": fixture_id}

        # Add home players and subs to the dictionary
        for i, player in enumerate(home_players, start=1):
            lineup_data[f"home_player_{i}"] = player
        for i, player in enumerate(home_subs, start=1):
            lineup_data[f"home_sub_{i}"] = player
        # Add away players and subs to the dictionary
        for i, player in enumerate(away_players, start=1):
            lineup_data[f"away_player_{i}"] = player
        for i, player in enumerate(away_subs, start=1):
            lineup_data[f"away_sub_{i}"] = player

        df_lineup = pd.DataFrame([lineup_data])

        # Create a DataFrame with the lineup data
        all_lineups = pd.concat([all_lineups, df_lineup], ignore_index=True)

    all_lineups.to_csv("lineups.csv")

    return all_lineups



leagues = {"Bundesliga" : "78",
           "Premier League" : "39",
           "La Liga" : "140",
           "Serie A" : "135"
           }

international_competitions = {"UEFA Champions League" : "2"}

df_matches = retrieve_league_season_data(leagues.get("Bundesliga"), 2022)
df_matches.to_csv("data/test.csv")