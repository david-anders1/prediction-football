from match_data_retrieval import *

LEAGUES_API_CODE = {"Bundesliga" : "78",
           "Premier League" : "39",
           "La Liga" : "140",
           "Serie A" : "135"
           }

INTERNATIONAL_COMPETITIONS_API_CODE = {"UEFA Champions League" : "2"}

# Webscraping from oddsportal.com requires a different formatting of the league and seasons strings
COUNTRY_AND_LEAGUES_ODDS_PORTAL = {"germany" : "bundesliga", 
               "england" : "premier-league", 
               "france" : "ligue-1", 
               "italy" : "serie-a",
               "spain": "laliga"
               }

SEASONS_ODDS_PORTAL = ["2021-2022", "2020-2021", "2019-2020", "2018-2019", "2017-2018", "2016-2017", "2015-2016", "2014-2015", "2013-2014", "2012-2013", "2011-2012",
           "2010-2011"]

SEASONS_API = [str(year) for year in range(2010, 2023)]


def compile_data_for_leagues_and_seasons(leagues, seasons):
    for league in leagues:
        for season in seasons:
            print(f"Getting data for {league} in season {season}")
            retrieve_competition_season_data(competition_name=league, competition_api_id=LEAGUES_API_CODE.get(league), season=season)



#compile_data_for_leagues_and_seasons(LEAGUES_API_CODE, SEASONS_API)

# fixture_id = "197055"
# url = f"https://api-football-v1.p.rapidapi.com/v3/fixtures/events"
# querystring = {"fixture" : fixture_id}
# response =  requests.request("GET", url, headers=HEADERS, params=querystring).json()

# for event in response["response"]:
#     if (event['type'] == 'subst'):
#         print(event)
