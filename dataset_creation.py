from match_data_retrieval import *
from web_scraper_odds import generate_links_current_season, generate_links_historic_seasons
from web_scraper_fifa import *
import time
import random

LEAGUES_API_CODE = {"Bundesliga" : "78",
           "Premier League" : "39",
           "La Liga" : "140",
           "Serie A" : "135",
           "Ligue 1" : "61"
           }

INTERNATIONAL_COMPETITIONS_API_CODE = {"UEFA Champions League" : "2",
                                        "UEFA Europa League" : "3"
                                      }

# Webscraping from oddsportal.com requires a different formatting of the league and seasons strings
COUNTRY_AND_LEAGUES_ODDS_PORTAL = {"germany" : "bundesliga", 
               "england" : "premier-league", 
               "france" : "ligue-1", 
               "italy" : "serie-a",
               "spain": "laliga"
               }

SEASONS_ODDS_PORTAL = ["2021-2022", "2020-2021", "2019-2020", "2018-2019", "2017-2018", "2016-2017", "2015-2016", "2014-2015", "2013-2014", "2012-2013", "2011-2012",
           "2010-2011"]

SEASONS_API = [str(year) for year in range(2015, 2019)]


def compile_data_for_leagues_and_seasons(leagues, seasons):
    for league in leagues:
        for season in seasons:
            print(f"Getting data for {league} in season {season}")
            retrieve_competition_season_data(competition_name=league, competition_api_id=LEAGUES_API_CODE.get(league), season=season)


def compile_data_for_internationalcomp_and_seasons(international_comp, seasons):
    for comp in international_comp:
        for season in seasons:
            print(f"Getting data for {comp} in season {season}")
            retrieve_competition_season_data(competition_name=comp, competition_api_id=INTERNATIONAL_COMPETITIONS_API_CODE.get(comp), season=season)


def compile_odds_for_leagues_and_seasons(league_mapping: dict, seasons: list):
    for country, league in league_mapping.items():
        print(f"Getting odds for {league}")
        generate_links_historic_seasons(sport="football", seasons=seasons, country=country, tournament=league)



def compile_fifa_player_data():
    conn = connect_db()
    query = "SELECT * FROM Players"
    df_players = pd.read_sql_query(query, conn)

    for index, player in df_players.iterrows():
        print(player["player_id"])
        # if player already exists in fifa table, skip to the next one
        if check_player_exists(conn, player["player_id"]):
            continue

        try:
            delay_between_webscrapes = random.uniform(0.2, 0.5)
            time.sleep(delay_between_webscrapes)
            
            soup_player = search_player_by_full_name(f"{player['first_name']} {player['last_name']}")

            if (soup_player is None):
                num_words_last_name = count_words(player["last_name"])
                for season in range(2015, 2024):
                    delay_between_webscrapes = random.uniform(0.05, 0.1)
                    time.sleep(delay_between_webscrapes)

                    teams = get_teams_for_player_in_season(player["player_id"], conn, season)
                    fifa_version = map_season_to_fifa_version(season)
 
                    for team in teams:
                        if(num_words_last_name > 1):
                            for part_last_name in (player["last_name"].split()):
                                url_player = search_player_by_last_name_and_team(part_last_name, team, fifa_version)
                                if (url_player):
                                    soup_player = get_soup_for_url(url_player)
                                    break
                        else:       
                            url_player = search_player_by_last_name_and_team(player["last_name"], team, fifa_version)
                            if (url_player):
                                soup_player = get_soup_for_url(url_player)
                                break
                        
            if (soup_player):
                compile_data_for_all_fifa_version_cards(player["player_id"], soup_player)
            else:
                print(f"Webscraping did not work for player {player['last_name']} with player id {player['player_id']}.")

        except AttributeError as e:
            continue

    conn.close()


compile_fifa_player_data()
#compile_odds_for_leagues_and_seasons(COUNTRY_AND_LEAGUES_ODDS_PORTAL, SEASONS_ODDS_PORTAL)
#compile_data_for_leagues_and_seasons(LEAGUES_API_CODE, SEASONS_API)
#compile_data_for_internationalcomp_and_seasons(INTERNATIONAL_COMPETITIONS_API_CODE, SEASONS_API)
#generate_links_current_season(sport="football", country="germany", tournament="bundesliga")
