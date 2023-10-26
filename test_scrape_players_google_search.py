import re
from web_scraper_fifa import get_soup_for_url, compile_data_for_all_fifa_version_cards
from googlesearch import search
import time

def extract_player_info_from_log(log_file):
    player_info = []
    with open(log_file, 'r') as file:
        for line in file:
            match = re.search(r"player with (\d+) using (.*), (.*), (FIFA \d+)", line)
            if match:
                player_id, name, team, fifa_version = match.groups()
                player_info.append({"player_id": player_id, "name": name, "team": team, "fifa_version": fifa_version})
    return player_info

def get_sofifa_link(query):
    for j in search(query + " site:sofifa.com", num=10, stop=10, pause=2):
        return j
    return None

def scrape_data_for_players(player_info):
    for player in player_info:
        query = f"{player['name']} {player['team']} {player['fifa_version']} sofifa"

        time.sleep(0.1)
        
        print(query)
        new_url = get_sofifa_link(query)
        
        if new_url:
            print(f"Found URL: {new_url}")
            try:
                soup_player = get_soup_for_url(new_url)
                compile_data_for_all_fifa_version_cards(player["player_id"], soup_player)
                print(f"Data compiled successfully for player with ID {player['player_id']}")
            except Exception as e:
                print(f"An error occurred while processing player with ID {player['player_id']}: {str(e)}")
        else:
            print(f"Could not find a Sofifa link for player with ID {player['player_id']}")






if __name__ == "__main__":
    log_file = 'fifa_webscraping_data.log'
    player_info = extract_player_info_from_log(log_file)
    scrape_data_for_players(player_info)
