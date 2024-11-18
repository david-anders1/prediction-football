from base_scraper import Scraper
from datetime import datetime
import re
import json


class FIFAPlayerScraper(Scraper):
    DATE_URL_COMPONENT_FIFA_VERSION = {
        "FIFA 15": ["150001", "150059"],
        "FIFA 16": ["160001", "160058"],
        "FIFA 17": ["170001", "170099"],
        "FIFA 18": ["180001", "180084"],
        "FIFA 19": ["190001", "190075"],
        "FIFA 20": ["200001", "200061"],
        "FIFA 21": ["210001", "210064"],
        "FIFA 22": ["220001", "220069"],
        "FIFA 23": ["230001", "230045"],
    }
    
    TEAM_NAME_MAPPING = {
        "Hertha Berlin": "Hertha BSC",
        "FC Schalke 04": "Schalke 04",
        "Brighton": "Brighton & Hove Albion",
        # add other team mappings as needed
    }

    def __init__(self, player_id, full_name, last_name, team_name):
        super().__init__()
        self.player_id = player_id
        self.full_name = full_name
        self.last_name = last_name
        self.team_name = team_name

    def search_player(self):
        url = f"https://sofifa.com/players?keyword={self.full_name}"
        return self.get_soup(url)

    def get_teams_for_season(self, season):
        query = """
        SELECT DISTINCT M.home_team 
        FROM Matches M 
        INNER JOIN StartingXI S ON M.fixture_id = S.fixture_id 
        WHERE S.player_id = ? AND M.season = ? AND S.team = 'home'
        UNION
        SELECT DISTINCT M.away_team 
        FROM Matches M 
        INNER JOIN StartingXI S ON M.fixture_id = S.fixture_id 
        WHERE S.player_id = ? AND M.season = ? AND S.team = 'away'
        """
        cursor = self.conn.cursor()
        cursor.execute(query, (self.player_id, season, self.player_id, season))
        return [row[0] for row in cursor.fetchall()]

    def get_player_info(self, soup):
        player_info = {}
        for div in soup.find_all('div', class_='block-quarter'):
            span = div.find('span', class_='bp3-tag')
            sub_div = div.find('div', class_='sub')
            if span and sub_div:
                label = sub_div.text.strip()
                player_info[label] = span.text.strip()
        return player_info

    def get_position_ratings(self, soup):
        position_ratings = {}
        div_tags = soup.select('div[class^="bp3-tag p"]')
        for div_tag in div_tags:
            position = div_tag.contents[0].strip()
            rating = re.sub(r'\+\d$', '', div_tag.contents[2].strip())
            position_ratings[position] = rating
        return position_ratings

    def get_skill_stats(self, soup):
        skill_data = {}
        stats = {}
        for row in soup.find_all('li'):
            stat_value_element = row.find('span', class_='bp3-tag')
            stat_name_element = row.find('span', {'role': 'tooltip'})
            if stat_value_element and stat_name_element:
                stat_value = stat_value_element.text
                stat_name = stat_name_element.text
                stats[stat_name] = stat_value
        skill_data['stats'] = stats
        return skill_data

    def save_player_data(self, player_data):
        query = """
        INSERT OR IGNORE INTO FIFA_Player_Statistics 
        (player_id, overall_rating, potential, preferred_foot, weak_foot, 
        skill_moves, work_rate, body_type, best_position, best_overall_rating, 
        position_ratings, skill_stats, fifa_version, date_fifa_card)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.save_to_db(query, player_data)
