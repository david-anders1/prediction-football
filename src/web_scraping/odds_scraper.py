# odds_scraper.py
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from base_scraper import Scraper
import time
import re


class OddsScraper(Scraper):
    def __init__(self, sport, country, tournament):
        super().__init__()
        self.sport = sport
        self.country = country
        self.tournament = tournament

    def get_driver(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("window-size=1024,768")
        options.add_argument("--no-sandbox")
        return webdriver.Chrome(options=options)

    def get_soup_for_page(self, url):
        driver = self.get_driver()
        driver.get(url)
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        driver.quit()
        return soup

    def get_match_odds(self, soup):
        soup_matches = soup.find_all('div', {'class': 'eventRow flex w-full flex-col text-xs'})
        match_odds = []

        for match in soup_matches:
            regex_match_href = re.compile(".*next-m:flex-row.*")
            match_href = match.find(lambda tag: tag.name == 'a' and 
                                    tag.get('href') and 
                                    re.search(regex_match_href, " ".join(tag.get('class', []))))
            if not match_href:
                continue
            href = match_href['href']

            playing_teams = match.find_all('a', {'title': True})
            if len(playing_teams) < 2:
                continue
            home_team = playing_teams[0].get('title')
            away_team = playing_teams[1].get('title')

            div_date = match.find(lambda tag: tag.name == 'div' and 
                                  re.search(r"\btext-black-main\b.*\bfont-main\b.*\bw-full\b.*\btext-xs\b.*\bfont-normal\b.*\bleading-5\b", 
                                            " ".join(tag.get('class', []))))
            if div_date:
                raw_text_string = div_date.text.strip()
                match_date = self.parse_date(raw_text_string)

            fixture_id = self.get_fixture_id(match_date, home_team, away_team)
            if fixture_id:
                odds = match.find_all(lambda tag: tag.name == 'p' and 
                                      re.search(r"\bheight-content\b", 
                                                " ".join(tag.get('class', []))))
                if len(odds) >= 3:
                    home_odds, draw_odds, away_odds = odds[0].text, odds[1].text, odds[2].text
                    match_odds.append({
                        'fixture_id': fixture_id,
                        'match_date': match_date,
                        'home_team': home_team,
                        'away_team': away_team,
                        'home_odds': home_odds,
                        'draw_odds': draw_odds,
                        'away_odds': away_odds,
                        'href': href
                    })
                    self.save_odds_in_db(match_odds[-1])

        return match_odds

    def parse_date(self, raw_text_string):
        if "Yesterday" in raw_text_string:
            return (datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')
        elif "Today" in raw_text_string:
            return datetime.now().strftime('%d.%m.%Y')
        elif "Tomorrow" in raw_text_string:
            return (datetime.now() + timedelta(days=1)).strftime('%d.%m.%Y')
        else:
            try:
                return datetime.strptime(raw_text_string, '%d %b %Y').strftime('%d.%m.%Y')
            except ValueError:
                return None

    def get_fixture_id(self, match_date, home_team, away_team):
        query = """
        SELECT fixture_id FROM Matches 
        WHERE game_date = ? AND home_team = ? AND away_team = ?
        """
        cursor = self.conn.cursor()
        cursor.execute(query, (match_date, home_team, away_team))
        result = cursor.fetchone()
        return
