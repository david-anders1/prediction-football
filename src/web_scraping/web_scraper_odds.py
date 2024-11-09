from selenium import webdriver
from bs4 import BeautifulSoup as bs
from selenium.webdriver.chrome.options import Options
import re
import pandas as pd
from datetime import datetime, timedelta
import time
import sqlite3
from numpy.random import random_sample
from db.db_manager import *
from selenium.webdriver.chrome.service import Service



def get_match_odds(soup, tournament):
    soup_matches = soup.find_all('div', {'class': 'eventRow flex w-full flex-col text-xs'})

    match_odds = []
    match_date = None

    for match in soup_matches:
        regex_match_href = re.compile(".*next-m:flex-row.*")
        match_href = match.find(lambda tag: tag.name == 'a' and 
                                   tag.get('href') and 
                                   re.search(regex_match_href, " ".join(tag.get('class', []))))
        href = match_href['href']

        playing_teams = match.find_all('a', {'title': True})
        home_team = playing_teams[0].get('title')  
        away_team = playing_teams[1].get('title')  

        div_date = match.find(lambda tag: tag.name == 'div' and 
                            re.search(r"\btext-black-main\b.*\bfont-main\b.*\bw-full\b.*\btext-xs\b.*\bfont-normal\b.*\bleading-5\b", 
                                        " ".join(tag.get('class', []))))
        print(div_date)
        if (div_date is not None):
            raw_text_string = div_date.text.strip()
             
            # Deal with "Yesterday", "Today", "Tomorrow" in datestring
            if("Yesterday" in raw_text_string):
                match_date = (datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')
            elif("Today" in raw_text_string):
                match_date = datetime.now().strftime('%d.%m.%Y')
            elif("Tomorrow" in raw_text_string):
                match_date = (datetime.now() + timedelta(days=1)).strftime('%d.%m.%Y')
            else:
                raw_text_string_without_relegation_promotion = raw_text_string.replace(" - Relegation", "").replace(" - Promotion", "").strip()
                # convert date string to desired format %d.%m.%Y
                match_date = datetime.strptime(raw_text_string_without_relegation_promotion, '%d %b %Y').strftime('%d.%m.%Y')

        global conn
        fixture_id = get_fixture_id(conn, match_date, home_team, away_team)
        if (fixture_id is None):
            continue

        odds = match.find_all(lambda tag: tag.name == 'p' and 
                            re.search(r"\bheight-content\b", 
                                        " ".join(tag.get('class', []))))

        home_odds = odds[0].text
        draw_odds = odds[1].text
        away_odds = odds[2].text
        
        match_odds = {
            'fixture_id' : fixture_id,
            'match_date': match_date,
            'home_team': home_team,
            'away_team': away_team,
            'home_odds': home_odds,
            'draw_odds': draw_odds,
            'away_odds': away_odds,
            'href' : href
        }

        save_odds_in_db(match_odds)

    return match_odds


def get_soup_matches_page(url_matches):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("window-size=1024,768")
    options.add_argument("--no-sandbox")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36")

    #options.binary_location = "D:/chromedriver_win32/chromedriver.exe"
    driver = webdriver.Chrome(service=Service("D:/chromedriver_win32/chromedriver.exe"), options=options)

    driver.get(url_matches)

    # Simulate scrolling to get all the contents of the current page
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2 + random_sample())

        # Calculate new scroll height and compare with last scroll height
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    page_source = driver.page_source
    soup = bs(page_source, 'lxml')
    driver.quit()

    return soup


def save_odds_in_db(match_odds):
    global conn
    c = conn.cursor()

    c.execute(f'''
        CREATE TABLE IF NOT EXISTS Odds (
            fixture_id TEXT PRIMARY KEY,
            game_date TEXT,
            home_team TEXT,
            away_team TEXT,
            home_odds REAL,
            draw_odds REAL,
            away_odds REAL,
            href TEXT,
            FOREIGN KEY (fixture_id) REFERENCES Matches(fixture_id)
        )
    ''')

    c.execute(f'''
        INSERT OR IGNORE INTO Odds (fixture_id, game_date, home_team, away_team, home_odds, draw_odds, away_odds, href)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (match_odds['fixture_id'], match_odds['match_date'], match_odds['home_team'], match_odds['away_team'], match_odds['home_odds'], match_odds['draw_odds'], match_odds['away_odds'], match_odds['href']))

    conn.commit()


def generate_links_historic_seasons(sport, seasons, country, tournament):
    pages = list(range(1,10))

    global conn
    conn = connect_db()
    for season in seasons:
        tournament_and_season = f"{tournament}-{season}"
        for page in pages:
            url_matches = f"https://www.oddsportal.com/{sport}/{country}/{tournament_and_season}/results/#/page/{page}/"
            soup_matches = get_soup_matches_page(url_matches)
            match_data = get_match_odds(soup_matches, tournament)

    close_db(conn)


def generate_links_current_season(sport, country, tournament):
    pages = list(range(1,10))

    global conn
    conn = connect_db()
    for page in pages:
        url_matches = f"https://www.oddsportal.com/{sport}/{country}/{tournament}/results/#/page/{page}/"
        soup_matches = get_soup_matches_page(url_matches)
        match_data = get_match_odds(soup_matches, tournament)

    close_db(conn)


