from selenium import webdriver
from bs4 import BeautifulSoup as bs
from selenium.webdriver.chrome.options import Options
import re
import pandas as pd
from datetime import datetime
import time
import sqlite3
from numpy.random import random_sample


def get_match_data(soup, tournament, save_in_db = True):
    soup_matches = soup.find_all('div', {'class': 'eventRow flex w-full flex-col text-xs'})

    match_data = []
    game_date = 0

    for match in soup_matches:
        regex_match_href = re.compile(".*next-m:flex-row.*")
        match_href = match.find(lambda tag: tag.name == 'a' and 
                                   tag.get('href') and 
                                   re.search(regex_match_href, " ".join(tag.get('class', []))))
        href = match_href['href']

        playing_teams = match.find_all('a', {'title': True})
        home_team = playing_teams[0].get('title')  
        away_team = playing_teams[1].get('title')  
        
        scores = match.find_all(lambda tag: tag.name == 'div' and re.search(r"\bhidden\b.*\bnext-m:!flex\b", str(tag.get('class', []))))

        home_score = scores[0].text
        away_score = scores[2].text

        div_date = match.find(lambda tag: tag.name == 'div' and 
                            re.search(r"\btext-black-main\b.*\bfont-main\b.*\bw-full\b.*\btext-xs\b.*\bfont-normal\b.*\bleading-5\b", 
                                        " ".join(tag.get('class', []))))
        if (div_date is not None):
            raw_text_string = div_date.text.strip()
            # TO DO: live matches contain the "today" string which messes up the date formatting 
            if("Today" in raw_text_string):
                continue
            raw_text_string_without_relegation = raw_text_string.replace(" - Relegation", "").strip()
            # convert date string to desired format %d.%m.%Y
            game_date = datetime.strptime(raw_text_string_without_relegation, '%d %b %Y').strftime('%d.%m.%Y')


        time = match.find('p', class_='whitespace-nowrap').text
        
        odds = match.find_all(lambda tag: tag.name == 'p' and 
                            re.search(r"\bheight-content\b", 
                                        " ".join(tag.get('class', []))))

        home_odds = odds[0].text
        draw_odds = odds[1].text
        away_odds = odds[2].text
        
        match_data.append({
            'game_date': game_date,
            'time': time,
            'home_team': home_team,
            'away_team': away_team,
            'home_score': home_score,
            'away_score': away_score,
            'home_odds': home_odds,
            'draw_odds': draw_odds,
            'away_odds': away_odds,
            'href' : href
        })
    
    if (save_in_db):
        create_sql_entries(match_data, tournament)

    return match_data


def get_soup_matches_page(url_matches):
    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)

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


def create_sql_entries(match_data, name_db):
    conn = sqlite3.connect(f"Data/{name_db}.db")

    c = conn.cursor()

    c.execute(f'''
        CREATE TABLE IF NOT EXISTS {name_db} (
            game_date TEXT,
            time TEXT,
            home_team TEXT,
            away_team TEXT,
            home_score INTEGER,
            away_score INTEGER,
            home_odds REAL,
            draw_odds REAL,
            away_odds REAL
        )
    ''')

    # Iterate over match_data and insert each match into the database
    for match in match_data:
        c.execute(f'''
            INSERT OR IGNORE INTO {name_db} (game_date, time, home_team, away_team, home_score, away_score, home_odds, draw_odds, away_odds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (match['game_date'], match['time'], match['home_team'], match['away_team'], match['home_score'], match['away_score'], match['home_odds'], match['draw_odds'], match['away_odds'], match['href']))

    conn.commit()

    conn.close()


def generate_links_historic_seasons(seasons, sport, country, tournament):
    pages = list(range(1,10))

    for season in seasons:
        tournament_and_season = f"{tournament}-{season}"
        for page in pages:
            url_matches = f"https://www.oddsportal.com/{sport}/{country}/{tournament_and_season}/results/#/page/{page}/"
            soup_matches = get_soup_matches_page(url_matches)
            match_data = get_match_data(soup_matches, tournament)


def generate_links_current_season(sport, country, tournament):
    pages = list(range(1,10))

    for page in pages:
        url_matches = f"https://www.oddsportal.com/{sport}/{country}/{tournament}/results/#/page/{page}/"
        soup_matches = get_soup_matches_page(url_matches)
        match_data = get_match_data(soup_matches, tournament)



sport = "football"
country = "germany"
tournament = "bundesliga"

tournaments = {"germany" : "bundesliga", 
               "england" : "premier-league", 
               "france" : "ligue-1", 
               "italy" : "serie-a",
               "spain": "laliga"
               }

seasons = ["2021-2022", "2020-2021", "2019-2020", "2018-2019", "2017-2018", "2016-2017", "2015-2016", "2014-2015", "2013-2014", "2012-2013", "2011-2012",
           "2010-2011"]

