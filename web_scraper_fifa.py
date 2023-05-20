import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import re


def get_soup_data_for_player(full_name, last_name, team_name):
    url_player = search_player_by_full_name(full_name)
    if url_player is None:
        url_player = search_player_by_last_name_and_team(last_name, team_name)
    if url_player is None:
        return 

    headers = {'User-Agent': UserAgent().chrome}
    response = requests.get(url_player, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    return soup


def get_player_info(soup):
    player_info = {}

    # Extract overall rating, potential, value, and wage
    for div in soup.find_all('div', class_='block-quarter'):
        span = div.find('span', class_='bp3-tag')
        sub_div = div.find('div', class_='sub')
        if span and sub_div:
            label = sub_div.text.strip()
            player_info[label] = span.text.strip()

    # Extract profile information
    for li in soup.find_all('li', class_='ellipsis'):
        label = li.find('label')
        if label:
            label_text = label.text.strip()
            value = li.text.replace(label_text, '').strip()
            player_info[label_text] = value

    return player_info


def get_position_ratings(soup):
    div_tags = soup.select('div[class^="bp3-tag p"]')

    # Extract the position and rating from each 'div' tag and store them in a dictionary
    position_ratings = {}
    for div_tag in div_tags:
        position = div_tag.contents[0].strip()  
        rating = div_tag.contents[2].strip()    # Get the rating from the third part of the tag's content (after the <br> tag)
        # Remove the "+x" part of the rating
        rating = re.sub(r'\+\d$', '', rating)
        position_ratings[position] = int(rating)

    return position_ratings

def get_skill_stats(soup):
    skill_data = {}

    stats = {}
    for row in soup.find_all('li'):
        stat_value = None
        stat_name = None

        stat_value_element = row.find('span', class_='bp3-tag')
        if stat_value_element is not None:
            stat_value = stat_value_element.text

        stat_name_element = row.find('span', {'role': 'tooltip'})
        if stat_name_element is not None:
            stat_name = stat_name_element.text

        if stat_value is not None and stat_name is not None:
            stats[stat_name] = stat_value

    skill_data['stats'] = stats
    
    return skill_data




def search_player_by_full_name(full_name):
    search_url = f"https://sofifa.com/players?keyword={full_name}"

    headers = {'User-Agent': UserAgent().chrome}
    response = requests.get(search_url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    player_soup = soup.find("a", {"role": "tooltip"}, href=lambda href: href and "/player/" in href)

    if player_soup:
        return get_complete_player_fifa_url(player_soup['href']) 
    else:
        return None


def search_player_by_last_name_and_team(last_name, team_name):
    search_url = f"https://sofifa.com/players?keyword={last_name}"

    headers = {'User-Agent': UserAgent().chrome}
    response = requests.get(search_url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    player_soups = soup.find_all("a", {"role": "tooltip"}, href=lambda href: href and "/player/" in href)

    for player_soup in player_soups:
        player_row = player_soup.find_parent("tr")
        team_player_row = player_row.find("a", href=lambda href: href and "/team/" in href).string
        if (team_player_row == team_name):
            return get_complete_player_fifa_url(player_soup['href'])
        
    return None


def get_complete_player_fifa_url(player_url):
    base_url = "https://sofifa.com"
    complete_url = f"{base_url}{player_url}"

    return complete_url

    