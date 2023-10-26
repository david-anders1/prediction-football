import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import re
from datetime import datetime
import json
from db_manager import connect_db
from copy import deepcopy

headers = {'User-Agent': UserAgent().chrome}



# Used for accessing fifa cards of previous fifa versions
date_url_component_fifa_version = { 
                                    #"FIFA 07": "070002",
                                    #"FIFA 08": "080002",
                                    #"FIFA 09": "090002",
                                    #"FIFA 10": "100002",
                                    #"FIFA 11": "110002",
                                    #"FIFA 12": "120002",
                                    #"FIFA 13": "130034",
                                    #"FIFA 14": "140052",
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

mapping_db_sofifa_naming = {
                                "Hertha Berlin" : "Hertha BSC",
                                "FC Schalke 04" : "Schalke 04",
                                "Brighton" : "Brighton & Hove Albion",
                                "1899 Hoffenheim" : "TSG Hoffenheim",
                                "Swansea" : "Swansea City",
                                "Union Berlin" : "FC Union Berlin",
                                "PSV Eindhoven" : "PSV",
                                "VfL Bochum" : "VfL Bochum 1848",
                                "Tottenham" : "Tottenham Hotspur",
                                "Wolves" : "Wolverhampton Wanderers",
                                "Leicester" : "Leicester City",
                                "West Ham" : "West Ham United",
                                "Bournemouth" : "AFC Bournemouth",
                                "Newcastle" : "Newcastle United",
                                "Norwich" : "Norwich City",
                                "Sheffield Utd" : "Sheffield United",
                                "Huddersfield" : "Huddersfield Town",
                                "FC Porto" : "Porto",
                                "SC Braga" : "Sporting Braga",
                                "West Brom" : "West Bromwich Albion",
                                "Atletico Madrid" : "Atlético Madrid",
                                "Leeds" : "Leeds United",
                                "Cardiff" : "Cardiff City", 
                                "Lyon" : "Olympique Lyonnais",
                                "AS Roma" : "Roma",
                                "SC Paderborn 07" : "Paderborn",
                                "Arminia Bielefeld" : "DSC Arminia Bielefeld",
                                "Barcelona" : "FC Barcelona",
                                "Club Brugge KV" : "Club Brugge",
                                "Spartak Moscow" : "Spartak Moskva",
                                "Swansea" : "Swansea City",
                                "Marseille" : "Olympique de Marseille",
                                "FC Midtjylland" : "Midtjylland"

                            }

def map_season_to_fifa_version(season_year):
    # Check if the season year is a valid integer
    if not isinstance(season_year, int):
        raise ValueError("Year should be an integer")
        
    if season_year < 1993 or season_year > 2099:
        raise ValueError("Year should be between 1993 and 2099")

    fifa_version = 'FIFA ' + str(season_year + 1)[-2:]
    
    return fifa_version


def get_soup_data_for_player(full_name, last_name, team_name):
    url_player = search_player_by_full_name(full_name)
    if url_player is None:
        url_player = search_player_by_last_name_and_team(last_name, team_name)
    if url_player is None:
        return 

    return get_soup_for_url(url_player)

def get_teams_for_player_in_season(player_id, conn, season):
    db_cursor = conn.cursor()
    
    # merge different tables to get team name where player has played in given season
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
    UNION
    SELECT DISTINCT M.home_team
    FROM Matches M 
    INNER JOIN Substitutes SUB ON M.fixture_id = SUB.fixture_id 
    WHERE SUB.player_id_subbed_in = ? AND M.season = ? AND SUB.team = M.home_team
    UNION
    SELECT DISTINCT M.away_team
    FROM Matches M 
    INNER JOIN Substitutes SUB ON M.fixture_id = SUB.fixture_id 
    WHERE SUB.player_id_subbed_in = ? AND M.season = ? AND SUB.team = M.away_team
    UNION
    SELECT DISTINCT M.home_team
    FROM Matches M 
    INNER JOIN Substitutes SUB ON M.fixture_id = SUB.fixture_id 
    WHERE SUB.player_id_subbed_off = ? AND M.season = ? AND SUB.team = M.home_team
    UNION
    SELECT DISTINCT M.away_team
    FROM Matches M 
    INNER JOIN Substitutes SUB ON M.fixture_id = SUB.fixture_id 
    WHERE SUB.player_id_subbed_off = ? AND M.season = ? AND SUB.team = M.away_team
    """
    
    db_cursor.execute(query, (player_id, season, player_id, season, player_id, season, player_id, season, player_id, season, player_id, season))
    
    rows = db_cursor.fetchall()
    
    # return the list of teams
    return [row[0] for row in rows]


def get_soup_for_url(search_url):
    try:
        response = requests.get(search_url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        return soup
    except requests.exceptions.ChunkedEncodingError as e:
        print(e)

def count_words(name):
    all_name_parts = name.replace("-", " ").split()

    return len(all_name_parts)

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
        position_ratings[position] = rating

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


    for fifa_version, url_components in reversed(date_url_component_fifa_version.items()):
        for fifa_version_url_component in url_components:
            search_url_fifa_version = search_url + "&r=" + fifa_version_url_component  + "&set=true"
            soup = get_soup_for_url(search_url_fifa_version)
            player_href = soup.find("a", {"role": "tooltip"}, href=lambda href: href and "/player/" in href)

            # if valid player soup found (i.e., player search was successful for the specific fifa version) continue with the rest of the code
            # once one version of a player card found, this card can be used to access all the versions for the player
            if player_href:
                complete_player_url = get_complete_player_fifa_url(player_href['href']) 
                soup_player = get_soup_for_url(complete_player_url)
                return soup_player
        
    return None


def search_player_by_last_name_and_team(last_name, team_name, fifa_version = "FIFA 23"):
    search_url = f"https://sofifa.com/players?keyword={last_name}"

    fifa_version_url_components = date_url_component_fifa_version[fifa_version]
    # try to find one valid url/soup for the player, then this valid soup can be used to find all card version of the player
    for url_component in fifa_version_url_components:
        search_url_fifa_version = search_url + "&r=" + url_component  + "&set=true"
        soup = get_soup_for_url(search_url_fifa_version)
        if (soup):
            break

    player_soups = soup.find_all("a", {"role": "tooltip"}, href=lambda href: href and "/player/" in href)

    if (team_name in mapping_db_sofifa_naming.keys()):
        team_name = mapping_db_sofifa_naming[team_name]

    for player_soup in player_soups:
        player_row = player_soup.find_parent("tr")
        try:
            team_player_row = player_row.find("a", href=lambda href: href and "/team/" in href).string
        except AttributeError:
            #logging.error(f"No team link found for player: {name}")
            team_player_row = None        
        if (team_player_row == team_name):
            return get_complete_player_fifa_url(player_soup['href'])
        
    return None

def compile_data_for_all_fifa_version_cards(player_id, player_soup):
    versions = scrape_fifa_versions(player_soup)
    conn = connect_db()

    for version in versions:
        url = get_complete_player_fifa_url(version["last_appearance_url"])
        date = version["last_appearance_date"]
        soup_version_player = get_soup_for_url(url)
        skill_stats_version_player = get_skill_stats(soup_version_player)["stats"]
        player_info_version_player = get_player_info(soup_version_player)
        position_ratings_version_player = get_position_ratings(soup_version_player)

        fifa_version, formatted_date_version_card = clean_date_string(date)
        if (fifa_version):
            insert_fifa_data(conn, player_id = player_id, player_info = player_info_version_player, player_skill_stats = skill_stats_version_player,
                        player_position_ratings = position_ratings_version_player,
                        fifa_version = fifa_version, date_fifa_card = formatted_date_version_card)


def get_complete_player_fifa_url(player_url):
    base_url = "https://sofifa.com"
    complete_url = f"{base_url}{player_url}"

    return complete_url

def scrape_fifa_versions(soup):    
    table = soup.find('table') 
    rows = table.find_all('tr')
    versions = []
    
    for row in rows:
        cols = row.find_all('td')
        if cols:
            season = cols[0].text
            team = cols[1].find('a').text
            last_appearance_url = cols[2].find('a')['href']
            last_appearance_date = cols[2].find('a').text
            
            # append the information as a dictionary to the versions list
            versions.append({
                'season': season,
                'team': team,
                'last_appearance_url': last_appearance_url,
                'last_appearance_date': last_appearance_date
            })
            
    return versions


def clean_date_string(date_fifa_string):
    # regular expression to match the FIFA version and date
    match = re.match(r'(FIFA|FC) (\d+) ([A-Za-z]+ \d+, \d+)', date_fifa_string)

    if match:
        fifa_version = int(match.group(2))
        date_str = match.group(3)
        
        # convert the date to the desired format
        date = datetime.strptime(date_str, '%b %d, %Y')
        formatted_date_version_card = date.strftime('%d.%m.%Y')

        return fifa_version, formatted_date_version_card


def insert_fifa_data(conn, player_id, player_info, player_skill_stats, player_position_ratings, fifa_version, date_fifa_card):
    db_cursor = conn.cursor()
    db_cursor.execute(f"INSERT OR IGNORE INTO FIFA_Player_Statistics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                      (player_id, 
                       player_info.get('Overall rating', None), 
                       player_info.get('Potential', None),
                       player_info.get('Preferred foot', None),
                       player_info.get('Weak foot', None),
                       player_info.get('Skill moves', None),
                       player_info.get('Work rate', None),
                       player_info.get('Body type', None),
                       player_info.get('Best Position', None),
                       player_info.get('Best Overall rating', None),
                       json.dumps(player_position_ratings), 
                       json.dumps(player_skill_stats),
                       fifa_version,
                       date_fifa_card
    ))
    conn.commit()



#soup_player = search_player_by_full_name(f"Jonas Hector")
#player_soup = get_soup_data_for_player("Florian Wirtz", "Wirtz", "Bayer Leverkusen")
#print(player_soup)

#skill_stats = get_skill_stats(player_soup)
# versions = scrape_fifa_versions(player_soup)
# conn = connect_db()

# for version in versions:
#     url = get_complete_player_fifa_url(version["last_appearance_url"])
#     date = version["last_appearance_date"]
#     soup_version_player = get_soup_for_url(url)
#     skill_stats_version_player = get_skill_stats(soup_version_player)["stats"]
#     player_info_version_player = get_player_info(soup_version_player)
#     position_ratings_version_player = get_position_ratings(soup_version_player)

#     fifa_version, formatted_date_version_card = clean_date_string(date)
#     insert_fifa_data(conn, player_id = "test1", player_info = player_info_version_player, player_skill_stats = skill_stats_version_player,
#                      player_position_ratings = position_ratings_version_player,
#                      fifa_version = fifa_version, date_fifa_card = formatted_date_version_card)



#print(skill_stats)