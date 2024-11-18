import sqlite3
from config import settings
import os

conn = sqlite3.connect(settings.paths.database) 
cursor = conn.cursor()

# SQL commands for creating the tables
create_results_table = '''
CREATE TABLE IF NOT EXISTS results (
    id_match TEXT PRIMARY KEY,  
    division TEXT,
    country TEXT,
    date DATE,
    home_team TEXT,
    away_team TEXT,
    ft_home_team_goals INT,
    ft_away_team_goals INT,
    ft_result TEXT,
    ht_result TEXT,
    ht_home_team_goals INT,
    ht_away_team_goals INT
);
'''

create_match_statistics_table = '''
CREATE TABLE IF NOT EXISTS match_statistics (
    id_match TEXT,
    attendance INT,
    referee TEXT,
    home_team_shots INT,
    away_team_shots INT,
    home_team_shots_on_target INT,
    away_team_shots_on_target INT,
    home_team_hit_woodwork INT,
    away_team_hit_woodwork INT,
    home_team_corners INT,
    away_team_corners INT,
    home_team_fouls_committed INT,
    away_team_fouls_committed INT,
    home_team_free_kicks_conceded INT,
    away_team_free_kicks_conceded INT,
    home_team_offsides INT,
    away_team_offsides INT,
    home_team_yellow_cards INT,
    away_team_yellow_cards INT,
    home_team_red_cards INT,
    away_team_red_cards INT,
    home_team_bookings_points INT,
    away_team_bookings_points INT,
    FOREIGN KEY (id_match) REFERENCES results (id_match),
    UNIQUE (id_match)
);
'''

# Execute the SQL commands
cursor.execute(create_results_table)
cursor.execute(create_match_statistics_table)

# Commit changes and close the connection
conn.commit()
conn.close()

print("Tables created successfully.")
