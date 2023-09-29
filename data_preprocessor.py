import pandas as pd
import numpy as np
from db_manager import fetch_data_from_db


# Function that returns the favorite team (with the best odds)
def get_favorite(row):
    if row['home_odds'] < row['away_odds']:
        return 'home'
    else:
        return 'away'
    
# Function that returns the underdog team (with the worst odds)
def get_underdog(row):
    if row['home_odds'] > row['away_odds']:
        return 'home'
    else:
        return 'away'
    
# Function that calculates the result of the game
def get_result(row):
    if row['home_goals'] > row['away_goals']:
        return 'home'  # home team won
    elif row['home_goals'] < row['away_goals']:
        return 'away'  # away team won
    else:
        return 'draw'  # draw
    

def merge_match_odds(df_matches, df_odds):
    df_merged_matches_odds = df_matches.merge(df_odds[['fixture_id', 'home_odds', 'draw_odds', 'away_odds']], on='fixture_id', how='inner')
    return df_merged_matches_odds

def calculate_margin(row, bet_target, bet_amount = 10):
    try:
        if bet_target == 'draw':
            if row['result'] == 'X':
                return bet_amount * float(row['draw_odds']) - bet_amount  # win, so return profit
            else:
                return -bet_amount  # loss, so return loss
        elif bet_target == 'favorite':
            odds = row['home_odds'] if row['favorite'] == 'home' else row['away_odds']
            if row['favorite'] == row['result']:
                return bet_amount * float(odds) - bet_amount  # win, so return profit
            else:
                return -bet_amount  # loss, so return loss
        else:  # bet_target is 'underdog'
            odds = row['home_odds'] if row['underdog'] == 'home' else row['away_odds']
            if row['underdog'] == row['result']:
                return bet_amount * float(odds) - bet_amount  # win, so return profit
            else:
                return -bet_amount  # loss, so return loss
    except ValueError as e:
        return None

def add_betting_info(df):
    df = df.copy()  # make a copy of the dataframe to avoid changing the original dataframe
    df['favorite'] = df.apply(get_favorite, axis=1)
    df['underdog'] = df.apply(get_underdog, axis=1)
    df['result'] = df.apply(get_result, axis=1)
    return df


def bet_on(df, bet_target, bet_amount = 10):
    df['win_margin_bet_on_'+bet_target] = df.apply(calculate_margin, args=(bet_target, bet_amount), axis=1)
    return df



def add_winning_streak_info(df):
    home_streaks = {}
    away_streaks = {}
    home_seasons = {}
    away_seasons = {}

    # Convert 'match_date' to datetime
    df['match_date'] = pd.to_datetime(df['match_date'], format='%d.%m.%Y')
    df = df.sort_values('match_date')

    for i, row in df.iterrows():
        home_team = row['home_team']
        away_team = row['away_team']
        result = row['result']
        season = row['season']
        
        # Check if the teams have played before, if not set their streaks to 0
        home_streaks.setdefault(home_team, 0)
        away_streaks.setdefault(away_team, 0)
        home_seasons.setdefault(home_team, season)
        away_seasons.setdefault(away_team, season)

        # Reset the streak if the season has changed
        if home_seasons[home_team] != season:
            home_streaks[home_team] = 0
            home_seasons[home_team] = season
        if away_seasons[away_team] != season:
            away_streaks[away_team] = 0
            away_seasons[away_team] = season

        # Store the streaks in the DataFrame
        df.at[i, 'home_winning_streak'] = home_streaks[home_team]
        df.at[i, 'away_winning_streak'] = away_streaks[away_team]

        # Update the streaks based on the game result
        # If the home team won, increase their streak and reset the away team's streak
        if result == 'home':
            home_streaks[home_team] += 1
            away_streaks[away_team] = 0
        # If the away team won, increase their streak and reset the home team's streak
        elif result == 'away':
            away_streaks[away_team] += 1
            home_streaks[home_team] = 0
        # If it was a draw, reset both team's streaks
        else:
            home_streaks[home_team] = 0
            away_streaks[away_team] = 0

    return df


def rolling_avg(group, numeric_cols, n_rolling_average = 5):
        group[numeric_cols] = group[numeric_cols].rolling(window=n_rolling_average, min_periods=1, closed="left").mean()
        return group


def fetch_and_merge_data():
    df_matches = fetch_data_from_db('Matches')
    df_odds = fetch_data_from_db('Odds')
    df_match_statistics = fetch_data_from_db('Match_Statistics')
    
    df_merged_matches_odds = merge_match_odds(df_matches, df_odds)
    df_betting_info = add_betting_info(df_merged_matches_odds)
    df_added_winning_streak = add_winning_streak_info(df_betting_info)
    
    df_added_winning_streak['goal_difference'] = df_added_winning_streak['home_goals'] - df_added_winning_streak['away_goals']
    df_added_winning_streak['fixture_id'] = df_added_winning_streak['fixture_id'].astype('int64')
    return df_added_winning_streak.merge(df_match_statistics, on="fixture_id")


def clean_data(df):
    df = df.replace('-', np.nan)
    df = df.dropna(subset=['home_odds', 'draw_odds', 'away_odds'])
    
    index_of_col_xgoals = df.columns.get_loc("home_expected_goals")
    df = df.iloc[:, :index_of_col_xgoals]
    return df


def prepare_unified_dataset(df, home_stats, away_stats):
    home_df = df[['home_team', 'match_date'] + home_stats].rename(columns={'home_team': 'team', **{stat: stat.replace('home_', '') for stat in home_stats}})
    home_df['type'] = 'home'
    
    away_df = df[['away_team', 'match_date'] + away_stats].rename(columns={'away_team': 'team', **{stat: stat.replace('away_', '') for stat in away_stats}})
    away_df['type'] = 'away'
    
    return pd.concat([home_df, away_df], axis=0).sort_values(by=['team', 'match_date']).reset_index(drop=True)


def calculate_rolling_avg(all_teams_df, numeric_cols, n_rolling_average=5):
    return all_teams_df.groupby('team').apply(lambda group: rolling_avg(group, numeric_cols, n_rolling_average)).reset_index(drop=True)


def merge_home_away_with_original(df, home_stats_df, away_stats_df):
    df_with_form_stats = df.merge(home_stats_df, left_on=['home_team', 'match_date'], right_on=['team', 'match_date'], how='left').drop(columns=['team', 'type'])
    return df_with_form_stats.merge(away_stats_df, left_on=['away_team', 'match_date'], right_on=['team', 'match_date'], how='left').drop(columns=['team', 'type'])


def preprocess_data():
    df = fetch_and_merge_data()
    df = clean_data(df)
    
    home_stats, away_stats = get_home_away_stats()
    all_teams_df = prepare_unified_dataset(df, home_stats, away_stats)
    numeric_cols = [col for col in all_teams_df.columns if all_teams_df[col].dtype != 'datetime64[ns]' and col not in ['team', 'type']]
    all_teams_df_grouped = calculate_rolling_avg(all_teams_df, numeric_cols)
    
    home_stats_df = get_renamed_df(all_teams_df_grouped, 'home', home_stats)
    away_stats_df = get_renamed_df(all_teams_df_grouped, 'away', away_stats)
    
    return merge_home_away_with_original(df, home_stats_df, away_stats_df)


def get_home_away_stats():
    home_stats = ["home_fouls", "home_corner_kicks", "home_offsides", "home_ball_possession", "home_yellow_cards", "home_red_cards", "home_goalkeeper_saves", "home_total_passes", "home_passes_accurate", "home_passes_percent"]
    away_stats = ["away_fouls", "away_corner_kicks", "away_offsides", "away_ball_possession", "away_yellow_cards", "away_red_cards", "away_goalkeeper_saves", "away_total_passes", "away_passes_accurate", "away_passes_percent"]
    return home_stats, away_stats



def get_renamed_df(all_teams_df_grouped, team_type, stats):
    stats_df = all_teams_df_grouped[all_teams_df_grouped['type'] == team_type].copy()
    cols_to_rename = {col: f'{team_type}_avg_{col}' for col in stats_df.columns if col not in ['team', 'match_date', 'type']}
    stats_df.rename(columns=cols_to_rename, inplace=True)
    return stats_df
