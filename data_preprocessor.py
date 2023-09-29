import pandas as pd
import numpy as np
from db_manager import fetch_data_from_db


def get_favorite(row):
    """
    Returns the favorite team, the team with the best odds.
    
    Parameters
    ----------
    row : pd.Series
        Row from a DataFrame containing home and away odds.
        
    Returns
    -------
    str
        'home' if home team is the favorite, otherwise 'away'.
    """
    if row['home_odds'] < row['away_odds']:
        return 'home'
    else:
        return 'away'


def get_underdog(row):
    """
    Returns the underdog team, the team with the worst odds.
    
    Parameters
    ----------
    row : pd.Series
        Row from a DataFrame containing home and away odds.
        
    Returns
    -------
    str
        'home' if home team is the underdog, otherwise 'away'.
    """
    if row['home_odds'] > row['away_odds']:
        return 'home'
    else:
        return 'away'


def get_result(row):
    """
    Calculates the result of the game based on number of goals.
    
    Parameters
    ----------
    row : pd.Series
        Row from a DataFrame containing home and away goals.
        
    Returns
    -------
    str
        'home' if home team won, 'away' if away team won, and 'draw' if draw.
    """
    if row['home_goals'] > row['away_goals']:
        return 'home'
    elif row['home_goals'] < row['away_goals']:
        return 'away'
    else:
        return 'draw'


def calculate_margin(row, bet_target, bet_amount=10):
    """
    Calculates the win or loss margin based on bet target and amount.
    
    Parameters
    ----------
    row : pd.Series
        Row from a DataFrame containing the necessary fields to calculate margin.
    bet_target : str
        The target of the bet. Can be 'draw', 'favorite', or 'underdog'.
    bet_amount : int, optional
        The amount of the bet, by default 10.
        
    Returns
    -------
    float or None
        The profit or loss from the bet.
    """
    try:
        if bet_target == 'draw':
            if row['result'] == 'X':
                return bet_amount * float(row['draw_odds']) - bet_amount
            else:
                return -bet_amount
        elif bet_target == 'favorite':
            odds = row['home_odds'] if row['favorite'] == 'home' else row['away_odds']
            if row['favorite'] == row['result']:
                return bet_amount * float(odds) - bet_amount
            else:
                return -bet_amount
        else:
            odds = row['home_odds'] if row['underdog'] == 'home' else row['away_odds']
            if row['underdog'] == row['result']:
                return bet_amount * float(odds) - bet_amount
            else:
                return -bet_amount
    except ValueError as e:
        return None


def add_betting_info(df):
    """
    Adds columns for favorite, underdog, and result to the DataFrame.
    
    Parameters
    ----------
    df : pd.DataFrame
        A DataFrame containing match data.
        
    Returns
    -------
    pd.DataFrame
        A new DataFrame with additional columns: 'favorite', 'underdog', and 'result' representing the favorite team, underdog team, and result of the match respectively.
    """
    df = df.copy()  # make a copy of the dataframe to avoid changing the original dataframe
    df['favorite'] = df.apply(get_favorite, axis=1)
    df['underdog'] = df.apply(get_underdog, axis=1)
    df['result'] = df.apply(get_result, axis=1)
    return df


def bet_on(df, bet_target, bet_amount = 10):
    """
    Applies betting on DataFrame and calculates winning margin.

    Parameters
    ----------
    df : pd.DataFrame
        A DataFrame containing match data.
    bet_target : str
        The betting target, can be 'favorite', 'underdog', or 'draw'.
    bet_amount : int, optional
        The amount to bet, default is 10.
        
    Returns
    -------
    pd.DataFrame
        A new DataFrame with an additional column representing the margin of winning or losing for the betting.
    """
    df['win_margin_bet_on_'+bet_target] = df.apply(calculate_margin, args=(bet_target, bet_amount), axis=1)
    return df



def add_winning_streak_info(df):
    """
    Adds winning streak information to a DataFrame with match data.

    This function computes the winning streaks of home and away teams for each match in the input DataFrame. 
    The winning streak is a running count of consecutive wins by a team, resetting to 0 whenever the team 
    experiences a loss or starts a new season. This additional information can provide insights into the 
    form of the teams leading up to a match.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with matches data sorted by date. It should include columns 'home_team', 'away_team', 
        'result', and 'season', representing the participating teams, match result, and the season 
        respectively.
        
    Returns
    -------
    pd.DataFrame
        A DataFrame with two new columns: 'home_winning_streak' and 'away_winning_streak' representing 
        the winning streak of home and away teams. The winning streak is reset to 0 at the end of each 
        season or after a loss, and it is incremented after a win.
        
    """
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


def calculate_rolling_avg(group, numeric_cols, n_rolling_average = 5):
    """
    Computes rolling averages for specified numeric columns in a DataFrame group.
    
    Parameters
    ----------
    group : pd.DataFrame
        A group or DataFrame for which the rolling average needs to be calculated.
        
    numeric_cols : list
        A list of strings representing the names of the numeric columns in the DataFrame
        for which the rolling average is to be computed.
        
    n_rolling_average : int, optional, default=5
        The window size for the rolling average calculation. Represents the number of rows
        included in each average calculation.
    """   
    group[numeric_cols] = group[numeric_cols].rolling(window=n_rolling_average, min_periods=1, closed="left").mean()
    return group


def merge_match_odds(df_matches, df_odds):
    """
    Merges match data with odds data based on fixture_id.
    
    Parameters
    ----------
    df_matches : pd.DataFrame
        DataFrame containing match data.
    df_odds : pd.DataFrame
        DataFrame containing odds data.
        
    Returns
    -------
    pd.DataFrame
        A merged DataFrame containing matches and odds information.
        
    Summary
    -------
    This function performs an inner merge of match and odds data on 'fixture_id', providing a unified view of both datasets.
    """
    df_merged_matches_odds = df_matches.merge(df_odds[['fixture_id', 'home_odds', 'draw_odds', 'away_odds']], on='fixture_id', how='inner')
    return df_merged_matches_odds

def fetch_and_merge_data():
    """
    This function fetches matches, odds, and match statistics data separately from the database and 
    performs multiple steps of data processing and merging to generate a consolidated DataFrame. 
    
    It calls several other functions to merge match and odds data, calculate betting information, 
    add winning streak information, compute goal difference, and format the 'fixture_id' column. 
    The resulting DataFrame provides a rich set of information which can be used for further analysis 
    or model development in predicting football match outcomes.
    
    Returns
    -------
    pd.DataFrame
        A merged DataFrame containing comprehensive match information including matches data, 
        odds data, match statistics, betting information, winning streak information, goal difference, 
        and fixture_id as int64.
        
    """
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
    """
    Cleans and processes the input DataFrame by removing unwanted rows and columns and making necessary adjustments.
    
    Parameters
    ----------
    df : pd.DataFrame
        A DataFrame to be cleaned, which contains columns including 'home_odds', 'draw_odds', 'away_odds', and 
        'home_expected_goals' among others.
        
    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with necessary adjustments and removal of unwanted rows and columns.
    """
    df = df.replace('-', np.nan)
    df = df.dropna(subset=['home_odds', 'draw_odds', 'away_odds'])
    
    index_of_col_xgoals = df.columns.get_loc("home_expected_goals")
    df = df.iloc[:, :index_of_col_xgoals]
    return df


def prepare_unified_dataset(df, home_stats, away_stats):
    """
    Prepares a unified dataset from separate home and away data, sorting by team and match date.
    
    Parameters
    ----------
    df : pd.DataFrame
        The original DataFrame containing separate home and away team statistics.
        
    home_stats : List[str]
        A list of column names representing statistics related to home teams.
        
    away_stats : List[str]
        A list of column names representing statistics related to away teams.
        
    Returns
    -------
    pd.DataFrame
        A unified DataFrame containing both home and away team data, sorted by team and match date.
    """
    home_df = df[['home_team', 'match_date'] + home_stats].rename(columns={'home_team': 'team', **{stat: stat.replace('home_', '') for stat in home_stats}})
    home_df['type'] = 'home'
    
    away_df = df[['away_team', 'match_date'] + away_stats].rename(columns={'away_team': 'team', **{stat: stat.replace('away_', '') for stat in away_stats}})
    away_df['type'] = 'away'
    
    return pd.concat([home_df, away_df], axis=0).sort_values(by=['team', 'match_date']).reset_index(drop=True)


def merge_home_away_with_original(df, home_stats_df, away_stats_df):
    """
    Merges home and away statistics DataFrames with the original DataFrame.
    
    Parameters
    ----------
    df : pd.DataFrame
        Original DataFrame containing match information.
        
    home_stats_df : pd.DataFrame
        DataFrame containing processed home team statistics.
        
    away_stats_df : pd.DataFrame
        DataFrame containing processed away team statistics.
        
    Returns
    -------
    pd.DataFrame
        A DataFrame containing the merged home and away statistics with the original match information.
    """
    df_with_form_stats = df.merge(home_stats_df, left_on=['home_team', 'match_date'], right_on=['team', 'match_date'], how='left').drop(columns=['team', 'type'])
    return df_with_form_stats.merge(away_stats_df, left_on=['away_team', 'match_date'], right_on=['team', 'match_date'], how='left').drop(columns=['team', 'type'])


def format_result(x):
    """
    Converts the match result into a numerical representation.
    
    Parameters
    ----------
    x : str
        A string representing the match result: 'home', 'draw', or 'away'.
        
    Returns
    -------
    int
        An integer representing the match result: 0 for home, 1 for draw, and 2 for away.
    """
    if(x == "home"):
        return 0
    elif(x == "draw"):
        return 1
    else:
        return 2

def preprocess_data():
    """
    Executes a series of data preprocessing steps 
    including fetching and merging data, cleaning, and performing feature engineering, 
    to prepare the data for further analysis and modeling.
    
    Returns
    -------
    pd.DataFrame
        A preprocessed and cleaned DataFrame ready for modeling.
    """
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
    """
    Gets lists of column names related to home and away team statistics.
    
    Returns
    -------
    Tuple[List[str], List[str]]
        A tuple containing two lists of string: 
        - The first list contains the names of columns related to home team statistics.
        - The second list contains the names of columns related to away team statistics.
    """
    home_stats = ["home_fouls", "home_corner_kicks", "home_offsides", "home_ball_possession", "home_yellow_cards", "home_red_cards", "home_goalkeeper_saves", "home_total_passes", "home_passes_accurate", "home_passes_percent"]
    away_stats = ["away_fouls", "away_corner_kicks", "away_offsides", "away_ball_possession", "away_yellow_cards", "away_red_cards", "away_goalkeeper_saves", "away_total_passes", "away_passes_accurate", "away_passes_percent"]
    return home_stats, away_stats



def get_renamed_df(all_teams_df_grouped, team_type):
    """
    This function renames the columns of a DataFrame containing team statistics by 
    prefixing them with the team type and ‘_avg_’ to indicate that they represent average values.
    
    Parameters
    ----------
    all_teams_df_grouped : pd.DataFrame
        DataFrame containing grouped team statistics.
        
    team_type : str
        A string indicating the type of team: 'home' or 'away'.
        
    Returns
    -------
    pd.DataFrame
        A DataFrame with renamed columns representing average team statistics.
    """
    stats_df = all_teams_df_grouped[all_teams_df_grouped['type'] == team_type].copy()
    cols_to_rename = {col: f'{team_type}_avg_{col}' for col in stats_df.columns if col not in ['team', 'match_date', 'type']}
    stats_df.rename(columns=cols_to_rename, inplace=True)
    
    return stats_df
