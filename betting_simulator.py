from data_preprocessor import format_result
import numpy as np

def simulate_betting(df, model, X_test, stake=10):
    """
    Simulates betting using the given model and calculates the profit or loss.

    The function simulates betting on games using the predicted probabilities from the model
    and the actual odds from the DataFrame. The profit or loss for each bet is calculated based on 
    whether the actual outcome matches the predicted outcome and the corresponding odds.
    The total profit or loss from all bets is returned.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the odds and actual results of the games.
    model : Model object
        Trained model object which is capable of performing `predict_proba` on the test set.
    X_test : pd.DataFrame
        Test data features used for predicting probabilities using the model.
    stake : int, optional
        The amount of money to be bet on each game, by default 10.
        
    Returns
    -------
    float
        The total profit or loss after simulating the betting.
    """
    odds_columns = ['home_odds', 'draw_odds', 'away_odds']
    
    # Convert odds to implied probabilities
    implied_probabilities = 1 / df[odds_columns]
    actual_outcomes = df['result'].apply(lambda x: format_result(x))
    predicted_probabilities = model.predict_proba(X_test)
    
    profit = 0
    
    for idx, (actual, probs) in enumerate(zip(actual_outcomes, predicted_probabilities)):
        best_bet = np.argmax(probs)
        if probs[best_bet] > implied_probabilities.iloc[idx, best_bet]:
            if best_bet == actual:
                profit += stake * df.iloc[idx].loc[odds_columns[best_bet]] - stake
            else:
                profit -= stake
                
    return profit 

