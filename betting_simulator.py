from data_preprocessor import format_result
import numpy as np

def simulate_betting(df, model, X_test, stake=10):
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

