from data_preprocessor import preprocess_data, format_result
from models import log_results_mlflow_with_cv, single_feature_assessment_ml_models
from sklearn.model_selection import train_test_split

models_to_evaluate = ["Logistic Regression", "Decision Tree", "Random Forest", "XGBoost"]

def do_analysis():
    df = preprocess_data()

    target_feature = "result"

    features = ['home_winning_streak', 'away_winning_streak']
    
    # Additional rolling average stats
    rolling_avg_features = [
        "home_avg_fouls", "home_avg_corner_kicks", "home_avg_offsides", "home_avg_ball_possession",
        "home_avg_yellow_cards", "home_avg_red_cards", "home_avg_goalkeeper_saves",
        "home_avg_total_passes", "home_avg_passes_accurate", "home_avg_passes_percent",
        
        "away_avg_fouls", "away_avg_corner_kicks", "away_avg_offsides", "away_avg_ball_possession",
        "away_avg_yellow_cards", "away_avg_red_cards", "away_avg_goalkeeper_saves",
        "away_avg_total_passes", "away_avg_passes_accurate", "away_avg_passes_percent"
    ]

    features += rolling_avg_features
    print(df.columns)
    
    df = df.dropna(subset=features)
    X_train, X_test, y_train, y_test = train_test_split(df[features], df[target_feature], test_size=0.2, random_state=42)
    #log_results_mlflow(X_train, y_train, X_test, y_test, models_to_evaluate = models_to_evaluate)
    single_feature_assessment_ml_models(X_train, y_train, X_test, y_test, models_to_evaluate = models_to_evaluate)


if __name__ == "__main__":
    do_analysis()

