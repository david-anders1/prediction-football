from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

def train_and_evaluate_models(X_train, y_train, X_test, y_test):
    lr = LogisticRegression(max_iter=1000)
    dt = DecisionTreeClassifier(random_state=42)
    rf = RandomForestClassifier(random_state=42)
    
    models = [lr, dt, rf]
    
    for model in models:
        model.fit(X_train, y_train)
        y_pred_train = model.predict(X_train)
        y_pred_test = model.predict(X_test)

        acc_train = accuracy_score(y_train, y_pred_train)
        acc_test = accuracy_score(y_test, y_pred_test)

        print(f"Model: {model.__class__.__name__}")
        print(f"Training Accuracy: {acc_train}")
        print(f"Test Accuracy: {acc_test}\n")
