from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from keras.models import Sequential
from keras.layers import Dense
from keras.optimizers import Adam
from sklearn.preprocessing import LabelEncoder

import mlflow
import mlflow.sklearn

import mlflow.keras  
from datetime import datetime


def train_and_evaluate_models(X_train, y_train, X_test, y_test):
    models = {
        'Logistic Regression': LogisticRegression(max_iter=1000),
        'Decision Tree': DecisionTreeClassifier(random_state=42),
        'Random Forest': RandomForestClassifier(random_state=42)
    }
    
    for name, model in models.items():
        with mlflow.start_run(nested=True, run_name=name):  # Start a new nested MLflow run for each model
            # Train model
            model.fit(X_train, y_train)
            y_pred_train = model.predict(X_train)
            y_pred_test = model.predict(X_test)
            acc_train = accuracy_score(y_train, y_pred_train)
            acc_test = accuracy_score(y_test, y_pred_test)

            # Log parameters, metrics, and model
            mlflow.log_param('model_name', name)
            mlflow.log_metric('training_accuracy', acc_train)
            mlflow.log_metric('test_accuracy', acc_test)
            mlflow.sklearn.log_model(model, f"{name}_model")


def train_neural_network(X_train, y_train, X_test, y_test):
    with mlflow.start_run(nested=True, run_name="Neural Network"):  # Start a new nested MLflow run for Neural Network
        encoder = LabelEncoder()
        y_train_encoded = encoder.fit_transform(y_train)
        y_test_encoded = encoder.transform(y_test)
        
        model_nn = Sequential([
            Dense(32, activation='relu', input_shape=(X_train.shape[1],)),
            Dense(16, activation='relu'),
            Dense(8, activation='relu'),
            Dense(len(encoder.classes_), activation='softmax')
        ])
        
        model_nn.compile(optimizer=Adam(0.001), loss='sparse_categorical_crossentropy', metrics=['accuracy'])
        model_nn.fit(X_train, y_train_encoded, epochs=20, batch_size=32, validation_split=0.1, verbose=1)

        train_loss, train_acc = model_nn.evaluate(X_train, y_train_encoded, verbose=0)
        test_loss, test_acc = model_nn.evaluate(X_test, y_test_encoded, verbose=0)

        mlflow.log_metric('training_accuracy', train_acc)
        mlflow.log_metric('test_accuracy', test_acc)
        mlflow.keras.log_model(model_nn, "NN_model")



def log_results_mlflow(X_train, y_train, X_test, y_test):
    with mlflow.start_run(run_name=f"models_{datetime.now}"): 
        train_and_evaluate_models(X_train, y_train, X_test, y_test)
        train_neural_network(X_train, y_train, X_test, y_test)
