from sklearn.model_selection import cross_val_score
import numpy as np
import mlflow
import mlflow.sklearn
import mlflow.keras
import xgboost as xgb
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import LabelEncoder
from keras.models import Sequential
from keras.layers import Dense
from keras.optimizers import Adam
from datetime import datetime
from itertools import compress
import pandas as pd
from sklearn.feature_selection import RFECV
from sklearn.model_selection import StratifiedKFold



def single_feature_assessment_ml_models(X_train, y_train, X_test, y_test, models_to_evaluate=None, cv=5):
    """
    Evaluates models for every single feature and logs the results using mlflow, and includes cross-validation.
    """
    encoder = LabelEncoder()
    y_train_encoded = encoder.fit_transform(y_train)
    y_test_encoded = encoder.transform(y_test)
    
    models = {
        'Logistic Regression': LogisticRegression(max_iter=1000),
        'Decision Tree': DecisionTreeClassifier(random_state=42),
        'Random Forest': RandomForestClassifier(random_state=42),
        'XGBoost': xgb.XGBClassifier(use_label_encoder=False, eval_metric='mlogloss', random_state=42)
    }
    
    if models_to_evaluate:
        models = {k: v for k, v in models.items() if k in models_to_evaluate}
        
    for name, model in models.items():
        for feature in X_train.columns:
            with mlflow.start_run(nested=True, run_name=f"{name}_{feature}"):
                X_train_single = X_train[[feature]]
                X_test_single = X_test[[feature]]
                
                model.fit(X_train_single, y_train_encoded)
                
                # Perform cross-validation and calculate mean accuracy
                cv_scores = cross_val_score(model, X_train_single, y_train_encoded, cv=cv, scoring='accuracy')
                acc_cv = np.mean(cv_scores)
                
                y_pred_train = model.predict(X_train_single)
                y_pred_test = model.predict(X_test_single)
                
                acc_train = accuracy_score(y_train_encoded, y_pred_train)
                acc_test = accuracy_score(y_test_encoded, y_pred_test)
                
                mlflow.log_param('model_name', name)
                mlflow.log_param('single_feature', feature)
                mlflow.log_metric('training_accuracy', acc_train)
                mlflow.log_metric('test_accuracy', acc_test)
                mlflow.log_metric('cross_val_accuracy', acc_cv)
                
                if name == 'XGBoost':
                    mlflow.xgboost.log_model(model, f"{name}_{feature}_model")
                else:
                    mlflow.sklearn.log_model(model, f"{name}_{feature}_model")


def single_feature_assessment_nn(X_train, y_train, X_test, y_test, cv=5):
    encoder = LabelEncoder()
    y_train_encoded = encoder.fit_transform(y_train)
    y_test_encoded = encoder.transform(y_test)
        
    for feature in X_train.columns:
        with mlflow.start_run(nested=True, run_name=f"NN_{feature}"):
            
            X_train_single = X_train[[feature]]
            X_test_single = X_test[[feature]]
            
            model_nn = Sequential([
                Dense(32, activation='relu', input_shape=(X_train_single.shape[1],)),
                Dense(16, activation='relu'),
                Dense(8, activation='relu'),
                Dense(len(encoder.classes_), activation='softmax')
            ])

            model_nn.compile(optimizer=Adam(0.001), loss='sparse_categorical_crossentropy', metrics=['accuracy'])

            val_accuracy = []
            val_size = len(X_train_single) // cv
            for i in range(cv):
                val_data = X_train_single[i * val_size: (i + 1) * val_size]
                val_targets = y_train_encoded[i * val_size: (i + 1) * val_size]

                partial_train_data = np.concatenate([X_train_single[:i * val_size], X_train_single[(i + 1) * val_size:]], axis=0)
                partial_train_targets = np.concatenate([y_train_encoded[:i * val_size], y_train_encoded[(i + 1) * val_size:]], axis=0)

                history = model_nn.fit(partial_train_data, partial_train_targets, epochs=20, batch_size=32, validation_data=(val_data, val_targets), verbose=0)
                val_accuracy.append(np.mean(history.history['val_accuracy']))

            acc_cv = np.mean(val_accuracy)
            train_loss, train_acc = model_nn.evaluate(X_train_single, y_train_encoded, verbose=0)
            test_loss, test_acc = model_nn.evaluate(X_test_single, y_test_encoded, verbose=0)

            mlflow.log_param('single_feature', feature)
            mlflow.log_metric('training_accuracy', train_acc)
            mlflow.log_metric('test_accuracy', test_acc)
            mlflow.log_metric('cross_val_accuracy', acc_cv)
            mlflow.keras.log_model(model_nn, f"NN_{feature}_model")
            


def train_and_evaluate_models_with_cv(X_train, y_train, X_test, y_test, models_to_evaluate=None, cv=5):
    encoder = LabelEncoder()
    y_train_encoded = encoder.fit_transform(y_train)
    y_test_encoded = encoder.transform(y_test)
    
    models = {
        'Logistic Regression': LogisticRegression(max_iter=1000),
        'Decision Tree': DecisionTreeClassifier(random_state=42),
        'Random Forest': RandomForestClassifier(random_state=42),
        'XGBoost': xgb.XGBClassifier(use_label_encoder=False, eval_metric='mlogloss', random_state=42)
    }
    
    if models_to_evaluate:
        models = {k: v for k, v in models.items() if k in models_to_evaluate}
    
    for name, model in models.items():
        with mlflow.start_run(nested=True, run_name=name):
            # Using StratifiedKFold to maintain the distribution of target class during cross-validation
            strat_k_fold = StratifiedKFold(n_splits=cv)
            
            # Applying RFE with cross-validation, starting with all features.
            selector = RFECV(estimator=model, step=1, cv=strat_k_fold, scoring='accuracy')
            selector = selector.fit(X_train, y_train_encoded)
            
            # Getting the features that are selected by RFE
            selected_features = list(compress(X_train.columns.tolist(), selector.support_))

            # Writing the selected features to a file
            with open("selected_features.txt", "w") as f:
                for feature in selected_features:
                    f.write("%s\n" % feature)
                    
            # Logging the file as an artifact
            mlflow.log_artifact("selected_features.txt")

            # Training the model with selected features
            X_train_selected = X_train[selected_features]
            X_test_selected = X_test[selected_features]

            model.fit(X_train_selected, y_train_encoded)
            
            y_pred_train = model.predict(X_train_selected)
            y_pred_test = model.predict(X_test_selected)
            acc_train = accuracy_score(y_train_encoded, y_pred_train)
            acc_test = accuracy_score(y_test_encoded, y_pred_test)

            # Logging parameters and metrics to MLflow
            mlflow.log_param('model_name', name)
            mlflow.log_metric('training_accuracy', acc_train)
            mlflow.log_metric('test_accuracy', acc_test)
            mlflow.log_metric('num_selected_features', len(selected_features))
            mlflow.log_metric('cross_val_accuracy', selector.cv_results_['mean_test_score'].mean())
            
            if name == 'XGBoost':
                mlflow.xgboost.log_model(model, f"{name}_model")
            else:
                mlflow.sklearn.log_model(model, f"{name}_model")


def train_neural_network_with_cv(X_train, y_train, X_test, y_test, cv=5):
    with mlflow.start_run(nested=True, run_name="Neural Network"): 
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
        
        val_accuracy = []
        val_size = len(X_train) // cv
        for i in range(cv):
            val_data = X_train[i * val_size: (i + 1) * val_size]
            val_targets = y_train_encoded[i * val_size: (i + 1) * val_size]
            
            partial_train_data = np.concatenate([X_train[:i * val_size], X_train[(i + 1) * val_size:]], axis=0)
            partial_train_targets = np.concatenate([y_train_encoded[:i * val_size], y_train_encoded[(i + 1) * val_size:]], axis=0)

            history = model_nn.fit(partial_train_data, partial_train_targets, epochs=20, batch_size=32, validation_data=(val_data, val_targets), verbose=0)
            val_accuracy.append(np.mean(history.history['val_accuracy']))
            
        acc_cv = np.mean(val_accuracy)
        train_loss, train_acc = model_nn.evaluate(X_train, y_train_encoded, verbose=0)
        test_loss, test_acc = model_nn.evaluate(X_test, y_test_encoded, verbose=0)

        mlflow.log_metric('training_accuracy', train_acc)
        mlflow.log_metric('test_accuracy', test_acc)
        mlflow.log_metric('cross_val_accuracy', acc_cv)
        mlflow.keras.log_model(model_nn, "NN_model")


def log_results_mlflow_with_cv(X_train, y_train, X_test, y_test, models_to_evaluate=None, evaluate_nn=True, cv=5):
    with mlflow.start_run(run_name=f"models_{datetime.now()}"):
        if(models_to_evaluate is not None):
            train_and_evaluate_models_with_cv(X_train, y_train, X_test, y_test, models_to_evaluate=models_to_evaluate, cv=cv)

        if(evaluate_nn):
            train_neural_network_with_cv(X_train, y_train, X_test, y_test, cv=cv)

