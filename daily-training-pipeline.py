import hopsworks
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.neighbors import KNeighborsRegressor
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import AdaBoostRegressor
from xgboost import XGBRegressor
from hsml.schema import Schema
from hsml.model_schema import ModelSchema
import json
import joblib
from datetime import datetime, timedelta

import os
import modal

LOCAL=False

if LOCAL == False:

   stub = modal.Stub("model training")
   image = modal.Image.debian_slim(python_version='3.9').pip_install(['hopsworks', 'requests',"datetime", "datasets","scikit-learn","matplotlib", "seaborn","xgboost"])
   @stub.function(image=image, schedule=modal.Period(hours=1), secret=modal.Secret.from_name("HOPSWORKS_API_KEY"))
   def f():
       g()


# Function to train models and evaluate performance
def train_and_evaluate_models(X_train, y_train, X_test, y_test):
   models = {
      'KNN': KNeighborsRegressor(),
      'Linear Regression': LinearRegression(),
      'AdaBoost Regression': AdaBoostRegressor(),
      'XGBoost': XGBRegressor()
   }

   results = {}

   for name, model in models.items():
      model.fit(X_train, y_train)
      y_pred = model.predict(X_test)
      rmse = mean_squared_error(y_test, y_pred, squared=False)
      r2 = r2_score(y_test, y_pred)
      results[name] = {'RMSE': rmse, 'R2 Score': r2}

   return results


# Function to find the best model and update it in the Model Registry
def find_best_model(results):
   best_model_name = min(results, key=lambda x: results[x]['RMSE'])
   best_model = results[best_model_name]
   return best_model, best_model_name


def g():

   # Load data and create a feature view
   project = hopsworks.login()
   fs = project.get_feature_store()
   # Load data from feature groups
   traffic_fg = fs.get_feature_group('hourly_traffic_features', version=1)
   weather_fg = fs.get_feature_group('hourly_weather_features', version=1)
   # Join traffic and weather data on ['day', 'hour']
   data = traffic_fg.read().merge(weather_fg.read(), on=['day', 'hour'])

   # Join traffic and weather data on ['day', 'hour']
   query = traffic_fg.select_all().join(weather_fg.select_all(), on=['day', 'hour'])
   # Create and update feature view
   feature_view = fs.get_or_create_feature_view(
      name="traffic_weather",
      description="Read from traffic_weather dataset",
      labels=["current_speed"],
      version=1,
      query=query)

   # Load historical data from CSV files
   traffic_train_path = 'https://raw.githubusercontent.com/FlyingFish760/ID2223_project/main/historical_dataset/traffic_trainset_hour.csv'
   weather_train_path = 'https://raw.githubusercontent.com/FlyingFish760/ID2223_project/main/historical_dataset/weather_trainset.csv'
   traffic_train = pd.read_csv(traffic_train_path)
   weather_train = pd.read_csv(weather_train_path)

   # Join traffic and weather historical data on ['day', 'hour']
   historical_dataset = traffic_train.merge(weather_train, on=['day', 'hour'])
   # Check for missing values and fill them
   historical_dataset = historical_dataset.ffill()
   # Fill any remaining missing values with mean
   historical_dataset = historical_dataset.fillna(historical_dataset.mean())
   # Merge historical dataset with real-time data
   train_data = pd.concat([data, historical_dataset])

   # Extract features and target
   X_train = train_data.drop(['free_flow_speed', 'weekend', 'confidence', 'day', 'current_speed'], axis=1)
   y_train = train_data['current_speed']

   print(X_train)

   # Load test data from CSV files
   traffic_testset_path = 'https://raw.githubusercontent.com/FlyingFish760/ID2223_project/main/historical_dataset/traffic_testset.csv'
   weather_testset_path = 'https://raw.githubusercontent.com/FlyingFish760/ID2223_project/main/historical_dataset/weather_testset.csv'

   traffic_test = pd.read_csv(traffic_testset_path)
   weather_test = pd.read_csv(weather_testset_path)

   # Join traffic and weather test data on ['day', 'hour']
   test_data = traffic_test.merge(weather_test, on=['day', 'hour'])

   # Extract features and target for testing
   X_test = test_data.drop(['free_flow_speed','weekend','confidence','day', 'current_speed'],axis=1)
   y_test = test_data['current_speed']

   print("X_test shape:", X_test.shape)
   print("y_test shape:", y_test.shape)

   # Train models and evaluate performance
   results = train_and_evaluate_models(X_train, y_train, X_test, y_test)

   # Print results
   print("Model Performances:")
   for name, metrics in results.items():
      print(f"{name}: RMSE  {metrics['RMSE']:.2f}, R2 Score  {metrics['R2 Score']:.2f}")

   # Find the best model and update it in the Model Registry
   best_model, best_model_name = find_best_model(results)


   # Upload the best model to the Hopsworks Model Registry
   mr = project.get_model_registry()

   # Create a directory for the model
   model_dir = "traffic_weather_model"
   if os.path.isdir(model_dir) == False:
      os.mkdir(model_dir)

   # Save the best model
   best_model_path = model_dir + '/best_model.joblib'
   joblib.dump(best_model, best_model_path)

   # Specify the schema of the model's input/output
   input_schema = Schema(X_train)  # Replace with the actual input schema
   output_schema = Schema(y_train)  # Replace with the actual output schema
   model_schema = ModelSchema(input_schema, output_schema)

   # Create an entry in the model registry
   traffic_weather_model = mr.python.create_model(
      name="traffic_weather_model",
      metrics={
         'RMSE': float(results[best_model_name]['RMSE']),
         'R2 Score': float(results[best_model_name]['R2 Score'])
      },
      model_schema=model_schema,
      description="Traffic Flow Predictor"
   )

   # Upload the model to the model registry
   traffic_weather_model.save(model_dir)

   print("Model and metrics saved to:", model_dir)

if __name__ == "__main__":
   if LOCAL == True:
      g()
   else:
      with stub.run():
         f()