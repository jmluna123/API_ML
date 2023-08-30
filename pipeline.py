import datetime
import pandas as pd
from conection import get_data

from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import joblib

MODEL = joblib.load("./models/agg_clust_5PC_60min.joblib")
NORMAL_CLUSTER = [1,3,4]

ANOMAL_COLOR = "#dc3545"
NORMAL_COLOR = "#28a745"

MIN_WINDOW = 60
COLUMNS = [
      "dtm_start",
      "dtm_end",
      "month",
      "weekday",
      "value",
      "mean",
      "median",
      "std",
      "min",
      "max",
      "value_k1",
      "mean_k1",
      "median_k1",
      "std_k1",
      "min_k1",
      "max_k1",
      "value_k2",
      "mean_k2",
      "median_k2",
      "std_k2",
      "min_k2",
      "max_k2"
    ]

data = []

top_data = {
  "last_dtm": "",
  "building_id": "11A",
  "daily_consume": 0,
  "daily_anomalies": 0,
  "count": 0,
}

global last_date
last_date = datetime.datetime.today()
last_date = datetime.datetime(last_date.year, last_date.month, last_date.day, last_date.hour, last_date.minute)

def prediction(df):
  #normalización
  df_X = df.loc[:, 'month':].values
  scaler = StandardScaler().fit(df_X)
  scaler_normalized = scaler.transform(df_X)
  
  #PCA
  df_pca = PCA(n_components=5).fit_transform(scaler_normalized)
  
  #prediction
  y = MODEL.fit_predict(df_pca)
  
  return y

def preprocess_data(df, df_update, start_date, end_date, r_filename="data.csv"):
  i = 0
  
  if len(df_update.index) > 0:
    i = len(df_update.index) -1
  
  while start_date < end_date:
    date = start_date + datetime.timedelta(minutes=MIN_WINDOW)
    weekno = date.weekday()
    
    mask = (df["dtm"] > start_date) & (df["dtm"] <= date)
    df_tmp = df.loc[mask]

    tmp_value = df_tmp["value"].sum() if not df_tmp.empty else 0.0
    tmp_mean = df_tmp["value"].mean() if not df_tmp.empty else 0.0
    tmp_median = df_tmp["value"].median() if not df_tmp.empty else 0.0
    tmp_std = df_tmp["value"].std() if not df_tmp.empty else 0.0
    tmp_min = df_tmp["value"].min() if not df_tmp.empty else 0.0
    tmp_max = df_tmp["value"].max() if not df_tmp.empty else 0.0

    df_update.loc[i] = [
        start_date,
        date,
        date.month,
        weekno,
        tmp_value,
        tmp_mean,
        tmp_median,
        tmp_std,
        tmp_min,
        tmp_max,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    ]

    if i - 1 >= 0:
      df_update.at[i - 1, "value_k1"] = tmp_value
      df_update.at[i - 1, "mean_k1"] = tmp_mean
      df_update.at[i - 1, "median_k1"] = tmp_median
      df_update.at[i - 1, "std_k1"] = tmp_std
      df_update.at[i - 1, "min_k1"] = tmp_min
      df_update.at[i - 1, "max_k1"] = tmp_max
    if i - 2 >= 0:
      df_update.at[i - 2, "value_k2"] = tmp_value
      df_update.at[i - 2, "mean_k2"] = tmp_mean
      df_update.at[i - 2, "median_k2"] = tmp_median
      df_update.at[i - 2, "std_k2"] = tmp_std
      df_update.at[i - 2, "min_k2"] = tmp_min
      df_update.at[i - 2, "max_k2"] = tmp_max
  
    start_date = date
    i = len(df_update.index)
  df_update.to_csv(r_filename, index=False)

def classificate_data(filename):
  df = pd.read_csv( "./files/data-" + filename)
  
  results = {
    "first_dtm": str(df.loc[0, "dtm_start"]),
    "last_dtm": str(df.loc[len(df)-1, "dtm_end"]),
    "daily_anomalies": 0,
    "consume": 0,
    "data": []
  }
  
  y = prediction(df)
  anomalies = 0
  consume = 0
  
  for i in range(len(df.index)):
    tmp_value = df.loc[i, 'value']
    tmp_dtm = df.loc[i, 'dtm_end']
    y_pred = y[i]
    
    tmp_color = NORMAL_COLOR
    if y_pred not in NORMAL_CLUSTER:
      tmp_color = ANOMAL_COLOR
      anomalies = anomalies +1
    
    consume = consume + tmp_value
    
    results['daily_anomalies'] = anomalies
    results['consume'] = consume
    results['data'].append({'hour': tmp_dtm[-8:-3], 'value': tmp_value, 'color': tmp_color})
  
  return results
  
def initialize_df(df):
  df_update = pd.DataFrame(columns= COLUMNS)
    
  end_date = datetime.datetime(last_date.year, last_date.month, last_date.day, last_date.hour, last_date.minute)
  start_date = datetime.datetime(end_date.year, end_date.month, end_date.day)
  
  preprocess_data(df, df_update, start_date, end_date)

def initialize_json():
  df = pd.read_csv('data.csv')
  data.clear()

  anomalies = 0
  consume = 0
  count = 0
  
  y = prediction(df)
  
  for i in range(len(df.index)):
    tmp_value = df.loc[i, 'value']
    tmp_dtm = df.loc[i, 'dtm_end']
    y_pred = y[i]
    
    tmp_color = NORMAL_COLOR
    if y_pred not in NORMAL_CLUSTER:
      tmp_color = ANOMAL_COLOR
      anomalies = anomalies +1
    
    consume = consume + tmp_value
    count = count + 1

    data.append({'hour': tmp_dtm[-8:] ,'value': tmp_value, 'color': tmp_color})

  top_data['daily_consume'] = consume
  top_data['daily_anomalies'] = anomalies
  top_data['count'] = count
  top_data['last_dtm'] = str(last_date.date())
  
def obtain_anomalies(filename):
  df = pd.read_csv("./files/" + filename)
  
  df["dtm"] = pd.to_datetime(df["dtm"])
  
  start_dtm = df.loc[0, 'dtm']
  end_dtm = df.loc[len(df) -1, 'dtm']
  
  start_date = datetime.datetime(start_dtm.year,start_dtm.month,start_dtm.day, start_dtm.hour)
  end_date = datetime.datetime(end_dtm.year, end_dtm.month, end_dtm.day, end_dtm.hour, end_dtm.minute)
  
  df_update = pd.DataFrame(columns= COLUMNS)
  
  preprocess_data(df, df_update, start_date, end_date, "./files/data-" + filename)
  return classificate_data(filename)
  

def initialize():
  get_data(last_date.date())
  
  df = pd.read_csv('tmp.csv')
  df["dtm"] = pd.to_datetime(df["dtm"])
  
  initialize_df(df)
  initialize_json()