import requests
import pandas as pd
from pymongo import MongoClient

client = MongoClient("mongodb://yoyo:yoyo12@host.docker.internal:27017")

def extract_all_data(station_id):
    """
    Extrait les données de l'API et retourne un DataFrame.
    """
    url = 'https://airqino-api.magentalab.it/v3/getStationHourlyAvg/'
    api_url = f"{url}{station_id}"
    response = requests.get(api_url)
    
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data["data"])
        return df
    else:
        raise Exception(f"Erreur API : {response.status_code}")



def transform_data(df_station):
    """
    Effectue les transformations nécessaires sur les données.
    """
    
    df_station['timestamp'] = pd.to_datetime(df_station['timestamp'])
    
    # Extraire la date sans l'heure pour l'agrégation par jour
    df_station['date'] = df_station['timestamp'].dt.date
    
    # Calculer les moyennes quotidiennes pour CO et PM2.5
    daily_avg = df_station.groupby('date')[['CO', 'PM2.5']].mean()
    return daily_avg


def insert_data_for_station(daily_avg, nom_station):
    """
    Insère les données agrégées dans MongoDB.
    """
    db_name = f"{nom_station}"  # Nom de la base de données
    db = client[db_name]         # Accéder à la base et à la collection
    collection = db["daily_averages"]
    
    # Convertir la colonne "date" en champ normal et en format string
    daily_avg = daily_avg.reset_index()  # Assure que "date" devient une colonne normale
    daily_avg["date"] = daily_avg["date"].astype(str)

    # Convertir le DataFrame en liste de dictionnaires
    records = daily_avg.to_dict('records')

    # Insertion des données dans MongoDB
    collection.insert_many(records)
    print(f"Données insérées pour la station {nom_station} dans la base '{db_name}'.")
