from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
import requests
from datetime import datetime
from include.utils.tasks import extract_all_data,insert_data_for_station, transform_data



STATION_ID = 283164601  # ID de la station
DB_NAME = "Station_pi"   # Nom de la base MongoDB



@dag(
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["air_quality"]
)
def etl_air_quality_station_1():
    
    @task.sensor(poke_interval=30, timeout=300, mode="poke")
    def is_api_available() -> PokeReturnValue:
        """ Vérifie si l'API est disponible """
        # Récupérer la connexion Airflow
        api = BaseHook.get_connection('air_quality_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}/{STATION_ID}"  # Ajouter l'ID de la station
        headers = api.extra_dejson.get('headers', {})  # Récupérer les en-têtes
        response = requests.get(url, headers=headers)
        condition = response.status_code == 200
        return PokeReturnValue(is_done=condition, xcom_value=url)

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_all_data,
        op_kwargs={"station_id": STATION_ID},  # Paramètres pour la fonction
    )

    
    # Transformation des données

    @task
    def transform_station(df_station):
        """ Fonction de transformation des données """
        # Appeler la fonction transform_data ici
        return transform_data(df_station)

    
    

    # Chargement des données dans MongoDB
    # Tâche de chargement des données dans MongoDB
    @task
    def load_task(transformed_data):
        """ Chargement des données dans MongoDB """
        insert_data_for_station(transformed_data, DB_NAME)

    
    transformed_result = transform_station(extract_task.output)


    # Utilisation des tâches
    api_check = is_api_available()
    

    api_check >> extract_task >> transformed_result >> load_task(transformed_result)
   

etl_air_quality_station_1()
