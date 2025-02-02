from include.utils.tasks import extract_all_data,transform_data,insert_data_for_station
from pymongo import MongoClient

def main():
    STATION_IDS = [283164601, 283181971]
    DB_NAMES = ["Station_1", "Station_2"]

    client = MongoClient("mongodb://yoyo:yoyo12@localhost:27017")
    
    for i, station_id in enumerate(STATION_IDS):
        try:
            # Extraction des données
            df_station = extract_all_data(station_id)
            df_station['station_id'] = station_id  # Ajouter l'ID de la station
            
            # Transformation des données
            daily_avg = transform_data(df_station)
            
            # Insertion dans MongoDB
            insert_data_for_station(daily_avg, DB_NAMES[i], client)
        
        except Exception as e:
            print(f"Erreur lors du traitement de la station {station_id}: {e}")

if __name__ == "__main__":
    main()
