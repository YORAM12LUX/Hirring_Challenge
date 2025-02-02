
# Hiring Challenge: Data Engineer (ETL Air Quality Project)

Data354 accompagne les organisations publiques et privées dans la valorisation de leurs données. Ce challenge vise à démontrer mes compétences en :

- Analyse métier et compréhension des besoins
- Extraction et transformation des données
- Programmation en Python et orchestration d’ETL
- Stockage des données (MongoDB)
- Visualisation des données (Apache Superset)
(Optionnel) Prévision avec un modèle de Machine Learning

## Solution 
La solution se décompose en plusieurs modules :

- Extraction : Récupération des données horaires depuis l’API AirQino deux  capteurs.
- Transformation : Calcul des moyennes journalières de CO et PM2.5.
- Chargement : Stockage des résultats dans des bases de données MongoDB distinctes pour chaque capteur.
- Visualisation : Création d’un dashboard interactif dans Apache Superset pour visualiser les tendances.
(Optionnel) Forecasting : Modèle de Machine Learning pour prédire les valeurs sur les prochaines heures.



## Architectures du projet

Création d'une pipeline orchestré avec Airflow , et la gestion des dépendances entre les différentes tâches est assurée par des opérateurs Python. Les fonctions d'extraction, de transformation et de chargement sont regroupées dans un module utilitaire.

Le projet s'appuie sur les technologies et services suivants :

- **Python** : pour le développement des fonctions ETL.
- **Pandas** : pour la manipulation et la transformation des données.
- **Requests** : pour interagir avec l'API.
- **MongoDB (local)** : pour stocker les données transformées.
- **MongoDB Compass** :  Pour avoir accès aux données sur une interface graphique
- **Airflow (Astronomer)** : pour orchestrer le pipeline ETL.
- **Docker & Docker Compose** : pour le déploiement et l'orchestration des différents services.
- **Apache Superset** : Pour la visualisation des données 


Le pipeline est organisé en quatre étapes principales :


1. **Verifier la diponibilité de l'API**
2. **Extraction** : Appel à l'API pour récupérer les données horaires de la station.
3. **Transformation** : Conversion des données brutes en moyennes journalières.
4. **Chargement** : Insertion des données transformées dans MongoDB Compass.


## Prérequis

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/)
- Compte GitHub pour cloner le projet
- [Astro CLI ](https://www.astronomer.io/docs/astro/cli/install-cli)
- [MongoDB Compass](https://www.mongodb.com/try/download/compass)
- [MongoDB](https://www.mongodb.com/try/download/shell)
- Python 3.8+
- [Apache drill](https://drill.apache.org/download/)


## Installation

1. **Cloner le dépôt GitHub** :

   ```bash
   git clone https://github.com/YORAM12LUX/Hirring_Challenge.git
   cd Hirring_Challenge
2. **Configuration de Docker-Compose**:
Ici nous avons deux dossiers images pour créer les conteneurs de Superset et Airflow respectivement 


## Vue de la pipeline

![Pipeline](Pipeline_ETL.drawio.png)
## Configuration

1. Airflow :

Configurez une connexion Airflow nommée air_quality_api via l'interface Web d'Airflow (Admin > Connections).
Exemple de configuration pour air_quality_api :
- Conn ID : air_quality_api
- Conn Type : HTTP
- Host : https://airqino-api.magentalab.it
- Extra (format JSON) : 
{
  "endpoint": "/v3/getStationHourlyAvg",
  "headers": {
    "Content-Type": "application/json"
  }
}

2. MongoDB :

Le projet se connecte à MongoDB Compass via l'URI mongodb://yoyo:yoyo12@host.docker.internal:27017. Vous pouvez ajuster cette URI dans le DAG si nécessaire. Mes services utilisent docker
