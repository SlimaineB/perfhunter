import requests
import pandas as pd

HISTORY_SERVER = "http://<history-server>:18080"
APP_ID = "application_1234567890"

def get_stage_metrics(app_id):
    """Récupère les métriques des stages depuis l'API REST du History Server"""
    url = f"{HISTORY_SERVER}/api/v1/applications/{app_id}/stages"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erreur {response.status_code}: Impossible de récupérer les données")
        return None

def optimize_shuffle_partitions(stage_data):
    """Analyse les métriques et recommande une valeur optimale pour spark.sql.shuffle.partitions"""
    shuffle_data = []
    for stage in stage_data:
        shuffle_data.append({
            "stageId": stage["stageId"],
            "shuffleRemoteBytesRead": stage.get("shuffleRemoteBytesRead", 0),
            "numTasks": stage["numTasks"]
        })

    df = pd.DataFrame(shuffle_data)

    # Calcul de la moyenne des métriques
    avg_remote_read = df["shuffleRemoteBytesRead"].mean()
    avg_num_tasks = df["numTasks"].mean()

    # Ajustement dynamique basé sur les lectures distantes et le nombre de tâches
    optimal_partitions = max(100, int(avg_num_tasks * 2 + avg_remote_read / 50000))

    print(f"🔄 Recommandation automatique : spark.sql.shuffle.partitions = {optimal_partitions}")
    return df, optimal_partitions

# Exécution
stage_data = get_stage_metrics(APP_ID)
if stage_data:
    df_analysis, recommended_partitions = optimize_shuffle_partitions(stage_data)
    print(df_analysis)
