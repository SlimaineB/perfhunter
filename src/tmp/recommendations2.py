import requests
import pandas as pd

HISTORY_SERVER = "http://<history-server>:18080"
APP_ID = "application_1234567890"

def get_api_data(endpoint):
    """Interroge l'API REST du History Server"""
    url = f"{HISTORY_SERVER}/api/v1/applications/{APP_ID}/{endpoint}"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"⚠️ Erreur {response.status_code}: Impossible de récupérer {endpoint}")
        return None

def analyze_performance():
    """Analyse les métriques et génère des recommandations"""
    executors_data = get_api_data("executors")
    stages_data = get_api_data("stages")

    # Vérification des exécutors
    cpu_utilization = []
    for executor in executors_data:
        cpu_utilization.append({
            "executorId": executor["id"],
            "cpuTime": executor.get("totalExecutorCpuTime", 0),
            "runTime": executor.get("totalExecutorRunTime", 0),
            "peakMemory": executor.get("peakExecutionMemory", 0),
            "gcTime": executor.get("totalJVMGCTime", 0)
        })

    df_executors = pd.DataFrame(cpu_utilization)

    # Vérification des stages
    shuffle_data = []
    for stage in stages_data:
        shuffle_data.append({
            "stageId": stage["stageId"],
            "shuffleRemoteBytesRead": stage.get("shuffleRemoteBytesRead", 0),
            "numTasks": stage["numTasks"],
            "executorRunTime": stage["executorRunTime"]
        })

    df_stages = pd.DataFrame(shuffle_data)

    # 🔄 Recommandation sur le partitionnement
    avg_remote_read = df_stages["shuffleRemoteBytesRead"].mean()
    avg_num_tasks = df_stages["numTasks"].mean()
    optimal_partitions = max(100, int(avg_num_tasks * 2 + avg_remote_read / 50000))

    # 🔥 Recommandation sur l’efficacité CPU
    avg_cpu_efficiency = (df_executors["cpuTime"] / df_executors["runTime"]).mean()
    cpu_recommendation = "OK" if avg_cpu_efficiency > 0.8 else "🔧 Augmenter le parallélisme"

    # 📌 Affichage des recommandations
    print(f"🔄 Recommandation : spark.sql.shuffle.partitions = {optimal_partitions}")
    print(f"🔥 Efficacité CPU moyenne : {avg_cpu_efficiency:.2f} ({cpu_recommendation})")
    print(f"📊 Temps moyen d’exécution des stages : {df_stages['executorRunTime'].mean():.2f} ms")

    return df_executors, df_stages, optimal_partitions

# Exécution
df_executors, df_stages, recommended_partitions = analyze_performance()
print(df_executors)
print(df_stages)
