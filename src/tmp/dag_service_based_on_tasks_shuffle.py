import requests
import networkx as nx
import matplotlib
matplotlib.use("Agg")  # Compatible avec WSL2
import matplotlib.pyplot as plt
from datetime import datetime

# Définir l'ID de l'application Spark et l'URL de l'History Server
APP_ID = "app-20250504202708-0023"
HISTORY_SERVER_URL = "http://localhost:18080/api/v1/applications"

# 📡 Récupérer les stages Spark
def get_stages():
    url = f"{HISTORY_SERVER_URL}/{APP_ID}/stages"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Erreur lors de la récupération des stages : {response.status_code}")

# 🔍 Récupérer les tâches associées à un stage
def get_tasks_by_stage(stage_id):
    url = f"{HISTORY_SERVER_URL}/{APP_ID}/stages/{stage_id}/0/taskList"  # Ajout de `attempt = 0`
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"⚠️ Erreur sur le stage {stage_id}: {response.status_code}")
        return []

# ⏳ Trier les tâches par date d'exécution
def sort_tasks_by_time(tasks):
    for task in tasks:
        task["launchTime"] = datetime.strptime(task["launchTime"].replace("GMT", ""), "%Y-%m-%dT%H:%M:%S.%f")
    return sorted(tasks, key=lambda x: x["launchTime"])

# 🛠 Construire le DAG des tâches en fonction du temps
def build_dag_from_tasks(stages):
    dag = nx.DiGraph()

    for stage in stages:
        stage_id = stage["stageId"]
        stage_name = stage["name"]
        tasks = sort_tasks_by_time(get_tasks_by_stage(stage_id))
        
        for i in range(len(tasks)):
            task_id = tasks[i]["taskId"]
            task_time = tasks[i]["launchTime"]
            dag.add_node(task_id, label=f"Tâche {task_id}\nStage {stage_id} ({stage_name})")

            if i > 0:
                prev_task_id = tasks[i - 1]["taskId"]
                dag.add_edge(prev_task_id, task_id)  # Relier les tâches successives

    return dag

# 🎨 Générer la visualisation du DAG
def visualize_dag(dag):
    plt.figure(figsize=(12, 8))
    pos = nx.spring_layout(dag, seed=42)
    labels = {node: dag.nodes[node]["label"] for node in dag.nodes}

    nx.draw(dag, pos, with_labels=True, labels=labels, node_color="lightblue",
            edge_color="gray", font_size=10, node_size=3000, font_weight="bold")

    plt.title("DAG basé sur les Tâches et leur Temps d'Exécution", fontsize=14)
    plt.savefig("dag_tasks_time.png")
    print("✅ Le DAG a été enregistré sous 'dag_tasks_time.png'")

# 🚀 Exécution du script
if __name__ == "__main__":
    print("📡 Récupération des stages...")
    stages = get_stages()

    print("🛠 Construction du DAG basé sur les tâches...")
    dag = build_dag_from_tasks(stages)

    print("🎨 Génération de la visualisation...")
    visualize_dag(dag)

    print("✅ Script terminé !")
