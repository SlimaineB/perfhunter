import requests
import networkx as nx
import matplotlib.pyplot as plt
from datetime import datetime

# Définir l'ID de l'application Spark et l'URL de l'History Server
APP_ID = "app-20250504202708-0023"
HISTORY_SERVER_URL = "http://localhost:18080/api/v1/applications"

# Fonction pour récupérer les données des stages
def get_stages():
    url = f"{HISTORY_SERVER_URL}/{APP_ID}/stages"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Erreur lors de la récupération des données : {response.status_code}")

# Fonction pour ordonner les stages selon leur timestamp
def sort_stages_by_time(stages):
    filtered_stages = [stage for stage in stages if stage.get("status") != "SKIPPED"]

    for stage in filtered_stages:
# Nettoyer le format du timestamp en retirant "GMT"
        print(stage["submissionTime"])
        cleaned_time = stage["submissionTime"].replace("GMT", "")
        stage["submissionTime"] = datetime.strptime(cleaned_time, "%Y-%m-%dT%H:%M:%S.%f")
        print(stage["submissionTime"])
    return sorted(filtered_stages, key=lambda x: x["submissionTime"])

# Fonction pour construire le DAG des stages
def build_dag(stages_sorted):
    dag = nx.DiGraph()
    for stage in stages_sorted:
        dag.add_node(stage["stageId"], name=stage["name"])
    for i in range(1, len(stages_sorted)):
        dag.add_edge(stages_sorted[i-1]["stageId"], stages_sorted[i]["stageId"])
    return dag



def visualize_dag(dag):
    plt.figure(figsize=(12, 8))  # Taille optimisée pour meilleure visibilité

    pos = nx.spring_layout(dag, seed=42)  # Disposition plus lisible

    # Récupération des noms et numéros des stages
    labels = {node: f"Stage {node}\n{dag.nodes[node]['name']}" for node in dag.nodes}

    # Dessiner le graphe avec les améliorations
    nx.draw(
        dag, pos, with_labels=True, labels=labels, node_color="lightcoral",
        edge_color="gray", font_size=10, node_size=3000, font_weight="bold"
    )

    plt.title("DAG des Stages Spark", fontsize=14)
    plt.savefig("dag_ameliore.png")  # Sauvegarde améliorée
    print("Le DAG a été enregistré sous 'dag_ameliore.png'")


# Exécution du script
if __name__ == "__main__":
    stages = get_stages()
    stages_sorted = sort_stages_by_time(stages)
    dag = build_dag(stages_sorted)
    visualize_dag(dag)
