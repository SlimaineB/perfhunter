import streamlit as st
import importlib
import os
from services.spark_history_fetcher import SparHistorykFetcher
from config.settings import API_ENDPOINT

# Fonction pour charger dynamiquement les heuristiques
def load_heuristics():
    heuristics = []
    heuristics_dir = os.path.join(os.path.dirname(__file__), "heuristics")
    for file in os.listdir(heuristics_dir):
        if file.endswith("_heuristic.py"):
            module_name = f"heuristics.{file[:-3]}"  # Supprime ".py" pour obtenir le nom du module
            module = importlib.import_module(module_name)
            for attr in dir(module):
                if attr.endswith("Heuristic"):
                    heuristic_class = getattr(module, attr)
                    if callable(heuristic_class) and hasattr(heuristic_class, "evaluate"):
                        heuristics.append(heuristic_class)
    return heuristics

# Streamlit app
st.title("PerfHunter - Spark Job Analyzer")

application_id = st.text_input("Application ID")
attempt_id = st.text_input("Attempt ID (optionnel, 1-100)")

if st.button("Générer les recommandations"):
    if application_id:
        # Convertir attempt_id en int si renseigné et valide
        attempt_id_param = None
        if attempt_id.strip():
            try:
                attempt_id_int = int(attempt_id)
                if 1 <= attempt_id_int <= 100:
                    attempt_id_param = attempt_id_int
                else:
                    st.warning("Attempt ID doit être entre 1 et 100.")
                    st.stop()
            except ValueError:
                st.warning("Attempt ID doit être un nombre entier.")
                st.stop()

        # Fetch data from Spark History Server
        spark_api = SparHistorykFetcher(API_ENDPOINT)
        history_data = spark_api.fetch_all_data(application_id, attempt_id_param)

        # Charger dynamiquement les heuristiques
        heuristics = load_heuristics()

        # Appliquer chaque heuristique et afficher les recommandations
        for heuristic in heuristics:
            st.subheader(f"Recommandations : {heuristic.__name__}")
            recommendations = heuristic.evaluate(history_data)
            st.write(recommendations)

        # Debugging: Log the fetched data
        st.subheader("Debug: job_data")
        st.json(history_data.get("jobs", []))
        st.subheader("Debug: stage_data")
        st.json(history_data.get("stages", []))
        st.subheader("Debug: executor_data")
        st.json(history_data.get("executors", []))
    else:
        st.warning("Veuillez renseigner l'Application ID.")