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

# Simple i18n dictionary (French/English)
LANG = st.sidebar.selectbox("Language / Langue", options=["English", "Français"], index=0)
i18n = {
    "English": {
        "title": "PerfHunter - Spark Job Analyzer",
        "search_expander": "🔍 Search for a Spark application",
        "filter_label": "Filter Spark applications by date and status:",
        "start_date_min": "Start date (min)",
        "start_date_max": "Start date (max)",
        "end_date_min": "End date (min)",
        "end_date_max": "End date (max)",
        "status": "Status",
        "status_options": ["", "completed", "running"],
        "limit": "Max number of applications",
        "search": "Search",
        "select_app": "Select an application",
        "select_attempt": "Select an attempt",
        "attempt_duration": "duration",
        "manual_app_id": "Application ID",
        "manual_attempt_id": "Attempt ID (optional, 1-100)",
        "generate": "Generate recommendations",
        "attempt_id_warning": "Attempt ID must be between 1 and 100.",
        "attempt_id_int_warning": "Attempt ID must be an integer.",
        "app_id_warning": "Please provide an Application ID.",
        "recommendations": "Recommendations",
        "debug_job": "Debug: job_data",
        "debug_stage": "Debug: stage_data",
        "debug_executor": "Debug: executor_data",
    },
    "Français": {
        "title": "PerfHunter - Analyseur de jobs Spark",
        "search_expander": "🔍 Rechercher une application Spark",
        "filter_label": "Filtrer les applications Spark par date et statut :",
        "start_date_min": "Date de début min",
        "start_date_max": "Date de début max",
        "end_date_min": "Date de fin min",
        "end_date_max": "Date de fin max",
        "status": "Statut",
        "status_options": ["", "completed", "running"],
        "limit": "Nombre max d'applications",
        "search": "Rechercher",
        "select_app": "Sélectionnez une application",
        "select_attempt": "Sélectionnez un attempt",
        "attempt_duration": "durée",
        "manual_app_id": "Application ID",
        "manual_attempt_id": "Attempt ID (optionnel, 1-100)",
        "generate": "Générer les recommandations",
        "attempt_id_warning": "Attempt ID doit être entre 1 et 100.",
        "attempt_id_int_warning": "Attempt ID doit être un nombre entier.",
        "app_id_warning": "Veuillez renseigner l'Application ID.",
        "recommendations": "Recommandations",
        "debug_job": "Debug: job_data",
        "debug_stage": "Debug: stage_data",
        "debug_executor": "Debug: executor_data",
    }
}
T = i18n[LANG]

# Streamlit app
st.title(T["title"])

# Application search screen
with st.expander(T["search_expander"]):
    st.write(T["filter_label"])
    col1, col2 = st.columns(2)
    with col1:
        min_date = st.date_input(T["start_date_min"], value=None)
        max_date = st.date_input(T["start_date_max"], value=None)
    with col2:
        min_end_date = st.date_input(T["end_date_min"], value=None)
        max_end_date = st.date_input(T["end_date_max"], value=None)
    status = st.selectbox(T["status"], options=T["status_options"], index=0)
    limit = st.number_input(T["limit"], min_value=1, max_value=1000, value=20)

    def date_to_str(d):
        return d.strftime("%Y-%m-%d") if d else None

    if st.button(T["search"]):
        spark_api = SparHistorykFetcher(API_ENDPOINT)
        applications = spark_api.list_applications(
            status=status if status else None,
            min_date=date_to_str(min_date),
            max_date=date_to_str(max_date),
            min_end_date=date_to_str(min_end_date),
            max_end_date=date_to_str(max_end_date),
            limit=limit
        )
    else:
        spark_api = SparHistorykFetcher(API_ENDPOINT)
        applications = spark_api.list_applications(limit=limit)

    app_options = [
        f"{app['id']} - {app['name']}" for app in applications
    ]
    selected_app = st.selectbox(T["select_app"], app_options)
    if selected_app:
        selected_app_id = selected_app.split(" - ")[0]
        app_obj = next(app for app in applications if app["id"] == selected_app_id)
        attempts = app_obj.get("attempts", [])
        attempt_options = [
            f"{i+1}: {a['startTime']} ({T['attempt_duration']}: {a['duration']} ms)" for i, a in enumerate(attempts)
        ]
        selected_attempt = st.selectbox(T["select_attempt"], attempt_options)
        selected_attempt_id = None
        if selected_attempt:
            idx = int(selected_attempt.split(":")[0]) - 1
            selected_attempt_id = idx + 1

# Manual override fields
application_id = st.text_input(T["manual_app_id"], value=selected_app_id if 'selected_app_id' in locals() else "")
attempt_id = st.text_input(T["manual_attempt_id"], value="")

if st.button(T["generate"]):
    if application_id:
        attempt_id_param = None
        if attempt_id.strip():
            try:
                attempt_id_int = int(attempt_id)
                if 1 <= attempt_id_int <= 100:
                    attempt_id_param = attempt_id_int
                else:
                    st.warning(T["attempt_id_warning"])
                    st.stop()
            except ValueError:
                st.warning(T["attempt_id_int_warning"])
                st.stop()

        spark_api = SparHistorykFetcher(API_ENDPOINT)
        history_data = spark_api.fetch_all_data(application_id, attempt_id_param)

        heuristics = load_heuristics()

        for heuristic in heuristics:
            st.subheader(f"{T['recommendations']} : {heuristic.__name__}")
            recommendations = heuristic.evaluate(history_data)
            st.write(recommendations)

        st.subheader(T["debug_job"])
        st.json(history_data.get("jobs", []))
        st.subheader(T["debug_stage"])
        st.json(history_data.get("stages", []))
        st.subheader(T["debug_executor"])
        st.json(history_data.get("executors", []))
    else:
        st.warning(T["app_id_warning"])