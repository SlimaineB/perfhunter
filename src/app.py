import streamlit as st
from services.spark_api import SparkAPI
from utils.recommendations import generate_data_skew_recommendations, evaluate_executor_memory, generate_stage_recommendations
from config.settings import API_ENDPOINT

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

        spark_api = SparkAPI(API_ENDPOINT)
        job_data = spark_api.fetch_job_data(application_id, attempt_id_param)
        stage_data = spark_api.fetch_stage_data(application_id, attempt_id_param)
        executor_data = spark_api.fetch_executor_data(application_id, attempt_id_param)

        data_skew_recommendations = generate_data_skew_recommendations(job_data)
        executor_memory_evaluation = evaluate_executor_memory(executor_data)
        stage_recommendations = generate_stage_recommendations(stage_data)
        
        st.subheader("Recommandations sur le skew des données")
        st.write(data_skew_recommendations)
        
        st.subheader("Évaluation de la mémoire des exécutors")
        st.write(executor_memory_evaluation)

        st.subheader("Recommandations sur les stages")
        st.write(stage_recommendations)

        # Log the fetched data
        st.subheader("Debug: job_data")
        st.json(job_data)
        st.subheader("Debug: stage_data")
        st.json(stage_data)
        st.subheader("Debug: executor_data")
        st.json(executor_data)
    else:
        st.warning("Veuillez renseigner l'Application ID.")