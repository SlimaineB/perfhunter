import streamlit as st
from services.spark_api import SparkAPI
from utils.recommendations import generate_data_skew_recommendations, evaluate_executor_memory
from config.settings import API_ENDPOINT

st.title("PerfHunter - Spark Job Analyzer")

application_id = st.text_input("Application ID")
attempt_id = st.text_input("Attempt ID (optionnel)")

if st.button("Générer les recommandations"):
    if application_id:
        spark_api = SparkAPI(API_ENDPOINT)
        job_data = spark_api.fetch_job_data(application_id, attempt_id if attempt_id else None)
        
        data_skew_recommendations = generate_data_skew_recommendations(job_data)
        #executor_memory_evaluation = evaluate_executor_memory(job_data)
        
        st.subheader("Recommandations sur le skew des données")
        st.write(data_skew_recommendations)
        
        st.subheader("Évaluation de la mémoire des exécutors")
        #st.write(executor_memory_evaluation)
    else:
        st.warning("Veuillez renseigner l'Application ID.")