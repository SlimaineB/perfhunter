import streamlit as st
import pandas as pd
from service.metrics_service import MetricsService
from service.spark_optimal_config_generator_service import SparkOptimalConfigGeneratorService
from service.heuristic_service import HeuristicsService  
from config.settings import API_ENDPOINT
from config.i18n import i18n

import streamlit.components.v1 as components

from ui.configuration_tab import configuration_tab
from ui.analytics_tab import analytics_tab

def run_ui(): 
    
    # Get the translation dictionary for the selected language
    LANG = st.sidebar.selectbox("Language / Langue", options=["English", "Français"], index=0)
    T = i18n[LANG]  

    # Title and logo
    st.markdown("""
        <div style="
            background-color: #2C3E50; 
            padding: 10px; 
            border-radius: 10px; 
            text-align: center;
        ">
            <h1 style="color: white;">🔥 Spark Perf Hunter 🚀</h1>
        </div>
    """, unsafe_allow_html=True)


    # Tabs
    tabs = st.tabs(["Analytics", "Configuration"])
    with tabs[0]:
        analytics_tab(T)
    with tabs[1]:
        configuration_tab(T)