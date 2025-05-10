import streamlit as st
from config.i18n import i18n
from pages.home import home_tab
from pages.configuration import configuration_tab

st.set_page_config(layout="wide")

# Sélection de la langue
LANG = st.sidebar.selectbox("Language / Langue", options=["English", "Français"], index=0)
T = i18n[LANG]  

st.markdown("""
    <div style="background-color: #2C3E50; padding: 10px; border-radius: 10px; text-align: center;">
        <h1 style="color: white;">🔥 Spark Perf Hunter 🚀</h1>
    </div>
""", unsafe_allow_html=True)

# Création des onglets
tabs = st.tabs([T["home_tab"], T["config_tab"]])
with tabs[0]:
    home_tab(T)
with tabs[1]:
    configuration_tab(T)
