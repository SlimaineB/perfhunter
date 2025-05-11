import streamlit as st
from config.settings import API_ENDPOINT    

def configuration_tab(T):
    st.subheader("⚙️ Configuration")
    st.info("Add your configuration options here (API endpoint, heuristics, etc).")
    st.write(f"Current API endpoint: `{API_ENDPOINT}`")
    # Add more configuration fields as needed

