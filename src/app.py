import streamlit as st
from ui.main_page import run_ui


st.set_page_config(page_title="Spark Perf Hunter", page_icon="🚀", layout="wide")

if __name__ == "__main__":
    run_ui()