import streamlit as st
from service.spark_history_fetcher_service import SparHistorykFetcherService
from service.spark_optimal_config_generator_service import SparkOptimalConfigGeneratorService
from service.heuristic_service import HeuristicsService  
from config.settings import API_ENDPOINT
from config.i18n import i18n

st.set_page_config(page_title="PerfHunter", page_icon=":bar_chart:")

# Initialize Spark API service once for the session
@st.cache_resource
def get_spark_api():
    return SparHistorykFetcherService(API_ENDPOINT)

def home_tab(T):
    st.title(T["title"])

    spark_api = get_spark_api()

    # Application search/filter UI
    with st.expander(T["search_expander"]):
        st.write(T["filter_label"])
        col1, col2 = st.columns(2)
        with col1:
            min_date = st.date_input(T["start_date_min"], value=None)
        with col2:
            max_date = st.date_input(T["start_date_max"], value=None)
        status = st.selectbox(T["status"], options=T["status_options"], index=0)
        limit = st.number_input(T["limit"], min_value=1, max_value=1000, value=20)

        def date_to_str(d):
            return d.strftime("%Y-%m-%d") if d else None

        # Fetch applications with or without filters
        if st.button(T["search"]):
            applications = spark_api.list_applications(
                status=status if status else None,
                min_date=date_to_str(min_date),
                max_date=date_to_str(max_date),
                limit=limit
            )
        else:
            applications = spark_api.list_applications(limit=limit)

        # Application and attempt selection
        app_options = [
            f"{app['id']} - {app['name']}" for app in applications
        ]
        selected_app = st.selectbox(T["select_app"], app_options)
        selected_app_id, selected_attempt_id = None, None
        if selected_app:
            selected_app_id = selected_app.split(" - ")[0]
            app_obj = next(app for app in applications if app["id"] == selected_app_id)
            attempts = app_obj.get("attempts", [])
            attempt_options = [
                f"{i+1}: {a['startTime']} ({T['attempt_duration']}: {a['duration']} ms)" for i, a in enumerate(attempts)
            ]
            selected_attempt = st.selectbox(T["select_attempt"], attempt_options)
            if selected_attempt:
                idx = int(selected_attempt.split(":")[0]) - 1
                selected_attempt_id = idx + 1

    # Manual override fields
    application_id = st.text_input(T["manual_app_id"], value=selected_app_id if selected_app_id else "")
    attempt_id = st.text_input(T["manual_attempt_id"], value="")

    # Generate recommendations on button click
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

            # Fetch Spark application data
            history_data = spark_api.fetch_all_data(application_id, attempt_id_param)

            # Génération de la configuration optimale
            config_generator = SparkOptimalConfigGeneratorService()
            optimal_config = config_generator.suggest_config(history_data)
            st.subheader("💡 Configuration Spark optimale suggérée")
            st.json(optimal_config)

            # Load and run heuristics
            heuristics = HeuristicsService().load_heuristics()
            for heuristic in heuristics:
                st.subheader(f"{T['recommendations']} : {heuristic.__name__}")
                recommendations = heuristic.evaluate(history_data)
                st.write(recommendations)

            # Debug sections
            st.subheader(T["debug_job"])
            st.json(history_data.get("jobs", []))
            st.subheader(T["debug_stage"])
            st.json(history_data.get("stages", []))
            st.subheader(T["debug_executor"])
            st.json(history_data.get("executors", []))
            st.subheader(T["debug_config"])
            st.json(history_data.get("config", []))
        else:
            st.warning(T["app_id_warning"])

def configuration_tab(T):
    st.title("Configuration")
    st.info("Add your configuration options here (API endpoint, heuristics, etc).")
    st.write(f"Current API endpoint: `{API_ENDPOINT}`")
    # Add more configuration fields as needed

def run_ui():
    LANG = st.sidebar.selectbox("Language / Langue", options=["English", "Français"], index=0)
    T = i18n[LANG]

    tabs = st.tabs(["Home", "Configuration"])
    with tabs[0]:
        home_tab(T)
    with tabs[1]:
        configuration_tab(T)