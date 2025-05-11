import streamlit as st
import pandas as pd
from service.metrics_service import MetricsService
from service.spark_optimal_config_generator_service import SparkOptimalConfigGeneratorService
from service.heuristic_service import HeuristicsService  
from config.settings import API_ENDPOINT
from config.i18n import i18n

import streamlit.components.v1 as components

from ui.configuration_tab import configuration_tab


# Initialize Spark API service once for the session
#@st.cache_resource
def get_metrics_service():
    return MetricsService(API_ENDPOINT)


def analytics_tab(T):

    st.subheader("📊 Analytics")
    metrics_service = get_metrics_service()

    # Application search/filter UI
    history_server_endpoint = st.sidebar.text_input(T["history_server_endpoint"], value=API_ENDPOINT)

    # Application search/filter UI
    with st.sidebar.expander(T["search_expander"]):
        st.subheader(f"{T['filter_label']} ")
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
            applications = metrics_service.list_applications(
                status=status if status else None,
                min_date=date_to_str(min_date),
                max_date=date_to_str(max_date),
                limit=limit
            )
        else:
            applications = metrics_service.list_applications(limit=limit)

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
    application_id = st.sidebar.text_input(T["manual_app_id"], value=selected_app_id if selected_app_id else "")
    attempt_id = "" #st.sidebar.text_input(T["manual_attempt_id"], value=selected_attempt_id if selected_attempt_id else "")


    # Load available heuristics
    heuristics = HeuristicsService().load_heuristics()

    with st.sidebar.expander(T["filter_heuristics_expander"], expanded=False):
        df = pd.DataFrame(data=[{"heuristic":heuristic.__name__, "enabled": True} for heuristic in heuristics], columns=["heuristic","enabled"])
        st.subheader(f"{T['heuristics_select']} ")
        edited_df = st.data_editor(df,hide_index=True)

    # Toggle to filter out "None" criticity rows    
    show_only_issues = st.sidebar.checkbox("Show only detected issues", value=True)
    
    debug_mode = st.sidebar.checkbox(T["debug_mode"], value=False)

    # Generate recommendations on button click
    if st.sidebar.button(T["generate"]):

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
            history_data = metrics_service.fetch_all_data(application_id, attempt_id_param)
            st.write(f"Data fetched for application ID: {application_id} and attempt ID: {attempt_id_param}")

          
            col1row1, col2row1, col3row1, col4row1, col5row1 = st.columns(5)
            
            col1row2, col2row2, col3row2, col4row2, col5row2 = st.columns(5)
            
            col1row3, col2row3, col3row3, col4row3, col5row3 = st.columns(5)
            

            col1row1.metric("App Duration", f"{metrics_service.get_application_duration()} sec", border=True)
            col2row1.metric("Num Executors",  f"{metrics_service.get_num_of_executors()}", border=True)
            col3row1.metric("Total Cores",  f"{metrics_service.get_total_cores()} cores", border=True)
            col4row1.metric("Configured Executor Heap Size", f"{metrics_service.get_configured_heap_memory()/1024/1024} MB", border=True)
            col5row1.metric("Configured Spark Memory", f"{round(metrics_service.get_total_available_spark_memory()/1024/1024,2)} MB", border=True)


            col1row2.metric("Critical Path Duration", f"{round(metrics_service.get_critical_path_duration_in_sec(),2)} sec", border=True)
    

            # 🔵 Ajouter du CSS pour styliser le cadre
            dynamic_metric( col1row3, "💾 Mean Heap Usage", value= round(metrics_service.get_ratio_on_heap_memory()*100,2), low_threshold=50, high_threshold=80) 
            dynamic_metric( col2row3, "💾 Max Heap Usage", value= round(metrics_service.get_max_ratio_on_heap_memory()*100,2), low_threshold=50, high_threshold=80)  
            dynamic_metric( col3row3, "⚙️ CPU Usage", round(metrics_service.get_ratio_cpu_vs_total_time()*100,2), low_threshold=50, high_threshold=80) 
            dynamic_metric(col4row3, "📊 Disk Space", 40, low_threshold=50, high_threshold=80) 
            dynamic_metric(col5row3, "📊 Failed Task", 94, low_threshold=0, high_threshold=1, unit="") 

            #st.subheader(f"{T['summary']}")
            #st.write(df_summary)

            for heuristic in heuristics : 
                if edited_df.loc[edited_df["heuristic"] == heuristic.__name__, "enabled"].values[0]:
                    recommendations = heuristic.evaluate(history_data)
                    if isinstance(recommendations, pd.DataFrame) and not recommendations.empty:
                        if show_only_issues:
                            recommendations = recommendations[recommendations["criticity"] != "None"]

                    if not recommendations.empty:
                        st.subheader(f"{T['recommendations']} : {heuristic.__name__}")
                        st.dataframe(recommendations)
                    else:
                        st.subheader(f"{T['recommendations']} : {heuristic.__name__}")
                        st.write("✅ No issues detected. Everything is within expected thresholds.")

           # Génération de la configuration optimale
            config_generator = SparkOptimalConfigGeneratorService()
            optimal_config = config_generator.suggest_config(history_data)
            st.subheader("💡 Configuration Spark optimale suggérée")
            st.json(optimal_config)


            # Debug sections
            if debug_mode :
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





def dynamic_metric(container, label, value, low_threshold, high_threshold,
                   low_color="#f1c40f", high_color="#e74c3c", normal_color="#2ecc71", unit="%"):
    """ Affichage dynamique de la métrique avec couleur variable et indicateur de tendance """
    if value <= low_threshold:
        color, symbol = low_color, "🔻" 
    elif value >= high_threshold:
        color, symbol = high_color, "🔺"  
    else:
        color, symbol = normal_color, "✅"  

    container.markdown(f"""
        <div style="
            border: 3px solid {color};
            padding: 10px;
            border-radius: 10px;
            font-size: 20px;
            color: {color};
            width: 100%;
            text-align: center;
        ">
            {label}: {symbol} <strong>{value}</strong>{unit}
        </div>
    """, unsafe_allow_html=True)





    

