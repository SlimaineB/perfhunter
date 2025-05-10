import streamlit as st

def dynamic_metric(container, label, value, low_threshold, high_threshold, unit="%"):
    if value <= low_threshold:
        color, symbol = "#f1c40f", "🔻" 
    elif value >= high_threshold:
        color, symbol = "#e74c3c", "🔺"  
    else:
        color, symbol = "#2ecc71", "✅"

    container.markdown(f"""
        <div style="border: 3px solid {color}; padding: 10px; border-radius: 10px; font-size: 20px; color: {color}; text-align: center;">
            {label}: {symbol} <strong>{value}</strong>{unit}
        </div>
    """, unsafe_allow_html=True)
