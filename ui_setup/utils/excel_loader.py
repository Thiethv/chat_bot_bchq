import pandas as pd
import streamlit as st

def load_excel(file):
    try:
        return pd.read_excel(file)
    except Exception as e:
        st.error(f"Lỗi đọc file Excel: {e}")
        return None
