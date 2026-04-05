import streamlit as st
st.write("DB URL found:", "SUPABASE_DB_URL" in st.secrets)
st.write("Groq key found:", "GROQ_API_KEY" in st.secrets)
try:
    from src.db import table_exists
    st.write("Table exists:", table_exists("intelligence"))
except Exception as e:
    st.error(f"Error: {e}")
