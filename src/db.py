import os
import pandas as pd
from sqlalchemy import create_engine, text


def _get_db_url():
    """Get DB URL from Streamlit secrets (cloud) or .env (local)."""
    # Try Streamlit secrets first (cloud deployment)
    try:
        import streamlit as st
        url = st.secrets.get("SUPABASE_DB_URL", None)
        if url:
            return url
    except Exception:
        pass
    # Fall back to .env for local development
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass
    return os.environ.get("SUPABASE_DB_URL")


def get_engine():
    url = _get_db_url()
    if not url:
        raise ValueError("SUPABASE_DB_URL not found in secrets or environment.")
    return create_engine(url)


def upload_dataframe(df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
    engine = get_engine()
    df.to_sql(table_name, engine, if_exists=if_exists, index=False, method="multi", chunksize=500)
    engine.dispose()


def read_table(table_name: str, limit: int = None) -> pd.DataFrame:
    engine = get_engine()
    query  = f'SELECT * FROM "{table_name}"'
    if limit:
        query += f" LIMIT {limit}"
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df


def read_query(sql: str) -> pd.DataFrame:
    engine = get_engine()
    df = pd.read_sql(sql, engine)
    engine.dispose()
    return df


def table_exists(table_name: str) -> bool:
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')"
            ))
            exists = result.scalar()
        engine.dispose()
        return bool(exists)
    except Exception:
        return False


def get_row_count(table_name: str) -> int:
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
            count = result.scalar()
        engine.dispose()
        return int(count)
    except Exception:
        return 0