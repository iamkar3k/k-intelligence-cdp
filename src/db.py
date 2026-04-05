import os
import pandas as pd


def _get_creds():
    """Get Supabase credentials from Streamlit secrets or .env."""
    try:
        import streamlit as st
        url  = st.secrets.get("SUPABASE_URL", None)
        key  = st.secrets.get("SUPABASE_SERVICE_KEY", None) or st.secrets.get("SUPABASE_KEY", None)
        db_url = st.secrets.get("SUPABASE_DB_URL", None)
        if url and key:
            return url, key, db_url
    except Exception:
        pass
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass
    url    = os.environ.get("SUPABASE_URL")
    key    = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_KEY")
    db_url = os.environ.get("SUPABASE_DB_URL")
    return url, key, db_url


def get_client():
    from supabase import create_client
    url, key, _ = _get_creds()
    if not url or not key:
        raise ValueError("SUPABASE_URL or SUPABASE_SERVICE_KEY not found.")
    return create_client(url, key)


def upload_dataframe(df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
    """Upload via SQLAlchemy — only used locally."""
    from sqlalchemy import create_engine
    _, _, db_url = _get_creds()
    engine = create_engine(db_url)
    df.to_sql(table_name, engine, if_exists=if_exists, index=False, method="multi", chunksize=500)
    engine.dispose()


def read_table(table_name: str) -> pd.DataFrame:
    """Read full table using SQLAlchemy — fastest for large tables."""
    from sqlalchemy import create_engine
    _, _, db_url = _get_creds()
    if db_url:
        try:
            engine = create_engine(db_url)
            df = pd.read_sql(f'SELECT * FROM "{table_name}"', engine)
            engine.dispose()
            return df
        except Exception:
            pass
    # Fallback to Supabase client with pagination
    client = get_client()
    all_rows = []
    page_size = 1000
    offset = 0
    while True:
        response = (
            client.table(table_name)
            .select("*")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        rows = response.data
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < page_size:
            break
        offset += page_size
    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()


def table_exists(table_name: str) -> bool:
    try:
        client = get_client()
        response = client.table(table_name).select("*", count="exact").limit(1).execute()
        return True
    except Exception:
        return False


def get_row_count(table_name: str) -> int:
    try:
        client = get_client()
        response = client.table(table_name).select("*", count="exact").limit(1).execute()
        return response.count if response.count else 0
    except Exception:
        return 0