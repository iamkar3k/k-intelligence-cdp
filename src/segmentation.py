import pandas as pd
import sqlite3
from datetime import datetime
from src.data_loader import get_connection


def apply_filters(
    intel: pd.DataFrame,
    rfm_tiers: list[str]   = None,
    channels: list[str]    = None,
    categories: list[str]  = None,
    email_optin: bool      = False,
    sms_optin: bool        = False,
    min_spend: float       = None,
    max_spend: float       = None,
) -> pd.DataFrame:
    """Apply audience manager filters and return matching customers."""
    df = intel.copy()

    if rfm_tiers:
        df = df[df["RFM_TIER"].isin(rfm_tiers)]

    if channels:
        df = df[df["SOURCE_CHANNEL"].isin(channels)]

    if categories:
        df = df[df["TOP_CATEGORY"].isin(categories)]

    if email_optin:
        df = df[(df["EMAIL_OPTIN"] == 1) & (df["EMAIL_OPTOUT"] == 0)]

    if sms_optin:
        df = df[(df["SMS_OPTIN"] == 1) & (df["SMS_OPTOUT"] == 0)]

    if min_spend is not None:
        df = df[df["MONETARY"] >= min_spend]

    if max_spend is not None:
        df = df[df["MONETARY"] <= max_spend]

    return df.reset_index(drop=True)


def save_segment(segment_name: str, filters: dict, guids: pd.Series):
    """Save a named segment to SQLite segments table."""
    conn = get_connection()

    # Create segments table if not exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS segments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            segment_name TEXT,
            created_at TEXT,
            filters TEXT,
            size INTEGER
        )
    """)

    # Save segment metadata
    conn.execute(
        "INSERT INTO segments (segment_name, created_at, filters, size) VALUES (?,?,?,?)",
        (segment_name, datetime.now().isoformat(), str(filters), len(guids))
    )
    seg_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Save GUIDs
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS segment_{seg_id}_guids (
            segment_name TEXT,
            GUID TEXT
        )
    """)
    guid_df = pd.DataFrame({"segment_name": segment_name, "GUID": guids})
    guid_df.to_sql(f"segment_{seg_id}_guids", conn, if_exists="replace", index=False)

    conn.commit()
    conn.close()
    return seg_id


def export_segment(filtered_df: pd.DataFrame, segment_name: str) -> pd.DataFrame:
    """Return export-ready dataframe with segment name + GUID."""
    export = filtered_df[["GUID"]].copy()
    export.insert(0, "SEGMENT_NAME", segment_name)
    return export


def list_saved_segments() -> pd.DataFrame:
    """Load all saved segment metadata."""
    conn = get_connection()
    try:
        df = pd.read_sql("SELECT * FROM segments ORDER BY created_at DESC", conn)
    except Exception:
        df = pd.DataFrame(columns=["id","segment_name","created_at","filters","size"])
    conn.close()
    return df
