import pandas as pd
import sqlite3
import os

DATA_PATH = "/Users/kar3kmac/Documents/BFWAI/Capstone/Data"

CSV_FILES = {
    "cust":     "Custdatabase_2026YTD_Original.csv",
    "email":    "EMAILOPTINFY2026.csv",
    "purchase": "Purchase_Behavior_FY26Q1.csv",
    "sms":      "SMSOPTINFY2026.csv",
}

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'segments.db')

REQUIRED_COLS = {
    "cust":     {"ACCT","GUID","EMAIL","PHONE","SOURCE_CHANNEL",
                 "INITIAL_CHANNEL","AS400_CHANNEL","ORDER_DATE","ORDER_AMT","ORDER"},
    "email":    {"GUID","OPTIN_DATE","OPTOUT_DATE","OPTIN_FLAG","OPTOUT_FLAG"},
    "purchase": {"ORDER_NUMBER","ITEM_NUMBER","PRODUCT_NAME",
                 "CATEGORY_DESC","SUBCATEGORY_DESC","PRICE_PAID"},
    "sms":      {"GUID","OPTIN_FLAG","OPTOUT_FLAG","OPTOUT_DATE","OPTIN_DATE"},
}


def _norm(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().upper() for c in df.columns]
    return df


def validate(df: pd.DataFrame, key: str) -> list:
    missing = REQUIRED_COLS[key] - set(df.columns)
    return sorted(missing)


def check_files_exist() -> list:
    missing = []
    for key, fname in CSV_FILES.items():
        if not os.path.exists(os.path.join(DATA_PATH, fname)):
            missing.append(fname)
    return missing


def load_csvs():
    errors = []

    cust     = _norm(pd.read_csv(os.path.join(DATA_PATH, CSV_FILES["cust"]),     low_memory=False))
    email    = _norm(pd.read_csv(os.path.join(DATA_PATH, CSV_FILES["email"]),    low_memory=False))
    purchase = _norm(pd.read_csv(os.path.join(DATA_PATH, CSV_FILES["purchase"]), low_memory=False))
    sms      = _norm(pd.read_csv(os.path.join(DATA_PATH, CSV_FILES["sms"]),      low_memory=False))

    for key, df, label in [
        ("cust",     cust,     "Customer Database"),
        ("email",    email,    "Email Opt-In"),
        ("purchase", purchase, "Purchase Behavior"),
        ("sms",      sms,      "SMS Opt-In"),
    ]:
        missing = validate(df, key)
        if missing:
            errors.append(f"{label} missing columns: {', '.join(missing)}")

    if errors:
        return None, None, errors

    # ── Strip curly braces from GUID ─────────────────────────────────────────
    cust["GUID"] = cust["GUID"].astype(str).str.strip().str.strip("{}")

    # ── Parse ORDER_DATE — stored as YYYYMMDD integer ─────────────────────────
    cust["ORDER_DATE"] = pd.to_datetime(cust["ORDER_DATE"].astype(str), format="%Y%m%d", errors="coerce")
    cust["ORDER_AMT"]  = pd.to_numeric(cust["ORDER_AMT"],  errors="coerce").fillna(0)
    cust["ORDER"]      = pd.to_numeric(cust["ORDER"],       errors="coerce")

    purchase["ORDER_NUMBER"] = pd.to_numeric(purchase["ORDER_NUMBER"], errors="coerce")
    purchase["PRICE_PAID"]   = pd.to_numeric(purchase["PRICE_PAID"],   errors="coerce").fillna(0)

    # ── Bring GUID into purchase via ORDER = ORDER_NUMBER ─────────────────────
    order_guid = cust[["ORDER","GUID"]].drop_duplicates(subset="ORDER")
    purchase   = purchase.merge(
        order_guid, left_on="ORDER_NUMBER", right_on="ORDER", how="left"
    ).drop(columns=["ORDER"])

    # ── Rename opt-in flags ───────────────────────────────────────────────────
    email = email[["GUID","OPTIN_FLAG","OPTOUT_FLAG"]].copy()
    email.columns = ["GUID","EMAIL_OPTIN","EMAIL_OPTOUT"]

    sms = sms[["GUID","OPTIN_FLAG","OPTOUT_FLAG"]].copy()
    sms.columns = ["GUID","SMS_OPTIN","SMS_OPTOUT"]

    # ── Build unified master ──────────────────────────────────────────────────
    master = (
        cust
        .merge(email, on="GUID", how="left")
        .merge(sms,   on="GUID", how="left")
    )

    master["EMAIL_OPTIN"]  = master["EMAIL_OPTIN"].fillna(0).astype(int)
    master["EMAIL_OPTOUT"] = master["EMAIL_OPTOUT"].fillna(0).astype(int)
    master["SMS_OPTIN"]    = master["SMS_OPTIN"].fillna(0).astype(int)
    master["SMS_OPTOUT"]   = master["SMS_OPTOUT"].fillna(0).astype(int)

    return master, purchase, []


def save_to_sqlite(master: pd.DataFrame, purchase: pd.DataFrame):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    master.to_sql("master",    conn, if_exists="replace", index=False)
    purchase.to_sql("purchase", conn, if_exists="replace", index=False)
    conn.close()


def get_connection():
    return sqlite3.connect(DB_PATH)


def db_exists() -> bool:
    return os.path.exists(DB_PATH)