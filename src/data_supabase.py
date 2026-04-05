import pandas as pd
from src.db import read_table, table_exists, get_row_count


def db_ready() -> bool:
    return table_exists("intelligence") and get_row_count("intelligence") > 0


def load_intelligence() -> pd.DataFrame:
    return read_table("intelligence")


def load_monthly_ltv() -> pd.DataFrame:
    try:
        return read_table("monthly_ltv")
    except Exception:
        return pd.DataFrame()


def load_category_maps() -> tuple:
    try:
        cat = read_table("category_map")
        cat_dict = dict(zip(cat["ACTUAL"], cat["MASKED"])) if not cat.empty else {}
    except Exception:
        cat_dict = {}
    try:
        sub = read_table("subcategory_map")
        sub_dict = dict(zip(sub["ACTUAL"], sub["MASKED"])) if not sub.empty else {}
    except Exception:
        sub_dict = {}
    return cat_dict, sub_dict


def load_category_ltv() -> dict:
    try:
        df = read_table("category_ltv")
        return dict(zip(df["CATEGORY_MASKED"], df["PRICE_PAID"])) if not df.empty else {}
    except Exception:
        return {}


def load_subcategory_ltv() -> dict:
    try:
        df = read_table("subcategory_ltv")
        if df.empty:
            return {}
        df = df.nlargest(10, "PRICE_PAID")
        return dict(zip(df["SUBCATEGORY_MASKED"], df["PRICE_PAID"]))
    except Exception:
        return {}


def get_summary_stats(intel: pd.DataFrame) -> dict:
    monthly  = load_monthly_ltv()
    cat_map, sub_map = load_category_maps()
    cat_ltv  = load_category_ltv()
    sub_ltv  = load_subcategory_ltv()

    ch_group  = intel.groupby("ORDER_DEMAND_GROUP").agg(CUSTOMERS=("GUID","nunique"), LTV=("LTV","sum")).reset_index().to_dict("records") if "ORDER_DEMAND_GROUP" in intel.columns else []
    ch_desc   = intel.groupby("ORDER_CHANNEL_DESC").agg(CUSTOMERS=("GUID","nunique"), LTV=("LTV","sum")).reset_index().to_dict("records") if "ORDER_CHANNEL_DESC" in intel.columns else []
    ent_group = intel.groupby("ENTERED_DEMAND_GROUP").agg(CUSTOMERS=("GUID","nunique"), LTV=("LTV","sum")).reset_index().to_dict("records") if "ENTERED_DEMAND_GROUP" in intel.columns else []
    ent_desc  = intel.groupby("ENTERED_CHANNEL_DESC").agg(CUSTOMERS=("GUID","nunique"), LTV=("LTV","sum")).reset_index().to_dict("records") if "ENTERED_CHANNEL_DESC" in intel.columns else []

    if "ENTERED_DEMAND_GROUP" in intel.columns and "ORDER_DEMAND_GROUP" in intel.columns:
        matrix = intel.groupby(["ENTERED_DEMAND_GROUP","ORDER_DEMAND_GROUP"]).agg(CUSTOMERS=("GUID","nunique")).reset_index().to_dict("records")
    else:
        matrix = []

    return {
        "total_customers":  len(intel),
        "email_optin":      int(intel["EMAIL_OPTIN"].sum()),
        "sms_optin":        int(intel["SMS_OPTIN"].sum()),
        "total_ltv":        float(intel["LTV"].sum()),
        "avg_ltv":          float(intel["LTV"].mean()),
        "new_customers":    int((intel["CUSTOMER_TYPE"]=="New").sum()),
        "repeat_customers": int((intel["CUSTOMER_TYPE"]=="Repeat").sum()),
        "new_ltv":          float(intel[intel["CUSTOMER_TYPE"]=="New"]["LTV"].sum()),
        "repeat_ltv":       float(intel[intel["CUSTOMER_TYPE"]=="Repeat"]["LTV"].sum()),
        "new_avg_ltv":      float(intel[intel["CUSTOMER_TYPE"]=="New"]["LTV"].mean()),
        "repeat_avg_ltv":   float(intel[intel["CUSTOMER_TYPE"]=="Repeat"]["LTV"].mean()),
        "rfm_dist":         intel["RFM_TIER"].value_counts().to_dict(),
        "buyer_type_dist":  intel["BUYER_TYPE"].value_counts().to_dict() if "BUYER_TYPE" in intel.columns else {},
        "cat_ltv":          cat_ltv,
        "sub_ltv":          sub_ltv,
        "ch_group":         ch_group,
        "ch_desc":          ch_desc,
        "ent_group":        ent_group,
        "ent_desc":         ent_desc,
        "ch_matrix":        matrix,
        "monthly_ltv":      monthly.to_dict("records") if not monthly.empty else [],
        "cat_map":          cat_map,
        "sub_map":          sub_map,
    }


def save_segment(segment_name: str, filters: dict, guids: pd.Series):
    from src.db import get_client
    from datetime import datetime
    client = get_client()
    rows = [{"SEGMENT_NAME": segment_name, "GUID": g, "CREATED_AT": datetime.now().isoformat()} for g in guids]
    # Insert in batches of 500
    for i in range(0, len(rows), 500):
        client.table("saved_segments").insert(rows[i:i+500]).execute()