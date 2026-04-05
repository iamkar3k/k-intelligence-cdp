import pandas as pd
import sqlite3
from datetime import datetime
from src.data_loader import get_connection

CHANNEL_MAP = {
    "ET": ("Triggered Emails",          "Demand Harness"),
    "NS": ("Natural Search",            "Demand Harness"),
    "OF": ("Offline Mktg Misc",         "Demand Capture"),
    "ON": ("Online Mktg Misc",          "Demand Creation"),
    "OP": ("Podcast - Offline",         "Demand Creation"),
    "OR": ("Radio - Offline",           "Demand Creation"),
    "OS": ("Space - Offline",           "Demand Creation"),
    "PH": ("Online Phone",              "Demand Creation"),
    "PR": ("Public Relations",          "Demand Capture"),
    "SB": ("Branded Paid Search",       "Demand Capture"),
    "SM": ("Social Marketing",          "Demand Capture"),
    "TM": ("SMS",                       "Demand Harness"),
    "TT": ("Triggered SMS",             "Demand Harness"),
    "WM": ("White Mail - No Key",       "Demand Capture"),
    "OT": ("TV - Offline",              "Demand Creation"),
    "LB": ("Light Box",                 "Demand Capture"),
    "CT": ("House Catalog BB Fill",     "Demand Capture"),
    "CO": ("House Catalog Offline",     "Demand Creation"),
    "AW": ("Affiliates/Whitelabels",    "Demand Capture"),
    "SN": ("Non-Branded Paid Search",   "Demand Capture"),
    "OO": ("Other - Offline",           "Demand Creation"),
    "GN": ("Generic Web",               "Demand Capture"),
    "EM": ("Email",                     "Demand Harness"),
    "CS": ("Cust Service",              "Demand Capture"),
}


def safe_qcut(series, q, labels, ascending=True):
    try:
        return pd.qcut(series, q=q, labels=labels, duplicates="drop").astype(int)
    except Exception:
        ranked = series.rank(method="first", ascending=ascending)
        return pd.cut(ranked, bins=q, labels=labels).astype(int)


def mask_categories(df):
    cat_map, subcat_map = {}, {}
    for i, c in enumerate(sorted(df["CATEGORY_DESC"].dropna().unique()), 1):
        cat_map[c] = f"Category {i}"
    if "SUBCATEGORY_DESC" in df.columns:
        for i, s in enumerate(sorted(df["SUBCATEGORY_DESC"].dropna().unique()), 1):
            subcat_map[s] = f"Subcategory {i}"
    df = df.copy()
    df["CATEGORY_MASKED"]    = df["CATEGORY_DESC"].map(cat_map)
    if "SUBCATEGORY_DESC" in df.columns:
        df["SUBCATEGORY_MASKED"] = df["SUBCATEGORY_DESC"].map(subcat_map)
    return df, cat_map, subcat_map


def enrich_channels(df):
    """Add channel description and demand group columns."""
    df = df.copy()
    df["ORDER_CHANNEL_DESC"]   = df["SOURCE_CHANNEL"].map(lambda x: CHANNEL_MAP.get(str(x).strip(), (str(x), "Unknown"))[0])
    df["ORDER_DEMAND_GROUP"]   = df["SOURCE_CHANNEL"].map(lambda x: CHANNEL_MAP.get(str(x).strip(), (str(x), "Unknown"))[1])
    df["ENTERED_CHANNEL_DESC"] = df["INITIAL_CHANNEL"].map(lambda x: CHANNEL_MAP.get(str(x).strip(), (str(x), "Unknown"))[0])
    df["ENTERED_DEMAND_GROUP"] = df["INITIAL_CHANNEL"].map(lambda x: CHANNEL_MAP.get(str(x).strip(), (str(x), "Unknown"))[1])
    return df


def compute_rfm(master):
    snapshot = master["ORDER_DATE"].max()
    if pd.isnull(snapshot):
        snapshot = datetime.now()

    rfm = (
        master.groupby("GUID")
        .agg(
            RECENCY   = ("ORDER_DATE", lambda x: (snapshot - x.max()).days),
            FREQUENCY = ("ORDER",      "count"),
            MONETARY  = ("ORDER_AMT",  "sum"),
        )
        .reset_index()
    )

    rfm["R_SCORE"]   = safe_qcut(rfm["RECENCY"],   4, [4,3,2,1], ascending=True)
    rfm["F_SCORE"]   = safe_qcut(rfm["FREQUENCY"], 4, [1,2,3,4], ascending=False)
    rfm["M_SCORE"]   = safe_qcut(rfm["MONETARY"],  4, [1,2,3,4], ascending=False)
    rfm["RFM_TOTAL"] = rfm["R_SCORE"] + rfm["F_SCORE"] + rfm["M_SCORE"]

    def tier(row):
        if row["RFM_TOTAL"] >= 10: return "Champion"
        elif row["RFM_TOTAL"] >= 8: return "Loyal"
        elif row["RFM_TOTAL"] >= 6: return "Potential"
        elif row["R_SCORE"] <= 2 and row["F_SCORE"] >= 3: return "At Risk"
        else: return "Lapsed"

    rfm["RFM_TIER"] = rfm.apply(tier, axis=1)
    return rfm


def compute_category_affinity(purchase):
    if "GUID" not in purchase.columns or purchase.empty:
        return pd.DataFrame(columns=["GUID","TOP_CATEGORY","TOP_SUBCATEGORY","CATEGORY_COUNT","BUYER_TYPE"])

    spend = purchase.groupby(["GUID","CATEGORY_DESC"])["PRICE_PAID"].sum().reset_index()
    top_cat = (
        spend.sort_values("PRICE_PAID", ascending=False)
        .groupby("GUID").first().reset_index()[["GUID","CATEGORY_DESC"]]
        .rename(columns={"CATEGORY_DESC":"TOP_CATEGORY"})
    )
    cat_count = spend.groupby("GUID")["CATEGORY_DESC"].nunique().reset_index()
    cat_count.columns = ["GUID","CATEGORY_COUNT"]

    top_sub = pd.DataFrame(columns=["GUID","TOP_SUBCATEGORY"])
    if "SUBCATEGORY_DESC" in purchase.columns:
        subspend = purchase.groupby(["GUID","SUBCATEGORY_DESC"])["PRICE_PAID"].sum().reset_index()
        top_sub = (
            subspend.sort_values("PRICE_PAID", ascending=False)
            .groupby("GUID").first().reset_index()[["GUID","SUBCATEGORY_DESC"]]
            .rename(columns={"SUBCATEGORY_DESC":"TOP_SUBCATEGORY"})
        )

    aff = top_cat.merge(cat_count, on="GUID", how="left")
    aff = aff.merge(top_sub, on="GUID", how="left") if not top_sub.empty else aff
    aff["BUYER_TYPE"] = aff["CATEGORY_COUNT"].apply(lambda x: "Multi-Category" if x > 1 else "Single-Category")
    return aff


def build_intelligence(master, purchase):
    rfm = compute_rfm(master)
    aff = compute_category_affinity(purchase)

    profile = (
        master.sort_values("ORDER_DATE", ascending=False)
        .groupby("GUID")
        .agg(
            EMAIL           = ("EMAIL",           "first"),
            PHONE           = ("PHONE",           "first"),
            SOURCE_CHANNEL  = ("SOURCE_CHANNEL",  "first"),
            INITIAL_CHANNEL = ("INITIAL_CHANNEL", "first"),
            AS400_CHANNEL   = ("AS400_CHANNEL",   "first"),
            EMAIL_OPTIN     = ("EMAIL_OPTIN",     "max"),
            EMAIL_OPTOUT    = ("EMAIL_OPTOUT",    "max"),
            SMS_OPTIN       = ("SMS_OPTIN",       "max"),
            SMS_OPTOUT      = ("SMS_OPTOUT",      "max"),
            TOTAL_ORDERS    = ("ORDER",           "nunique"),
            TOTAL_SPEND     = ("ORDER_AMT",       "sum"),
            FIRST_ORDER     = ("ORDER_DATE",      "min"),
            LAST_ORDER      = ("ORDER_DATE",      "max"),
        )
        .reset_index()
    )

    profile["CUSTOMER_TYPE"] = profile["TOTAL_ORDERS"].apply(lambda x: "Repeat" if x > 1 else "New")
    profile["LTV"]           = profile["TOTAL_SPEND"]
    profile = enrich_channels(profile)

    intel = (
        profile
        .merge(rfm[["GUID","RECENCY","FREQUENCY","MONETARY","RFM_TIER"]], on="GUID", how="left")
        .merge(aff, on="GUID", how="left")
    )

    conn = get_connection()
    intel.to_sql("intelligence", conn, if_exists="replace", index=False)

    # Monthly LTV + customers
    master2 = master.copy()
    master2["YEAR_MONTH"] = master2["ORDER_DATE"].dt.to_period("M").astype(str)
    order_counts = master2.groupby("GUID")["ORDER"].nunique()
    master2["CUSTOMER_TYPE"] = master2["GUID"].map(lambda g: "Repeat" if order_counts.get(g,0) > 1 else "New")
    monthly = (
        master2.groupby(["YEAR_MONTH","CUSTOMER_TYPE"])
        .agg(LTV=("ORDER_AMT","sum"), CUSTOMERS=("GUID","nunique"), AVG_LTV=("ORDER_AMT","mean"))
        .reset_index()
    )
    monthly.to_sql("monthly_ltv", conn, if_exists="replace", index=False)

    # Category LTV
    if not purchase.empty and "CATEGORY_DESC" in purchase.columns:
        _, cat_map, subcat_map = mask_categories(purchase)
        cat_df = pd.DataFrame(list(cat_map.items()), columns=["ACTUAL","MASKED"])
        cat_df.to_sql("category_map", conn, if_exists="replace", index=False)
        sub_df = pd.DataFrame(list(subcat_map.items()), columns=["ACTUAL","MASKED"])
        sub_df.to_sql("subcategory_map", conn, if_exists="replace", index=False)

        cat_ltv = purchase.groupby("CATEGORY_DESC")["PRICE_PAID"].sum().reset_index()
        cat_ltv["CATEGORY_MASKED"] = cat_ltv["CATEGORY_DESC"].map(cat_map)
        cat_ltv.to_sql("category_ltv", conn, if_exists="replace", index=False)

        if "SUBCATEGORY_DESC" in purchase.columns:
            sub_ltv = purchase.groupby("SUBCATEGORY_DESC")["PRICE_PAID"].sum().reset_index()
            sub_ltv["SUBCATEGORY_MASKED"] = sub_ltv["SUBCATEGORY_DESC"].map(subcat_map)
            sub_ltv.to_sql("subcategory_ltv", conn, if_exists="replace", index=False)

    conn.close()
    return intel


def load_intelligence():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM intelligence", conn)
    conn.close()
    return df


def get_summary_stats(intel):
    conn = get_connection()
    try:    monthly   = pd.read_sql("SELECT * FROM monthly_ltv", conn)
    except: monthly   = pd.DataFrame()
    try:    cat_map   = pd.read_sql("SELECT * FROM category_map", conn).set_index("ACTUAL")["MASKED"].to_dict()
    except: cat_map   = {}
    try:    sub_map   = pd.read_sql("SELECT * FROM subcategory_map", conn).set_index("ACTUAL")["MASKED"].to_dict()
    except: sub_map   = {}
    try:    cat_ltv   = pd.read_sql("SELECT * FROM category_ltv", conn)
    except: cat_ltv   = pd.DataFrame()
    try:    sub_ltv   = pd.read_sql("SELECT * FROM subcategory_ltv", conn)
    except: sub_ltv   = pd.DataFrame()
    conn.close()

    # Category LTV with masked names
    cat_ltv_dict = {}
    if not cat_ltv.empty:
        cat_ltv_dict = cat_ltv.set_index("CATEGORY_MASKED")["PRICE_PAID"].to_dict()

    sub_ltv_dict = {}
    if not sub_ltv.empty:
        sub_ltv_dict = (
            sub_ltv.nlargest(10, "PRICE_PAID")
            .set_index("SUBCATEGORY_MASKED")["PRICE_PAID"].to_dict()
        )

    # Channel deep-dive
    ch_group   = intel.groupby("ORDER_DEMAND_GROUP").agg(CUSTOMERS=("GUID","nunique"), LTV=("LTV","sum")).reset_index().to_dict("records") if "ORDER_DEMAND_GROUP" in intel.columns else []
    ch_desc    = intel.groupby("ORDER_CHANNEL_DESC").agg(CUSTOMERS=("GUID","nunique"), LTV=("LTV","sum")).reset_index().to_dict("records") if "ORDER_CHANNEL_DESC" in intel.columns else []
    ent_group  = intel.groupby("ENTERED_DEMAND_GROUP").agg(CUSTOMERS=("GUID","nunique"), LTV=("LTV","sum")).reset_index().to_dict("records") if "ENTERED_DEMAND_GROUP" in intel.columns else []
    ent_desc   = intel.groupby("ENTERED_CHANNEL_DESC").agg(CUSTOMERS=("GUID","nunique"), LTV=("LTV","sum")).reset_index().to_dict("records") if "ENTERED_CHANNEL_DESC" in intel.columns else []

    # Channel matrix (entered -> order)
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
        "cat_ltv":          cat_ltv_dict,
        "sub_ltv":          sub_ltv_dict,
        "ch_group":         ch_group,
        "ch_desc":          ch_desc,
        "ent_group":        ent_group,
        "ent_desc":         ent_desc,
        "ch_matrix":        matrix,
        "monthly_ltv":      monthly.to_dict("records") if not monthly.empty else [],
        "cat_map":          cat_map,
        "sub_map":          sub_map,
    }