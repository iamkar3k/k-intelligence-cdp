"""
One-time script to upload processed data to Supabase.
Run once: python3 src/upload_data.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
from src.data_loader  import load_csvs
from src.intelligence import build_intelligence, get_summary_stats
from src.db           import upload_dataframe, get_engine
from dotenv import load_dotenv

load_dotenv()


def upload_all():
    print("Step 1 — Loading and joining CSVs...")
    master, purchase, errors = load_csvs()
    if errors:
        for e in errors: print(f"  ERROR: {e}")
        return

    print(f"  ✅ Master: {len(master):,} rows | Purchase: {len(purchase):,} rows")

    print("\nStep 2 — Computing intelligence layer...")
    intel = build_intelligence(master, purchase)
    print(f"  ✅ Intelligence: {len(intel):,} unique profiles")

    print("\nStep 3 — Uploading to Supabase...")

    tables = {
        "master":      master,
        "purchase":    purchase,
        "intelligence": intel,
    }

    # Monthly LTV
    master2 = master.copy()
    master2["YEAR_MONTH"] = master2["ORDER_DATE"].dt.to_period("M").astype(str)
    order_counts = master2.groupby("GUID")["ORDER"].nunique()
    master2["CUSTOMER_TYPE"] = master2["GUID"].map(lambda g: "Repeat" if order_counts.get(g,0) > 1 else "New")
    monthly = (
        master2.groupby(["YEAR_MONTH","CUSTOMER_TYPE"])
        .agg(LTV=("ORDER_AMT","sum"), CUSTOMERS=("GUID","nunique"), AVG_LTV=("ORDER_AMT","mean"))
        .reset_index()
    )
    tables["monthly_ltv"] = monthly

    # Category LTV
    if not purchase.empty and "CATEGORY_DESC" in purchase.columns:
        from src.intelligence import mask_categories, CHANNEL_MAP
        _, cat_map, subcat_map = mask_categories(purchase)

        cat_map_df = pd.DataFrame(list(cat_map.items()), columns=["ACTUAL","MASKED"])
        sub_map_df = pd.DataFrame(list(subcat_map.items()), columns=["ACTUAL","MASKED"])
        cat_ltv    = purchase.groupby("CATEGORY_DESC")["PRICE_PAID"].sum().reset_index()
        cat_ltv["CATEGORY_MASKED"] = cat_ltv["CATEGORY_DESC"].map(cat_map)

        tables["category_map"]    = cat_map_df
        tables["subcategory_map"] = sub_map_df
        tables["category_ltv"]    = cat_ltv

        if "SUBCATEGORY_DESC" in purchase.columns:
            sub_ltv = purchase.groupby("SUBCATEGORY_DESC")["PRICE_PAID"].sum().reset_index()
            sub_ltv["SUBCATEGORY_MASKED"] = sub_ltv["SUBCATEGORY_DESC"].map(subcat_map)
            tables["subcategory_ltv"] = sub_ltv

    for name, df in tables.items():
        print(f"  Uploading {name} ({len(df):,} rows)...", end=" ")
        try:
            # Convert datetime cols to string for postgres compatibility
            for col in df.select_dtypes(include=["datetime64"]).columns:
                df[col] = df[col].astype(str)
            upload_dataframe(df, name)
            print("✅")
        except Exception as e:
            print(f"❌ {e}")

    print("\n✅ All data uploaded to Supabase successfully!")
    print("You can now deploy the app — data is live in the cloud.")


if __name__ == "__main__":
    upload_all()
