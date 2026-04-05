import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys, os

sys.path.insert(0, os.path.dirname(__file__))

from src.data_supabase import db_ready, load_intelligence, get_summary_stats, save_segment
from src.segmentation  import apply_filters, export_segment
from src.campaign      import generate_campaign_copy

st.set_page_config(page_title="K-Intelligence CDP", page_icon="🧠", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
* { font-family:'Inter',sans-serif; }

[data-testid="stSidebar"] { background:#0a0a0a; border-right:1px solid #222; }
[data-testid="stSidebar"] * { color:#f0f0f0 !important; }

.brand-header { padding:1.5rem 1rem 1rem; border-bottom:1px solid #222; margin-bottom:1rem; }
.brand-title  { font-size:14px; font-weight:700; color:#fff !important; line-height:1.3; }
.brand-sub    { font-size:9px; color:#cc2200 !important; text-transform:uppercase; letter-spacing:0.15em; margin-top:3px; font-weight:600; }

.metric-card {
    background:#111; border-radius:8px; padding:0.8rem 1rem;
    border:1px solid #222; border-top:2px solid #cc2200;
    margin-bottom:0.5rem;
}
.metric-label { font-size:10px; color:#888; text-transform:uppercase; letter-spacing:0.1em; font-weight:600; white-space:nowrap; }
.metric-value { font-size:17px; font-weight:700; color:#fff; margin-top:4px; white-space:nowrap; }
.metric-sub   { font-size:10px; color:#cc2200; margin-top:2px; }

.section-header {
    font-size:11px; font-weight:700; color:#888; text-transform:uppercase;
    letter-spacing:0.12em; margin:1.25rem 0 0.65rem;
    padding-bottom:5px; border-bottom:1px solid #222;
}
.status-badge  { display:inline-flex; align-items:center; gap:5px; padding:3px 10px; border-radius:20px; font-size:11px; font-weight:600; }
.badge-success { background:#111; color:#fff; border:1px solid #cc2200; }
.badge-warning { background:#111; color:#cc2200; border:1px solid #cc2200; }
.badge-info    { background:#111; color:#888; border:1px solid #333; }
.filter-card   { background:#111; border:1px solid #222; border-radius:10px; padding:1.25rem; margin-bottom:1rem; }
.campaign-box  { background:#0a0a0a; border:1px solid #222; border-radius:10px; padding:1.5rem; line-height:1.9; color:#f0f0f0; font-size:14px; }
.page-title    { font-size:22px; font-weight:700; color:#fff; letter-spacing:-0.02em; margin-bottom:2px; }
.page-sub      { font-size:12px; color:#666; margin-bottom:1.25rem; }
</style>
""", unsafe_allow_html=True)

for k in ["intel","filtered","stats","campaign_copy"]:
    if k not in st.session_state: st.session_state[k] = None

BG   = "rgba(0,0,0,0)"
FC   = "#f0f0f0"
GC   = "#222222"
RED  = "#cc2200"
GRAY = "#888888"
COLORS = [RED, "#ffffff", "#888888", "#444444", "#ff6644"]

def cl(fig, title=""):
    fig.update_layout(
        paper_bgcolor=BG, plot_bgcolor=BG, font_color=FC, font_size=11,
        title=dict(text=title, font=dict(size=12, color=FC)),
        margin=dict(t=36,b=16,l=8,r=8),
        xaxis=dict(gridcolor=GC, zerolinecolor=GC),
        yaxis=dict(gridcolor=GC, zerolinecolor=GC),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=FC, size=10)),
    )
    return fig

def fmt_m(v):
    if v >= 1_000_000: return f"${v/1_000_000:.1f}M"
    if v >= 1_000:     return f"${v/1_000:.0f}K"
    return f"${v:.0f}"

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""<div class="brand-header">
        <div class="brand-title">Customer Data Platform</div>
        <div class="brand-sub">Powered by K-Intelligence</div>
    </div>""", unsafe_allow_html=True)

    page = st.radio("nav", ["Data Ingestion","Audience Insights","Segment Studio","Campaign Composer"], label_visibility="collapsed")
    st.markdown("---")
    if st.session_state.intel is not None:
        st.markdown(f'<span class="status-badge badge-success">● {len(st.session_state.intel):,} profiles active</span>', unsafe_allow_html=True)
    else:
        st.markdown('<span class="status-badge badge-warning">⚠ No data ingested</span>', unsafe_allow_html=True)
    if st.session_state.filtered is not None:
        st.markdown(f'<br><span class="status-badge badge-info">◈ Segment: {len(st.session_state.filtered):,} profiles</span>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — DATA INGESTION
# ══════════════════════════════════════════════════════════════════════════════
if page == "Data Ingestion":
    st.markdown('<div class="page-title">🧠 Data Ingestion</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">K-Intelligence connects to your Supabase cloud database — no file uploads needed.</div>', unsafe_allow_html=True)

    st.markdown('<div class="section-header">Database Status</div>', unsafe_allow_html=True)

    with st.spinner("Checking Supabase connection..."):
        try:
            from src.db import get_engine, get_row_count, table_exists
            engine = get_engine()
            engine.dispose()
            conn_ok = True
            conn_err = None
        except Exception as e:
            conn_ok = False
            conn_err = str(e)

    if conn_ok:
        try:
            count = get_row_count("intelligence")
            ready = count > 0
            st.info(f"Debug — intelligence row count: {count}")
        except Exception as e:
            ready = False
            st.error(f"Debug — get_row_count failed: {e}")
    else:
        ready = False

    if conn_ok and ready:
        st.markdown('<span class="status-badge badge-success">● Supabase Connected — Data Ready</span>', unsafe_allow_html=True)
        st.markdown("---")

        if st.button("🔄 Load Intelligence Layer"):
            with st.spinner("Loading customer profiles from Supabase..."):
                intel = load_intelligence()
                st.session_state.intel = intel
                st.session_state.stats = get_summary_stats(intel)
            st.success(f"✅ {len(intel):,} unified customer profiles loaded.")
            st.info("Navigate to **Audience Insights** to explore your data.")

        if st.session_state.intel is not None:
            st.markdown("---")
            st.markdown('<div class="section-header">Loaded Data Summary</div>', unsafe_allow_html=True)
            c1, c2, c3 = st.columns(3)
            c1.markdown(f"""<div class="metric-card">
                <div class="metric-label">Customer Profiles</div>
                <div class="metric-value">{len(st.session_state.intel):,}</div>
            </div>""", unsafe_allow_html=True)
            c2.markdown(f"""<div class="metric-card">
                <div class="metric-label">Email Opted-In</div>
                <div class="metric-value">{int(st.session_state.intel["EMAIL_OPTIN"].sum()):,}</div>
            </div>""", unsafe_allow_html=True)
            c3.markdown(f"""<div class="metric-card">
                <div class="metric-label">SMS Opted-In</div>
                <div class="metric-value">{int(st.session_state.intel["SMS_OPTIN"].sum()):,}</div>
            </div>""", unsafe_allow_html=True)
    elif conn_ok and not ready:
        st.markdown('<span class="status-badge badge-warning">⚠ Connected but no data found</span>', unsafe_allow_html=True)
        st.error("Tables not found in Supabase. Run upload_data.py locally first.")
    else:
        st.markdown('<span class="status-badge badge-warning">⚠ Supabase connection failed</span>', unsafe_allow_html=True)
        st.error(f"Connection error: {conn_err}")
        st.markdown("**Check your SUPABASE_DB_URL in Streamlit secrets.**")


# PAGE 2 — AUDIENCE INSIGHTS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Audience Insights":
    st.markdown('<div class="page-title">📊 Audience Insights</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Unified view — LTV, engagement, behavior, and channel distribution.</div>', unsafe_allow_html=True)

    if st.session_state.intel is None:
        st.warning("No data ingested yet. Go to **Data Ingestion** first.")
        st.stop()

    intel = st.session_state.intel
    stats = st.session_state.stats

    # ── KPIs ──────────────────────────────────────────────────────────────────
    st.markdown('<div class="section-header">Key Performance Indicators</div>', unsafe_allow_html=True)
    k1,k2,k3,k4,k5,k6 = st.columns(6)
    kpis = [
        ("Total Profiles",   f"{stats['total_customers']:,}",       "",                                    k1),
        ("Total LTV",        fmt_m(stats['total_ltv']),              f"Avg {fmt_m(stats['avg_ltv'])}",      k2),
        ("New Customers",    f"{stats['new_customers']:,}",          f"LTV {fmt_m(stats['new_ltv'])}",      k3),
        ("Repeat Customers", f"{stats['repeat_customers']:,}",       f"LTV {fmt_m(stats['repeat_ltv'])}",   k4),
        ("Email Opted-In",   f"{stats['email_optin']:,}",            "Contactable",                         k5),
        ("SMS Opted-In",     f"{stats['sms_optin']:,}",              "Contactable",                         k6),
    ]
    for label,value,sub,col in kpis:
        col.markdown(f"""<div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-sub">{sub}</div>
        </div>""", unsafe_allow_html=True)

    # ── Customer Segmentation ─────────────────────────────────────────────────
    st.markdown('<div class="section-header">Customer Segmentation</div>', unsafe_allow_html=True)
    p1,p2,p3 = st.columns(3)

    with p1:
        rfm_df = pd.DataFrame(stats["rfm_dist"].items(), columns=["Tier","Count"])
        fig = px.pie(rfm_df, names="Tier", values="Count",
                     color_discrete_sequence=COLORS, hole=0.5)
        cl(fig, "RFM Tier Distribution")
        fig.update_traces(textfont_color="#fff")
        st.plotly_chart(fig, use_container_width=True)

    with p2:
        nv = pd.DataFrame({"Type":["New","Repeat"],
                           "Count":[stats["new_customers"],stats["repeat_customers"]]})
        fig2 = px.pie(nv, names="Type", values="Count",
                      color_discrete_sequence=[RED,"#ffffff"], hole=0.5)
        cl(fig2, "New vs Repeat Customers")
        fig2.update_traces(textfont_color="#fff")
        st.plotly_chart(fig2, use_container_width=True)

    with p3:
        bt = pd.DataFrame(stats["buyer_type_dist"].items(), columns=["Type","Count"])
        fig3 = px.pie(bt, names="Type", values="Count",
                      color_discrete_sequence=[RED,"#ffffff"], hole=0.5)
        cl(fig3, "Single vs Multi-Category Buyers")
        fig3.update_traces(textfont_color="#fff")
        st.plotly_chart(fig3, use_container_width=True)

    # ── Month-on-Month ────────────────────────────────────────────────────────
    st.markdown('<div class="section-header">Month-on-Month Performance — New vs Repeat</div>', unsafe_allow_html=True)
    if stats["monthly_ltv"]:
        mdf = pd.DataFrame(stats["monthly_ltv"]).sort_values("YEAR_MONTH")
        tab1,tab2 = st.tabs(["📈 LTV by Month","👥 Customers by Month"])

        with tab1:
            fig_l = go.Figure()
            for ctype, color in [("New","#888888"),("Repeat",RED)]:
                d = mdf[mdf["CUSTOMER_TYPE"]==ctype]
                # Total LTV bars — left Y axis
                fig_l.add_trace(go.Bar(
                    x=d["YEAR_MONTH"], y=d["LTV"],
                    name=f"{ctype} Total LTV",
                    marker_color=color, opacity=0.85,
                    yaxis="y1"
                ))
                # Avg LTV lines — right Y axis
                fig_l.add_trace(go.Scatter(
                    x=d["YEAR_MONTH"], y=d["AVG_LTV"],
                    name=f"{ctype} Avg LTV",
                    mode="lines+markers",
                    line=dict(color=color, dash="dot", width=2),
                    marker=dict(size=6),
                    yaxis="y2"
                ))
            fig_l.update_layout(
                paper_bgcolor=BG, plot_bgcolor=BG, font_color=FC, font_size=11,
                barmode="group",
                xaxis=dict(tickangle=-45, gridcolor=GC),
                yaxis=dict(
                    title="Total LTV ($)", gridcolor=GC, zerolinecolor=GC,
                    tickformat="$.2s",
                ),
                yaxis2=dict(
                    title=dict(text="Avg LTV ($)", font=dict(color="#aaa")),
                    overlaying="y", side="right",
                    tickformat="$,.0f",
                    showgrid=False,
                    tickfont=dict(color="#aaa"),
                ),
                legend=dict(orientation="h", y=1.12, bgcolor="rgba(0,0,0,0)", font=dict(color=FC, size=10)),
                margin=dict(t=50, b=40, l=10, r=60),
                height=420,
            )
            st.plotly_chart(fig_l, use_container_width=True)
            st.caption("Bars = Total LTV (left axis) · Dotted lines = Avg LTV per customer (right axis)")

        with tab2:
            fig_c = go.Figure()
            for ctype, color in [("New","#888888"),("Repeat",RED)]:
                d = mdf[mdf["CUSTOMER_TYPE"]==ctype]
                fig_c.add_trace(go.Bar(x=d["YEAR_MONTH"], y=d["CUSTOMERS"],
                                       name=ctype, marker_color=color, opacity=0.85,
                                       text=d["CUSTOMERS"], textposition="outside",
                                       textfont=dict(color=FC, size=9)))
            cl(fig_c)
            fig_c.update_layout(barmode="group", xaxis_tickangle=-45,
                                 legend=dict(orientation="h", y=1.1))
            st.plotly_chart(fig_c, use_container_width=True)

    # ── Avg LTV New vs Repeat ─────────────────────────────────────────────────
    st.markdown('<div class="section-header">Average LTV — New vs Repeat</div>', unsafe_allow_html=True)
    a1,a2 = st.columns(2)
    with a1:
        st.markdown(f"""<div class="filter-card" style="text-align:center;">
            <div class="metric-label">New Customer Avg LTV</div>
            <div style="font-size:28px;font-weight:700;color:#fff;margin:8px 0">{fmt_m(stats['new_avg_ltv'])}</div>
            <div class="metric-sub">{stats['new_customers']:,} customers</div>
        </div>""", unsafe_allow_html=True)
    with a2:
        st.markdown(f"""<div class="filter-card" style="text-align:center;border-top:2px solid {RED};">
            <div class="metric-label">Repeat Customer Avg LTV</div>
            <div style="font-size:28px;font-weight:700;color:{RED};margin:8px 0">{fmt_m(stats['repeat_avg_ltv'])}</div>
            <div class="metric-sub">{stats['repeat_customers']:,} customers</div>
        </div>""", unsafe_allow_html=True)

    # ── Channel Deep-Dive ─────────────────────────────────────────────────────
    st.markdown('<div class="section-header">Channel Deep-Dive</div>', unsafe_allow_html=True)
    ch_tab1, ch_tab2, ch_tab3 = st.tabs(["🎯 Demand Group","📡 Order Channel","🔀 Channel Flow Matrix"])

    with ch_tab1:
        dg1,dg2 = st.columns(2)
        with dg1:
            if stats["ch_group"]:
                df_g = pd.DataFrame(stats["ch_group"]).sort_values("CUSTOMERS", ascending=True)
                fig_g = px.bar(df_g, x="CUSTOMERS", y="ORDER_DEMAND_GROUP", orientation="h",
                               color_discrete_sequence=[RED], text="CUSTOMERS")
                fig_g.update_traces(texttemplate="%{text:,}", textfont_color=FC)
                cl(fig_g, "Order Channel — Demand Group (Customers)")
                st.plotly_chart(fig_g, use_container_width=True)
        with dg2:
            if stats["ent_group"]:
                df_eg = pd.DataFrame(stats["ent_group"]).sort_values("CUSTOMERS", ascending=True)
                fig_eg = px.bar(df_eg, x="CUSTOMERS", y="ENTERED_DEMAND_GROUP", orientation="h",
                                color_discrete_sequence=["#888888"], text="CUSTOMERS")
                fig_eg.update_traces(texttemplate="%{text:,}", textfont_color=FC)
                cl(fig_eg, "Entered Channel — Demand Group (Customers)")
                st.plotly_chart(fig_eg, use_container_width=True)

    with ch_tab2:
        cd1,cd2 = st.columns(2)
        with cd1:
            if stats["ch_desc"]:
                df_cd = pd.DataFrame(stats["ch_desc"]).sort_values("CUSTOMERS", ascending=True)
                fig_cd = px.bar(df_cd, x="CUSTOMERS", y="ORDER_CHANNEL_DESC", orientation="h",
                                color_discrete_sequence=[RED])
                cl(fig_cd, "Order Channel (Customers)")
                st.plotly_chart(fig_cd, use_container_width=True)
        with cd2:
            if stats["ent_desc"]:
                df_ed = pd.DataFrame(stats["ent_desc"]).sort_values("CUSTOMERS", ascending=True)
                fig_ed = px.bar(df_ed, x="CUSTOMERS", y="ENTERED_CHANNEL_DESC", orientation="h",
                                color_discrete_sequence=["#888888"])
                cl(fig_ed, "Entered Channel (Customers)")
                st.plotly_chart(fig_ed, use_container_width=True)

    with ch_tab3:
        if stats["ch_matrix"]:
            mx = pd.DataFrame(stats["ch_matrix"])
            pivot = mx.pivot_table(index="ENTERED_DEMAND_GROUP", columns="ORDER_DEMAND_GROUP",
                                   values="CUSTOMERS", aggfunc="sum", fill_value=0)
            fig_mx = go.Figure(data=go.Heatmap(
                z=pivot.values,
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale=[[0,"#111"],[0.5,"#661100"],[1,RED]],
                text=pivot.values,
                texttemplate="%{text:,}",
                textfont=dict(color="#fff", size=11),
                showscale=True,
            ))
            fig_mx.update_layout(
                paper_bgcolor=BG, plot_bgcolor=BG, font_color=FC,
                xaxis_title="Order Channel (Demand Group)",
                yaxis_title="Entered Channel (Demand Group)",
                margin=dict(t=20,b=20,l=20,r=20),
            )
            st.plotly_chart(fig_mx, use_container_width=True)
            st.caption("Matrix shows customer count moving from their Entered Channel (rows) to Order Channel (columns)")

    # ── Category & Subcategory LTV ────────────────────────────────────────────
    st.markdown('<div class="section-header">Category & Subcategory LTV</div>', unsafe_allow_html=True)
    ca1,ca2 = st.columns(2)

    with ca1:
        if stats["cat_ltv"]:
            cat_df = pd.DataFrame(stats["cat_ltv"].items(), columns=["Category","LTV"])
            cat_df = cat_df.sort_values("LTV", ascending=True)
            cat_df["LTV_FMT"] = cat_df["LTV"].apply(fmt_m)
            fig_cat = px.bar(cat_df, x="LTV", y="Category", orientation="h",
                             color_discrete_sequence=[RED], text="LTV_FMT")
            fig_cat.update_traces(textfont_color=FC, textposition="outside")
            cl(fig_cat, "LTV by Category (Masked)")
            st.plotly_chart(fig_cat, use_container_width=True)

    with ca2:
        if stats["sub_ltv"]:
            sub_df = pd.DataFrame(stats["sub_ltv"].items(), columns=["Subcategory","LTV"])
            sub_df = sub_df.sort_values("LTV", ascending=True)
            sub_df["LTV_FMT"] = sub_df["LTV"].apply(fmt_m)
            fig_sub = px.bar(sub_df, x="LTV", y="Subcategory", orientation="h",
                             color_discrete_sequence=["#888888"], text="LTV_FMT")
            fig_sub.update_traces(textfont_color=FC, textposition="outside")
            cl(fig_sub, "LTV by Subcategory (Masked)")
            st.plotly_chart(fig_sub, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — SEGMENT STUDIO
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Segment Studio":
    st.markdown('<div class="page-title">👥 Segment Studio</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Build precision audience segments using behavioral, transactional, and consent signals.</div>', unsafe_allow_html=True)

    if st.session_state.intel is None:
        st.warning("No data ingested. Go to **Data Ingestion** first.")
        st.stop()

    intel   = st.session_state.intel
    stats   = st.session_state.stats
    cat_map = stats.get("cat_map", {})
    sub_map = stats.get("sub_map", {})

    st.markdown("---")
    f1, f2 = st.columns([1, 2])

    with f1:
        st.markdown('<div class="section-header">Segment Filters</div>', unsafe_allow_html=True)

        # RFM Tier
        rfm_opts = sorted(intel["RFM_TIER"].dropna().unique().tolist())
        sel_rfm  = st.multiselect("RFM Tier", rfm_opts, default=rfm_opts)

        # Order Channel Group
        dg_opts = sorted(intel["ORDER_DEMAND_GROUP"].dropna().unique().tolist()) if "ORDER_DEMAND_GROUP" in intel.columns else []
        sel_dg  = st.multiselect("Order Channel Group", dg_opts, default=dg_opts)

        # Entered Channel Group
        ent_dg_opts = sorted(intel["ENTERED_DEMAND_GROUP"].dropna().unique().tolist()) if "ENTERED_DEMAND_GROUP" in intel.columns else []
        sel_ent_dg  = st.multiselect("Entered Channel Group", ent_dg_opts, default=ent_dg_opts)

        # Category (masked)
        raw_cats  = sorted(intel["TOP_CATEGORY"].dropna().unique().tolist())
        mask_cats = [cat_map.get(c, c) for c in raw_cats]
        cat_disp  = st.multiselect("Top Category", mask_cats, default=[])
        rev_cat   = {v: k for k, v in cat_map.items()}
        sel_cats  = [rev_cat.get(c, c) for c in cat_disp]

        # Subcategory (masked)
        raw_subs  = sorted(intel["TOP_SUBCATEGORY"].dropna().unique().tolist()) if "TOP_SUBCATEGORY" in intel.columns else []
        mask_subs = [sub_map.get(s, s) for s in raw_subs]
        sub_disp  = st.multiselect("Top Subcategory", mask_subs, default=[])
        rev_sub   = {v: k for k, v in sub_map.items()}
        sel_subs  = [rev_sub.get(s, s) for s in sub_disp]

        # Customer Type
        sel_ctype = st.multiselect("Customer Type", ["New", "Repeat"], default=["New", "Repeat"])

        # Consent
        st.markdown("**Consent**")
        email_only = st.checkbox("Email opted-in only")
        sms_only   = st.checkbox("SMS opted-in only")

        # Avg LTV Range
        st.markdown("**Avg LTV Range ($)**")
        ltv_min_val = float(intel["LTV"].min())
        ltv_max_val = float(intel["LTV"].max())
        min_ltv = st.number_input("Min Avg LTV", min_value=0.0, value=ltv_min_val, step=10.0)
        max_ltv = st.number_input("Max Avg LTV", min_value=0.0, value=ltv_max_val, step=10.0)

        build_btn = st.button("🔍 Build Segment")

    with f2:
        st.markdown('<div class="section-header">Segment Preview</div>', unsafe_allow_html=True)

        if build_btn:
            filtered = intel.copy()

            # Channel group filters
            if sel_dg and len(sel_dg) < len(dg_opts) and "ORDER_DEMAND_GROUP" in filtered.columns:
                filtered = filtered[filtered["ORDER_DEMAND_GROUP"].isin(sel_dg)]
            if sel_ent_dg and len(sel_ent_dg) < len(ent_dg_opts) and "ENTERED_DEMAND_GROUP" in filtered.columns:
                filtered = filtered[filtered["ENTERED_DEMAND_GROUP"].isin(sel_ent_dg)]

            # Category filters
            if sel_cats:
                filtered = filtered[filtered["TOP_CATEGORY"].isin(sel_cats)]
            if sel_subs and "TOP_SUBCATEGORY" in filtered.columns:
                filtered = filtered[filtered["TOP_SUBCATEGORY"].isin(sel_subs)]

            # RFM tier
            if sel_rfm and len(sel_rfm) < len(rfm_opts):
                filtered = filtered[filtered["RFM_TIER"].isin(sel_rfm)]

            # Customer type
            if sel_ctype and len(sel_ctype) < 2:
                filtered = filtered[filtered["CUSTOMER_TYPE"].isin(sel_ctype)]

            # Consent
            if email_only:
                filtered = filtered[(filtered["EMAIL_OPTIN"] == 1) & (filtered["EMAIL_OPTOUT"] == 0)]
            if sms_only:
                filtered = filtered[(filtered["SMS_OPTIN"] == 1) & (filtered["SMS_OPTOUT"] == 0)]

            # Avg LTV range
            filtered = filtered[(filtered["LTV"] >= min_ltv) & (filtered["LTV"] <= max_ltv)]

            st.session_state.filtered = filtered.reset_index(drop=True)

        if st.session_state.filtered is not None:
            filtered = st.session_state.filtered
            size = len(filtered)

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Audience",   f"{size:,}")
            m2.metric("Email OK",   f"{int((filtered['EMAIL_OPTIN']==1).sum()):,}")
            m3.metric("SMS OK",     f"{int((filtered['SMS_OPTIN']==1).sum()):,}")
            m4.metric("Avg LTV",    f"${filtered['LTV'].mean():,.0f}" if size > 0 else "$0")

            if size > 0:
                r1, r2 = st.columns(2)
                with r1:
                    seg_rfm = filtered["RFM_TIER"].value_counts().reset_index()
                    seg_rfm.columns = ["Tier", "Count"]
                    fig_r = px.bar(seg_rfm, x="Tier", y="Count", color_discrete_sequence=[RED])
                    fig_r.update_layout(paper_bgcolor=BG, plot_bgcolor=BG, font_color=FC,
                                        margin=dict(t=10,b=10), showlegend=False,
                                        xaxis=dict(gridcolor=GC), yaxis=dict(gridcolor=GC))
                    st.plotly_chart(fig_r, use_container_width=True)

                with r2:
                    nv = filtered["CUSTOMER_TYPE"].value_counts().reset_index()
                    nv.columns = ["Type", "Count"]
                    fig_nv = px.pie(nv, names="Type", values="Count",
                                    color_discrete_sequence=[RED, "#888888"], hole=0.5)
                    fig_nv.update_layout(paper_bgcolor=BG, font_color=FC,
                                         margin=dict(t=10,b=10),
                                         legend=dict(font=dict(color=FC)))
                    fig_nv.update_traces(textfont_color="#fff")
                    st.plotly_chart(fig_nv, use_container_width=True)

            st.markdown("---")
            seg_name = st.text_input("Segment Name", placeholder="e.g. High-Value Repeat Buyers Q1")
            if seg_name and size > 0:
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("💾 Save Segment"):
                        save_segment(seg_name, {}, filtered["GUID"])
                        st.success(f"'{seg_name}' saved!")
                with c2:
                    exp = export_segment(filtered, seg_name)
                    st.download_button("⬇️ Export CSV",
                        data=exp.to_csv(index=False).encode(),
                        file_name=f"{seg_name.replace(' ','_')}.csv", mime="text/csv")
                st.dataframe(exp.head(10), use_container_width=True)
            elif size == 0:
                st.warning("No profiles match. Adjust your filters.")
        else:
            st.info("Set filters and click **Build Segment**.")


# PAGE 4 — CAMPAIGN COMPOSER
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Campaign Composer":
    st.markdown('<div class="page-title">📧 Campaign Composer</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Generate AI-personalized campaign copy tailored to your segment profile.</div>', unsafe_allow_html=True)

    if st.session_state.intel is None:
        st.warning("No data ingested. Go to **Data Ingestion** first.")
        st.stop()
    if st.session_state.filtered is None:
        st.warning("No segment built. Go to **Segment Studio** first.")
        st.stop()

    filtered = st.session_state.filtered
    stats    = get_summary_stats(filtered) if len(filtered) > 0 else {}

    st.markdown("---")
    left, right = st.columns([1, 2])

    with left:
        st.markdown('<div class="section-header">Campaign Setup</div>', unsafe_allow_html=True)

        seg_name = st.text_input("Campaign Name", placeholder="e.g. Spring Re-Engagement Drive")

        goal = st.selectbox("Campaign Objective", [
            "Drive repeat purchase",
            "Reactivate lapsed customers",
            "Promote new product category",
            "Reward loyal customers",
            "Upsell high-value customers",
            "Win back at-risk customers",
        ])

        st.markdown('<div class="section-header">Copy Guidance</div>', unsafe_allow_html=True)

        keywords = st.text_area(
            "Keywords & Tone",
            placeholder="e.g. summer sale, exclusive offer, urgency, warm tone, loyalty reward...",
            height=90,
            help="Add keywords, offers, or tone guidance to shape the email copy"
        )

        st.markdown("**Product Links** (optional — up to 3)")
        link1 = st.text_input("Product Link 1", placeholder="https://yoursite.com/product1")
        link2 = st.text_input("Product Link 2", placeholder="https://yoursite.com/product2")
        link3 = st.text_input("Product Link 3", placeholder="https://yoursite.com/product3")
        product_links = [l for l in [link1, link2, link3] if l.strip()]

        st.markdown('<div class="section-header">Segment Summary</div>', unsafe_allow_html=True)
        st.markdown(f"""<div class="filter-card">
            <div class="metric-label">Audience Size</div>
            <div style="font-size:20px;font-weight:700;color:#fff;margin:4px 0">{len(filtered):,}</div>
            <div class="metric-label" style="margin-top:10px">Email Eligible</div>
            <div style="font-size:16px;color:{RED};margin:4px 0">{int((filtered["EMAIL_OPTIN"]==1).sum()):,}</div>
            <div class="metric-label" style="margin-top:10px">Avg LTV</div>
            <div style="font-size:16px;color:#888;margin:4px 0">${filtered["LTV"].mean():,.0f}</div>
        </div>""", unsafe_allow_html=True)

        gen_btn = st.button("✨ Generate Campaign Copy")

    with right:
        st.markdown('<div class="section-header">Generated Copy</div>', unsafe_allow_html=True)

        if gen_btn:
            if not seg_name:
                st.error("Please enter a campaign name.")
            else:
                with st.spinner("Generating personalized copy with Groq AI..."):
                    try:
                        copy = generate_campaign_copy(
                            seg_name, stats, goal,
                            keywords=keywords,
                            product_links=product_links,
                        )
                        st.session_state.campaign_copy = copy
                    except Exception as e:
                        st.error(f"API error: {e}")

        if st.session_state.campaign_copy:
            copy = st.session_state.campaign_copy

            st.markdown("**Subject Line**")
            st.info(copy.get("subject", ""))

            st.markdown("**Preview Text**")
            st.info(copy.get("preview", ""))

            st.markdown("**Email Body**")
            body_html = copy.get("body", "").replace(chr(10), "<br>")
            st.markdown(f'<div class="campaign-box">{body_html}</div>', unsafe_allow_html=True)

            st.markdown("**Call to Action**")
            st.success(copy.get("cta", ""))

            st.markdown("---")
            # Show product links used
            if product_links:
                st.markdown("**Product Links Embedded**")
                for i, link in enumerate(product_links, 1):
                    st.markdown(f"`Product {i}:` {link}")

            full = f"""SUBJECT: {copy.get("subject","")}
PREVIEW: {copy.get("preview","")}

{copy.get("body","")}

CTA: {copy.get("cta","")}"""

            st.download_button(
                "⬇️ Download Copy",
                data=full.encode(),
                file_name="campaign_copy.txt",
                mime="text/plain"
            )
        else:
            st.info("Fill in campaign details and click **Generate Campaign Copy**.")