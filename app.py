import streamlit as st
import tempfile
import os
import pandas as pd
from pipeline import run_pipeline

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
MASTER_CSV = "cases_master_cleaned_FINAL_UPDATED.csv"

st.set_page_config(
    page_title="Deadlining",
    page_icon="🗂️",
    layout="wide"
)

# ---------------------------------------------------------
# MASTER DATA LOADER
# ---------------------------------------------------------
@st.cache_data
def load_master_data():
    if os.path.exists(MASTER_CSV):
        return pd.read_csv(MASTER_CSV)
    return pd.DataFrame()

# initialize displayed dataframe on app open
if "current_df" not in st.session_state:
    st.session_state.current_df = load_master_data()

# ---------------------------------------------------------
# FORENSIC STYLING
# ---------------------------------------------------------
st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=Inter:wght@300;400;700&family=Playfair+Display:ital,wght@0,700;1,700&display=swap" rel="stylesheet">

<style>
    [data-testid="stAppViewContainer"] {
        background-color: #05070a;
        background-image: radial-gradient(circle at 2px 2px, rgba(255,255,255,0.015) 1px, transparent 0);
        background-size: 32px 32px;
    }

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
        color: #e2e8f0;
    }

    .mono {
        font-family: 'IBM Plex Mono', monospace !important;
        letter-spacing: 0.05em;
    }

    .hero {
        padding: 2.5rem 2rem;
        border: 1px solid rgba(255,255,255,0.08);
        border-left: 5px solid #9b1c1c;
        background: linear-gradient(135deg, rgba(15,23,42,0.9), rgba(10,15,28,0.95));
        border-radius: 4px;
        margin-bottom: 2rem;
    }

    .hero-kicker {
        font-family: 'IBM Plex Mono', monospace;
        color: #9b1c1c;
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.25em;
        margin-bottom: 0.5rem;
    }

    .hero h1 {
        font-family: 'Georgia', serif;
        font-size: 3.2rem;
        font-weight: 700;
        color: #ffffff;
        margin: 0 0 0.5rem 0;
    }

    .soft-card {
        background: rgba(15, 23, 42, 0.7);
        backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.08);
        padding: 1.5rem;
        border-radius: 8px;
        margin-bottom: 1rem;
    }

    .metric-box {
        background: #0a0f1a;
        border: 1px solid #1e293b;
        padding: 1.2rem;
        border-radius: 4px;
        text-align: left;
    }

    .metric-label {
        font-family: 'IBM Plex Mono', monospace;
        color: #64748b;
        font-size: 0.65rem;
        text-transform: uppercase;
        margin-bottom: 0.4rem;
    }

    .metric-value {
        color: #f8fafc;
        font-size: 1.8rem;
        font-weight: 700;
        font-family: 'Georgia', serif;
    }

    .stButton > button {
        width: 100%;
        background: transparent;
        border: 1px solid #9b1c1c !important;
        color: #9b1c1c !important;
        font-family: 'IBM Plex Mono', monospace;
        font-weight: 600;
        border-radius: 2px;
        transition: 0.2s ease;
    }

    .stButton > button:hover {
        background: #9b1c1c !important;
        color: white !important;
    }

    [data-testid="stSidebar"] {
        background-color: #030508;
        border-right: 1px solid #1e293b;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# HEADER
# ---------------------------------------------------------
st.markdown("""
<div class="hero">
    <div class="hero-kicker">Spatial Forensic Research Interface</div>
    <h1>Deadlining</h1>
    <p style="color: #94a3b8; font-family: 'Georgia', serif; font-style: italic; max-width: 850px; line-height: 1.6;">
        A spatial data pipeline for analyzing patterns of disappearance, homicide, and institutional neglect 
        affecting Black women in Washington, D.C.
    </p>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# TOP LAYOUT
# ---------------------------------------------------------
left, right = st.columns([1.05, 0.95], gap="large")

with left:
    st.markdown('<div class="soft-card">', unsafe_allow_html=True)
    st.markdown('<div class="mono" style="font-size:0.9rem; color:white; margin-bottom:1rem;">[ DATA_INTAKE ]</div>', unsafe_allow_html=True)
    uploaded_file = st.file_uploader("Upload intake CSV", type=["csv"])
    replace_master = st.checkbox("Replace master file", value=False)
    run_clicked = st.button("RUN PIPELINE SEQUENCE")
    st.markdown('</div>', unsafe_allow_html=True)

with right:
    st.markdown('<div class="soft-card">', unsafe_allow_html=True)
    st.markdown('<div class="mono" style="font-size:0.9rem; color:white; margin-bottom:1rem;">[ SYSTEM_PROTOCOLS ]</div>', unsafe_allow_html=True)
    st.markdown("""
    <div style="font-size: 0.85rem; color: #94a3b8; line-height: 1.8;">
        • SCHEMA ALIGNMENT: Matching intake rows to master structure.<br>
        • NORMALIZATION: Standardizing IDs and text fields.<br>
        • GEO-FIDELITY: Verifying spatial coordinates via geocoding.<br>
        • AUDIT: Flagging records for human-in-the-loop review.
    </div>
    """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# ---------------------------------------------------------
# EXECUTION
# ---------------------------------------------------------
if uploaded_file is not None and run_clicked:
    with st.spinner("Processing Forensic Sequence..."):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp.write(uploaded_file.getbuffer())
            temp_path = tmp.name

        try:
            df = run_pipeline(temp_path, replace_master=replace_master)
            st.session_state.current_df = df
            st.cache_data.clear()
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    st.success("Sequence Complete.")

elif uploaded_file is None and run_clicked:
    st.warning("Please upload a CSV first.")

# ---------------------------------------------------------
# ALWAYS DISPLAY CURRENT MASTER / CURRENT SESSION DATA
# ---------------------------------------------------------
df = st.session_state.current_df

if df is not None and not df.empty:
    total_cases = len(df)
    homicide_count = int((df["case_type"] == "Homicide").sum()) if "case_type" in df.columns else 0
    missing_count = int((df["case_type"] == "Missing").sum()) if "case_type" in df.columns else 0
    unidentified_count = int((df["case_type"] == "Unidentified").sum()) if "case_type" in df.columns else 0
    review_count = int((df["review_needed"] == 1).sum()) if "review_needed" in df.columns else 0
    mapped_count = int((df["usable_for_mapping"] == 1).sum()) if "usable_for_mapping" in df.columns else 0

    st.markdown("### Dataset Summary")
    m1, m2, m3, m4, m5, m6 = st.columns(6)

    metrics = [
        ("Total Cases", total_cases),
        ("Homicides", homicide_count),
        ("Missing", missing_count),
        ("Unidentified", unidentified_count),
        ("Needs Review", review_count),
        ("Mappable", mapped_count),
    ]

    for col, (label, value) in zip([m1, m2, m3, m4, m5, m6], metrics):
        with col:
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-label">{label}</div>
                <div class="metric-value">{value}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("### Spatial Preview")
    if {"latitude", "longitude"}.issubset(df.columns):
        map_df = df.copy()
        map_df["latitude"] = pd.to_numeric(map_df["latitude"], errors="coerce")
        map_df["longitude"] = pd.to_numeric(map_df["longitude"], errors="coerce")
        map_df = map_df.dropna(subset=["latitude", "longitude"])

        if len(map_df) > 0:
            st.map(map_df[["latitude", "longitude"]], use_container_width=True)
        else:
            st.info("No valid coordinates available for mapping yet.")
    else:
        st.info("Latitude/longitude columns not found.")

    st.markdown("### Review & Data Tables")
    tab1, tab2, tab3 = st.tabs(["All Cases", "Needs Review", "Mappable Cases"])

    with tab1:
        st.dataframe(df, use_container_width=True)

    with tab2:
        if "review_needed" in df.columns:
            st.dataframe(df[df["review_needed"] == 1], use_container_width=True)
        else:
            st.info("No review flag column found.")

    with tab3:
        if "usable_for_mapping" in df.columns:
            st.dataframe(df[df["usable_for_mapping"] == 1], use_container_width=True)
        else:
            st.info("No mappable flag column found.")

    st.markdown("### Export Files")
    d1, d2, d3 = st.columns(3)

    with d1:
        st.download_button(
            label="Download updated master CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name=MASTER_CSV,
            mime="text/csv",
            use_container_width=True
        )

    with d2:
        if "review_needed" in df.columns:
            review_df = df[df["review_needed"] == 1].copy()
            st.download_button(
                label="Download needs-review CSV",
                data=review_df.to_csv(index=False).encode("utf-8"),
                file_name="cases_needing_review.csv",
                mime="text/csv",
                use_container_width=True
            )

    with d3:
        if "usable_for_mapping" in df.columns:
            mappable_df = df[df["usable_for_mapping"] == 1].copy()
            st.download_button(
                label="Download mappable cases CSV",
                data=mappable_df.to_csv(index=False).encode("utf-8"),
                file_name="cases_mappable.csv",
                mime="text/csv",
                use_container_width=True
            )

else:
    st.info(f"No master data found yet. Add `{MASTER_CSV}` to the repo, or run the pipeline to generate it.")
