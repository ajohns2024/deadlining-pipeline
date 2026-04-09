import streamlit as st
import tempfile
import os
import pandas as pd

from pipeline import run_pipeline

st.set_page_config(
    page_title="Deadlining Pipeline",
    page_icon="🗂️",
    layout="wide"
)

# ---------------------------------------------------------
# STYLING
# ---------------------------------------------------------
st.markdown("""
<style>
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1250px;
    }

    .hero {
        padding: 2rem 2rem 1.5rem 2rem;
        border-radius: 24px;
        background: linear-gradient(135deg, #111827 0%, #0f172a 45%, #1e293b 100%);
        border: 1px solid rgba(255,255,255,0.08);
        box-shadow: 0 10px 30px rgba(0,0,0,0.25);
        margin-bottom: 1.25rem;
    }

    .hero h1 {
        color: white;
        font-size: 3rem;
        margin: 0 0 0.35rem 0;
        line-height: 1.05;
    }

    .hero p {
        color: #cbd5e1;
        font-size: 1rem;
        margin: 0;
        max-width: 900px;
        line-height: 1.6;
    }

    .soft-card {
        background: #111827;
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 20px;
        padding: 1.1rem 1.1rem 1rem 1.1rem;
        margin-bottom: 1rem;
    }

    .section-title {
        color: white;
        font-size: 1.15rem;
        font-weight: 700;
        margin-bottom: 0.75rem;
    }

    .small-note {
        color: #94a3b8;
        font-size: 0.92rem;
        line-height: 1.5;
    }

    .metric-box {
        background: #111827;
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 18px;
        padding: 1rem;
        text-align: center;
    }

    .metric-label {
        color: #94a3b8;
        font-size: 0.9rem;
        margin-bottom: 0.35rem;
    }

    .metric-value {
        color: white;
        font-size: 1.8rem;
        font-weight: 800;
    }

    div[data-testid="stFileUploader"] {
        background: #111827;
        border-radius: 18px;
        padding: 0.5rem;
        border: 1px solid rgba(255,255,255,0.08);
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# HEADER
# ---------------------------------------------------------
st.markdown("""
<div class="hero">
    <h1>Deadlining Case Intake Pipeline</h1>
    <p>
        Upload a new intake CSV, standardize and integrate incoming case records,
        geocode available addresses, review data quality, and export an updated master dataset
        for spatial analysis of missing, murdered, and unidentified Black women in Washington, D.C.
    </p>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# LAYOUT
# ---------------------------------------------------------
left, right = st.columns([1.1, 0.9], gap="large")

with left:
    st.markdown('<div class="soft-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Upload & Run</div>', unsafe_allow_html=True)

    uploaded_file = st.file_uploader("Upload intake CSV", type=["csv"])
    replace_master = st.checkbox("Replace master file", value=False)

    st.markdown(
        '<div class="small-note">Use this to append a new intake file to your current master dataset. '
        'If “Replace master file” is checked, the updated file becomes the new master inside the app environment for that session.</div>',
        unsafe_allow_html=True
    )

    run_clicked = st.button("Run Pipeline", use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

with right:
    st.markdown('<div class="soft-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">What this app does</div>', unsafe_allow_html=True)
    st.markdown("""
    <div class="small-note">
        • Aligns incoming data to the master schema<br>
        • Standardizes case types and names<br>
        • Preserves existing case IDs and assigns missing ones<br>
        • Geocodes rows with valid addresses and missing coordinates<br>
        • Flags rows needing review<br>
        • Produces an export-ready updated master CSV
    </div>
    """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# ---------------------------------------------------------
# RUN PIPELINE
# ---------------------------------------------------------
if uploaded_file is not None and run_clicked:
    with st.spinner("Running pipeline..."):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp.write(uploaded_file.getbuffer())
            temp_path = tmp.name

        try:
            df = run_pipeline(temp_path, replace_master=replace_master)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    st.success("Pipeline complete.")

    # -----------------------------------------------------
    # PREP
    # -----------------------------------------------------
    total_cases = len(df)
    homicide_count = int((df["case_type"] == "Homicide").sum()) if "case_type" in df.columns else 0
    missing_count = int((df["case_type"] == "Missing").sum()) if "case_type" in df.columns else 0
    unidentified_count = int((df["case_type"] == "Unidentified").sum()) if "case_type" in df.columns else 0
    review_count = int((df["review_needed"] == 1).sum()) if "review_needed" in df.columns else 0
    mapped_count = int((df["usable_for_mapping"] == 1).sum()) if "usable_for_mapping" in df.columns else 0

    # -----------------------------------------------------
    # METRICS
    # -----------------------------------------------------
    st.markdown("### Summary")
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

    st.write("")

    # -----------------------------------------------------
    # CHARTS
    # -----------------------------------------------------
    c1, c2 = st.columns(2, gap="large")

    with c1:
        st.markdown('<div class="soft-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Case Type Breakdown</div>', unsafe_allow_html=True)
        if "case_type" in df.columns:
            case_counts = df["case_type"].fillna("Unknown").value_counts()
            st.bar_chart(case_counts)
        else:
            st.info("No case_type column found.")
        st.markdown('</div>', unsafe_allow_html=True)

    with c2:
        st.markdown('<div class="soft-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Data Quality Overview</div>', unsafe_allow_html=True)
        if "review_needed" in df.columns:
            quality_counts = df["review_needed"].map({0: "Clean / usable", 1: "Needs review"}).fillna("Unknown").value_counts()
            st.bar_chart(quality_counts)
        else:
            st.info("No review_needed column found.")
        st.markdown('</div>', unsafe_allow_html=True)

    # -----------------------------------------------------
    # MAP
    # -----------------------------------------------------
    st.markdown("### Map Preview")

    if {"latitude", "longitude"}.issubset(df.columns):
        map_df = df.copy()

        map_df["latitude"] = pd.to_numeric(map_df["latitude"], errors="coerce")
        map_df["longitude"] = pd.to_numeric(map_df["longitude"], errors="coerce")

        map_df = map_df.dropna(subset=["latitude", "longitude"])

        if len(map_df) > 0:
            st.markdown(
                '<div class="small-note">This map shows all records with valid coordinates in the updated dataset.</div>',
                unsafe_allow_html=True
            )
            st.map(map_df[["latitude", "longitude"]], use_container_width=True)
        else:
            st.info("No mappable cases yet. Add latitude/longitude or geocodable addresses to see map output.")
    else:
        st.info("Latitude and longitude columns not found.")

    # -----------------------------------------------------
    # FILTERED TABLES
    # -----------------------------------------------------
    st.markdown("### Review & Preview")

    tab1, tab2, tab3 = st.tabs(["All Cases", "Needs Review", "Mappable Cases"])

    with tab1:
        st.dataframe(df, use_container_width=True, height=400)

    with tab2:
        if "review_needed" in df.columns:
            review_df = df[df["review_needed"] == 1].copy()
            st.dataframe(review_df, use_container_width=True, height=400)
        else:
            review_df = pd.DataFrame()
            st.info("No review_needed column found.")

    with tab3:
        if "usable_for_mapping" in df.columns:
            mappable_df = df[df["usable_for_mapping"] == 1].copy()
            st.dataframe(mappable_df, use_container_width=True, height=400)
        else:
            mappable_df = pd.DataFrame()
            st.info("No usable_for_mapping column found.")

    # -----------------------------------------------------
    # DOWNLOADS
    # -----------------------------------------------------
    st.markdown("### Downloads")

    d1, d2, d3 = st.columns(3)

    with d1:
        st.download_button(
            label="Download updated master CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="cases_master_cleaned_FINAL_UPDATED.csv",
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

elif uploaded_file is None and run_clicked:
    st.warning("Please upload a CSV first.")
