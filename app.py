import streamlit as st
import tempfile
import os
import pandas as pd

from pipeline import run_pipeline

st.set_page_config(
    page_title="Deadlining",
    page_icon="🗂️",
    layout="wide"
)

# ---------------------------------------------------------
# STYLING
# ---------------------------------------------------------
st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=Playfair+Display:wght@500;600;700;800&display=swap" rel="stylesheet">

<style>
html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

[data-testid="stAppViewContainer"] {
    background:
        radial-gradient(circle at top left, rgba(99,102,241,0.10), transparent 28%),
        radial-gradient(circle at top right, rgba(168,85,247,0.10), transparent 24%),
        linear-gradient(180deg, #07111f 0%, #0b1220 100%);
}

.block-container {
    padding-top: 2rem;
    padding-bottom: 2rem;
    max-width: 1280px;
}

.hero {
    padding: 2.8rem 2.6rem 2.2rem 2.6rem;
    border-radius: 30px;
    background:
        radial-gradient(circle at top left, rgba(129,140,248,0.22), transparent 26%),
        linear-gradient(135deg, rgba(15,23,42,0.95), rgba(17,24,39,0.96) 55%, rgba(30,41,59,0.92));
    border: 1px solid rgba(255,255,255,0.08);
    box-shadow: 0 24px 60px rgba(0,0,0,0.35);
    margin-bottom: 1.4rem;
}

.hero-kicker {
    color: #c4b5fd;
    text-transform: uppercase;
    letter-spacing: 0.18em;
    font-size: 0.78rem;
    font-weight: 700;
    margin-bottom: 0.8rem;
}

.hero h1 {
    font-family: 'Playfair Display', serif;
    font-size: 3.5rem;
    color: white;
    margin: 0 0 0.6rem 0;
    line-height: 1.02;
}

.hero p {
    color: #d2d9e6;
    font-size: 1.04rem;
    max-width: 930px;
    line-height: 1.75;
    margin-bottom: 0;
}

.section-title {
    font-size: 1.12rem;
    font-weight: 700;
    color: white;
    margin-bottom: 0.85rem;
    letter-spacing: 0.01em;
}

.soft-card {
    background: rgba(15, 23, 42, 0.68);
    backdrop-filter: blur(16px);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 22px;
    padding: 1.25rem;
    margin-bottom: 1rem;
    box-shadow: 0 10px 25px rgba(0,0,0,0.18);
}

.metric-box {
    background:
        linear-gradient(145deg, rgba(15,23,42,0.95), rgba(17,24,39,0.92));
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 18px;
    padding: 1rem 0.8rem;
    text-align: center;
    min-height: 118px;
    display: flex;
    flex-direction: column;
    justify-content: center;
}

.metric-label {
    color: #9fb0c7;
    font-size: 0.84rem;
    margin-bottom: 0.35rem;
    font-weight: 500;
}

.metric-value {
    color: white;
    font-size: 1.95rem;
    font-weight: 800;
    line-height: 1.1;
}

.small-note {
    color: #a8b5c7;
    font-size: 0.94rem;
    line-height: 1.65;
}

.detail-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0,1fr));
    gap: 0.75rem;
    margin-top: 0.75rem;
}

.detail-item {
    padding: 0.8rem 0.9rem;
    border-radius: 14px;
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.05);
}

.detail-label {
    color: #94a3b8;
    font-size: 0.8rem;
    margin-bottom: 0.25rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}

.detail-value {
    color: white;
    font-size: 0.97rem;
    font-weight: 600;
}

.stButton > button {
    background: linear-gradient(135deg, #6366f1, #8b5cf6);
    border: none;
    color: white;
    font-weight: 700;
    border-radius: 14px;
    padding: 0.72rem 1rem;
    box-shadow: 0 8px 20px rgba(99,102,241,0.25);
    transition: all 0.18s ease;
}

.stButton > button:hover {
    transform: translateY(-1px);
    background: linear-gradient(135deg, #7c83ff, #a78bfa);
}

.stDownloadButton > button {
    border-radius: 14px;
    font-weight: 700;
}

div[data-testid="stFileUploader"] {
    background: rgba(15,23,42,0.72);
    border-radius: 18px;
    padding: 0.65rem;
    border: 1px solid rgba(255,255,255,0.06);
}

[data-testid="stCheckbox"] label {
    color: #d6deea !important;
}

[data-testid="stMarkdownContainer"] p {
    line-height: 1.65;
}

h3 {
    color: white !important;
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
    <p>
        A spatial data pipeline for analyzing patterns of disappearance, homicide, and institutional neglect
        affecting Black women in Washington, D.C.
        <br><br>
        This system standardizes intake data, preserves case structure, geocodes available addresses,
        flags records needing review, and prepares export-ready datasets for mapping, spatial analysis,
        and research publication.
    </p>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# TOP LAYOUT
# ---------------------------------------------------------
left, right = st.columns([1.05, 0.95], gap="large")

with left:
    st.markdown('<div class="soft-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Upload & Run</div>', unsafe_allow_html=True)

    uploaded_file = st.file_uploader("Upload intake CSV", type=["csv"])
    replace_master = st.checkbox("Replace master file", value=False)

    st.markdown(
        """
        <div class="small-note">
            Upload a new intake file to append and standardize against your existing master dataset.
            If <strong>Replace master file</strong> is selected, the updated output becomes the working
            master inside the app environment for that run.
        </div>
        """,
        unsafe_allow_html=True
    )

    run_clicked = st.button("Run Pipeline", use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

with right:
    st.markdown('<div class="soft-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">System Details</div>', unsafe_allow_html=True)
    st.markdown("""
    <div class="small-note">
        This interface is designed for iterative case ingestion and research-grade cleaning.
    </div>

    <div class="detail-grid">
        <div class="detail-item">
            <div class="detail-label">Schema Alignment</div>
            <div class="detail-value">Incoming rows are matched to the master structure.</div>
        </div>
        <div class="detail-item">
            <div class="detail-label">Case Standardization</div>
            <div class="detail-value">Names, case types, IDs, and text fields are normalized.</div>
        </div>
        <div class="detail-item">
            <div class="detail-label">Geocoding</div>
            <div class="detail-value">Rows with usable addresses and missing coordinates are geocoded.</div>
        </div>
        <div class="detail-item">
            <div class="detail-label">Quality Review</div>
            <div class="detail-value">Incomplete or spatially unusable records are flagged for review.</div>
        </div>
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
    # PREP SUMMARY VALUES
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

    st.write("")

    # -----------------------------------------------------
    # RESEARCH DETAILS
    # -----------------------------------------------------
    a1, a2 = st.columns(2, gap="large")

    with a1:
        st.markdown('<div class="soft-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Pipeline Output</div>', unsafe_allow_html=True)

        percent_review = round((review_count / total_cases) * 100, 1) if total_cases else 0
        percent_mappable = round((mapped_count / total_cases) * 100, 1) if total_cases else 0

        st.markdown(f"""
        <div class="detail-grid">
            <div class="detail-item">
                <div class="detail-label">Rows Processed</div>
                <div class="detail-value">{total_cases}</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">Mappable Share</div>
                <div class="detail-value">{percent_mappable}%</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">Needs Review Share</div>
                <div class="detail-value">{percent_review}%</div>
            </div>
            <div class="detail-item">
                <div class="detail-label">Primary Output</div>
                <div class="detail-value">cases_master_cleaned_FINAL_UPDATED.csv</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)

    with a2:
        st.markdown('<div class="soft-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Research Context</div>', unsafe_allow_html=True)
        st.markdown("""
        <div class="small-note">
            This interface supports iterative intake and spatial preparation for case-based research on
            disappearance, homicide, and racialized institutional neglect. The updated outputs are structured
            for mapping workflows, dataset auditing, and downstream statistical or spatial analysis.
        </div>
        """, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

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
            quality_counts = (
                df["review_needed"]
                .map({0: "Clean / usable", 1: "Needs review"})
                .fillna("Unknown")
                .value_counts()
            )
            st.bar_chart(quality_counts)
        else:
            st.info("No review_needed column found.")
        st.markdown('</div>', unsafe_allow_html=True)

    # -----------------------------------------------------
    # MAP
    # -----------------------------------------------------
    st.markdown("### Spatial Preview")

    if {"latitude", "longitude"}.issubset(df.columns):
        map_df = df.copy()
        map_df["latitude"] = pd.to_numeric(map_df["latitude"], errors="coerce")
        map_df["longitude"] = pd.to_numeric(map_df["longitude"], errors="coerce")
        map_df = map_df.dropna(subset=["latitude", "longitude"])

        if len(map_df) > 0:
            st.markdown(
                """
                <div class="small-note">
                    This map displays all records in the updated dataset with valid coordinates.
                    It can be used as a quick visual check before exporting to your larger mapping workflow.
                </div>
                """,
                unsafe_allow_html=True
            )
            st.map(map_df[["latitude", "longitude"]], use_container_width=True)
        else:
            st.info("No mappable cases yet. Add valid coordinates or geocodable addresses to see map output.")
    else:
        st.info("Latitude and longitude columns not found.")

    # -----------------------------------------------------
    # TABS
    # -----------------------------------------------------
    st.markdown("### Review & Data Tables")

    tab1, tab2, tab3 = st.tabs(["All Cases", "Needs Review", "Mappable Cases"])

    with tab1:
        st.dataframe(df, use_container_width=True, height=420)

    with tab2:
        if "review_needed" in df.columns:
            review_df = df[df["review_needed"] == 1].copy()
            st.dataframe(review_df, use_container_width=True, height=420)
        else:
            review_df = pd.DataFrame()
            st.info("No review_needed column found.")

    with tab3:
        if "usable_for_mapping" in df.columns:
            mappable_df = df[df["usable_for_mapping"] == 1].copy()
            st.dataframe(mappable_df, use_container_width=True, height=420)
        else:
            mappable_df = pd.DataFrame()
            st.info("No usable_for_mapping column found.")

    # -----------------------------------------------------
    # DOWNLOADS
    # -----------------------------------------------------
    st.markdown("### Export Files")

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
