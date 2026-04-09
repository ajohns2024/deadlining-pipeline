import streamlit as st
import tempfile
import os
import pandas as pd
import plotly.express as px
from pipeline import run_pipeline # Ensuring this matches your file name

# ---------------------------------------------------------
# 1. PAGE CONFIG & SYSTEM STYLING
# ---------------------------------------------------------
st.set_page_config(page_title="Deadlining", page_icon="🗂️", layout="wide")

st.markdown("""
<style>
    /* Forensic Lab Aesthetic */
    [data-testid="stAppViewContainer"] {
        background-color: #05070a;
        color: #e2e8f0;
    }
    .hero {
        padding: 2rem;
        border-left: 5px solid #9b1c1c;
        background: rgba(15, 23, 42, 0.9);
        margin-bottom: 2rem;
    }
    h1, h2, h3 { font-family: 'Georgia', serif; color: #ffffff; }
    .mono { font-family: 'monospace'; color: #94a3b8; font-size: 0.8rem; }
    
    /* Card Container */
    .soft-card {
        background: rgba(15, 23, 42, 0.7);
        border: 1px solid rgba(255, 255, 255, 0.08);
        padding: 1.5rem;
        border-radius: 4px;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# 2. HEADER & INPUT
# ---------------------------------------------------------
st.markdown("""
<div class="hero">
    <div class="mono">SPATIAL FORENSIC INTERFACE // SYSTEM_ACTIVE</div>
    <h1>Deadlining</h1>
    <p style="font-family: 'Georgia', serif; font-style: italic;">Researching patterns of disappearance and institutional neglect.</p>
</div>
""", unsafe_allow_html=True)

col_input, col_info = st.columns([1, 1], gap="large")

with col_input:
    st.markdown('<div class="soft-card">', unsafe_allow_html=True)
    uploaded_file = st.file_uploader("Upload Intake CSV", type=["csv"])
    replace_master = st.checkbox("Replace master file", value=False)
    run_clicked = st.button("RUN PIPELINE SEQUENCE")
    st.markdown('</div>', unsafe_allow_html=True)

with col_info:
    st.markdown('<div class="soft-card">', unsafe_allow_html=True)
    st.markdown("### System Protocols")
    st.write("Ensure your CSV matches the standard forensic schema. The pipeline will automatically flag records missing geo-coordinates or case types.")
    st.markdown('</div>', unsafe_allow_html=True)

# ---------------------------------------------------------
# 3. DASHBOARD LOGIC (What you want to see)
# ---------------------------------------------------------
if uploaded_file is not None and run_clicked:
    with st.spinner("Processing Data..."):
        # Temp file handling
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp.write(uploaded_file.getbuffer())
            temp_path = tmp.name
        
        try:
            # Running your original pipeline function
            df = run_pipeline(temp_path, replace_master=replace_master)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    # Calculate original variables
    total_cases = len(df)
    homicide_count = int((df["case_type"] == "Homicide").sum()) if "case_type" in df.columns else 0
    missing_count = int((df["case_type"] == "Missing").sum()) if "case_type" in df.columns else 0
    unidentified_count = int((df["case_type"] == "Unidentified").sum()) if "case_type" in df.columns else 0
    review_count = int((df["review_needed"] == 1).sum()) if "review_needed" in df.columns else 0
    mapped_count = int((df["usable_for_mapping"] == 1).sum()) if "usable_for_mapping" in df.columns else 0

    # DISPLAY METRICS
    st.markdown("---")
    st.subheader("Statistical Summary")
    m = st.columns(6)
    labels = ["Total", "Homicides", "Missing", "Unidentified", "To Review", "Mappable"]
    vals = [total_cases, homicide_count, missing_count, unidentified_count, review_count, mapped_count]
    
    for i, col in enumerate(m):
        col.metric(labels[i], vals[i])

    # DISPLAY CHARTS
    st.markdown("---")
    c1, c2 = st.columns(2)

    with c1:
        st.markdown("**Case Distribution Audit**")
        if "case_type" in df.columns:
            fig = px.bar(df['case_type'].value_counts().reset_index(), 
                         x='count', y='case_type', orientation='h',
                         color_discrete_sequence=['#9b1c1c'])
            fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color="#fff")
            st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown("**Data Integrity Status**")
        if "review_needed" in df.columns:
            fig_pie = px.pie(df, names='review_needed', hole=0.4,
                             color_discrete_sequence=['#475569', '#9b1c1c'])
            fig_pie.update_layout(paper_bgcolor='rgba(0,0,0,0)', font_color="#fff")
            st.plotly_chart(fig_pie, use_container_width=True)

    # DISPLAY MAP
    st.markdown("---")
    st.subheader("Spatial Forensic Mapping")
    if "latitude" in df.columns and "longitude" in df.columns:
        st.map(df.dropna(subset=['latitude', 'longitude'])[['latitude', 'longitude']])

    # DISPLAY DATA TABLES (The "Review" section)
    st.markdown("---")
    st.subheader("Data Review Tables")
    t1, t2 = st.tabs(["Full Dataset", "Flagged for Review"])
    with t1:
        st.dataframe(df, use_container_width=True)
    with t2:
        if "review_needed" in df.columns:
            st.dataframe(df[df["review_needed"] == 1], use_container_width=True)

else:
    # This shows when no data is loaded yet
    st.info("Awaiting CSV upload and pipeline execution...")
