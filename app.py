import streamlit as st
import tempfile
import os

from pipeline import run_pipeline

st.set_page_config(page_title="Deadlining Pipeline", layout="wide")

st.title("Deadlining Case Intake Pipeline")

uploaded_file = st.file_uploader("Upload intake CSV", type=["csv"])
replace_master = st.checkbox("Replace master file", value=False)

if uploaded_file is not None:
    st.success(f"Uploaded: {uploaded_file.name}")

    if st.button("Run Pipeline"):
        with st.spinner("Running pipeline..."):
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                tmp.write(uploaded_file.getbuffer())
                temp_path = tmp.name

            try:
                df = run_pipeline(temp_path, replace_master=replace_master)

                st.success("Pipeline complete")

                st.subheader("Preview")
                st.dataframe(df.head(20), use_container_width=True)

                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download updated CSV",
                    csv,
                    "cases_master_cleaned_FINAL_UPDATED.csv",
                    "text/csv"
                )

            finally:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
