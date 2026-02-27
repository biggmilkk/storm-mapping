# app.py (Streamlit)
import os
import tempfile
import streamlit as st
from jtwc_kmz_cleaner.converter import convert_jtwc_kmz

st.set_page_config(page_title="JTWC KMZ Cleaner", layout="centered")
st.title("JTWC KMZ Cleaner")
st.write("Upload a raw JTWC warning KMZ and get back a cleaned KMZ (points + track + 34kt swath).")

template = st.file_uploader("Template KMZ (styling) — e.g., Urmil.kmz", type=["kmz"])
src = st.file_uploader("Raw JTWC KMZ — e.g., sh2326.kmz", type=["kmz"])

simplify = st.slider("Swath simplify tolerance (degrees)", min_value=0.0, max_value=0.1, value=0.02, step=0.005)

if template and src:
    if st.button("Convert"):
        with tempfile.TemporaryDirectory() as td:
            template_path = os.path.join(td, "template.kmz")
            src_path = os.path.join(td, "input.kmz")
            out_path = os.path.join(td, "cleaned.kmz")

            with open(template_path, "wb") as f:
                f.write(template.getbuffer())
            with open(src_path, "wb") as f:
                f.write(src.getbuffer())

            convert_jtwc_kmz(src_path, out_path, template_path, simplify_tolerance=simplify)

            with open(out_path, "rb") as f:
                st.download_button(
                    "Download cleaned KMZ",
                    data=f,
                    file_name="cleaned.kmz",
                    mime="application/vnd.google-earth.kmz",
                )
