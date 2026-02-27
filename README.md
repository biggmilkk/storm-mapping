JTWC KMZ Cleaner
================

What it does
------------
Converts a raw JTWC warning KMZ into a simplified KMZ (similar to your Urmil.kmz format):

- Keeps only the forecast center points
- Builds a single forecast track LineString
- Unions all 34kt wind radii polygons into one "34 knot Danger Swath"
- Copies styles + icons from a *template* KMZ (your Urmil.kmz)

Run as CLI
----------
1) Install dependencies:

    pip install -r requirements.txt

2) Convert:

    python -m jtwc_kmz_cleaner.cli sh2326.kmz cleaned.kmz --template Urmil.kmz

Run as a web app
----------------
    streamlit run app.py

Notes / Customization
---------------------
- Category mapping uses Australian TC categories based on knots:
  <34 = disturbance, 34-47 = Cat1, 48-63 = Cat2, >=64 = Cat3+
- If you want different icons/categories, you can adjust `australian_category()` in converter.py.
