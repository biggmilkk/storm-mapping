import io
import re
import zipfile
import streamlit as st
from lxml import etree
from shapely.geometry import Polygon
from shapely.ops import unary_union

KML_NS = "http://www.opengis.net/kml/2.2"
NSMAP = {None: KML_NS}  # default namespace

def read_kmz_kml_bytes(kmz_bytes: bytes) -> bytes:
    with zipfile.ZipFile(io.BytesIO(kmz_bytes), "r") as z:
        names = z.namelist()
        kml_name = "doc.kml" if "doc.kml" in names else next((n for n in names if n.lower().endswith(".kml")), None)
        if not kml_name:
            raise ValueError("No .kml found inside KMZ.")
        return z.read(kml_name)

def parse_kml(kml_bytes: bytes) -> etree._Element:
    parser = etree.XMLParser(recover=True, huge_tree=True, remove_blank_text=True)
    return etree.fromstring(kml_bytes, parser=parser)

def q(tag: str) -> str:
    return f"{{{KML_NS}}}{tag}"

def txt(el) -> str:
    return (el.text or "").strip() if el is not None and el.text else ""

def is_forecast_folder(name: str) -> bool:
    return "forecast" in name.lower()

def extract_point(pm):
    c = pm.find(".//" + q("Point") + "/" + q("coordinates"))
    if c is None:
        return None
    s = txt(c)
    if not s:
        return None
    lon, lat, *_ = s.split(",")
    return float(lon), float(lat)

def extract_polygon_rings(pm):
    rings = []
    path = ".//" + q("Polygon") + "//" + q("outerBoundaryIs") + "//" + q("LinearRing") + "/" + q("coordinates")
    for coords_el in pm.findall(path):
        s = txt(coords_el)
        if not s:
            continue
        pts = []
        for triplet in s.split():
            lon, lat, *_ = triplet.split(",")
            pts.append((float(lon), float(lat)))
        if len(pts) >= 4:
            rings.append(pts)
    return rings

def unwrap_ring(ring):
    if not ring:
        return ring
    out = [ring[0]]
    prev = ring[0][0]
    for lon, lat in ring[1:]:
        cand = [lon, lon + 360.0, lon - 360.0]
        best = min(cand, key=lambda x: abs(x - prev))
        out.append((best, lat))
        prev = best
    return out

def ring_to_poly(ring):
    r = unwrap_ring(ring)
    if r[0] != r[-1]:
        r = r + [r[0]]
    try:
        poly = Polygon(r)
        if not poly.is_valid or poly.is_empty:
            poly = poly.buffer(0)
        return poly if (poly and not poly.is_empty) else None
    except Exception:
        return None

def build_clean_kml(points, swath_geom):
    # Minimal KML with embedded styles
    kml = etree.Element(q("kml"), nsmap=NSMAP)
    doc = etree.SubElement(kml, q("Document"))
    etree.SubElement(doc, q("name")).text = "Cleaned JTWC Forecast"

    # Styles (simple defaults)
    # Note: KML colors are aabbggrr (alpha, blue, green, red)
    style_point = etree.SubElement(doc, q("Style"), id="ptStyle")
    iconstyle = etree.SubElement(style_point, q("IconStyle"))
    etree.SubElement(iconstyle, q("scale")).text = "1.1"
    icon = etree.SubElement(iconstyle, q("Icon"))
    etree.SubElement(icon, q("href")).text = "http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png"

    style_line = etree.SubElement(doc, q("Style"), id="lineStyle")
    linestyle = etree.SubElement(style_line, q("LineStyle"))
    etree.SubElement(linestyle, q("color")).text = "ff00ffff"  # yellow-ish
    etree.SubElement(linestyle, q("width")).text = "3"

    style_poly = etree.SubElement(doc, q("Style"), id="polyStyle")
    polystyle = etree.SubElement(style_poly, q("PolyStyle"))
    etree.SubElement(polystyle, q("color")).text = "4d0000ff"  # semi-transparent red
    linestyle2 = etree.SubElement(style_poly, q("LineStyle"))
    etree.SubElement(linestyle2, q("color")).text = "ff0000ff"
    etree.SubElement(linestyle2, q("width")).text = "2"

    folder = etree.SubElement(doc, q("Folder"))
    etree.SubElement(folder, q("name")).text = "Forecast"

    # Track
    pm_track = etree.SubElement(folder, q("Placemark"))
    etree.SubElement(pm_track, q("name")).text = "Storm Track"
    etree.SubElement(pm_track, q("styleUrl")).text = "#lineStyle"
    ls = etree.SubElement(pm_track, q("LineString"))
    etree.SubElement(ls, q("tessellate")).text = "1"
    etree.SubElement(ls, q("coordinates")).text = " ".join(f"{lon},{lat},0" for lon, lat, _ in points)

    # Points
    for lon, lat, label in points:
        pm = etree.SubElement(folder, q("Placemark"))
        etree.SubElement(pm, q("name")).text = label
        etree.SubElement(pm, q("styleUrl")).text = "#ptStyle"
        p = etree.SubElement(pm, q("Point"))
        etree.SubElement(p, q("coordinates")).text = f"{lon},{lat},0"

    # Swath
    if swath_geom is not None and (not swath_geom.is_empty):
        pm_sw = etree.SubElement(folder, q("Placemark"))
        etree.SubElement(pm_sw, q("name")).text = "34 knot Danger Swath"
        etree.SubElement(pm_sw, q("styleUrl")).text = "#polyStyle"

        def write_poly(parent, poly):
            poly_el = etree.SubElement(parent, q("Polygon"))
            etree.SubElement(poly_el, q("tessellate")).text = "1"
            ob = etree.SubElement(poly_el, q("outerBoundaryIs"))
            lr = etree.SubElement(ob, q("LinearRing"))
            etree.SubElement(lr, q("coordinates")).text = " ".join(f"{x},{y},0" for x, y in poly.exterior.coords)

        if swath_geom.geom_type == "Polygon":
            write_poly(pm_sw, swath_geom)
        elif swath_geom.geom_type == "MultiPolygon":
            mg = etree.SubElement(pm_sw, q("MultiGeometry"))
            for poly in swath_geom.geoms:
                write_poly(mg, poly)

    return etree.tostring(kml, xml_declaration=True, encoding="UTF-8", pretty_print=False)

def convert_raw_jtwc_kmz_to_clean_kml(raw_kmz: bytes, simplify_tol: float = 0.02) -> bytes:
    raw_kml = read_kmz_kml_bytes(raw_kmz)
    root = parse_kml(raw_kml)

    doc = root.find(".//" + q("Document"))
    if doc is None:
        raise ValueError("No <Document> found in KML.")

    # Find forecast folder
    forecast = None
    for f in doc.findall(".//" + q("Folder")):
        name_el = f.find("./" + q("name"))
        if name_el is not None and is_forecast_folder(txt(name_el)):
            forecast = f
            break
    if forecast is None:
        forecast = doc

    points = []
    polys = []

    for pm in forecast.findall(".//" + q("Placemark")):
        name = txt(pm.find("./" + q("name")))

        pt = extract_point(pm)
        if pt:
            lon, lat = pt
            points.append((lon, lat, name or "Forecast Point"))
            continue

        if re.search(r"\b34\b", name) and re.search(r"\bkt\b|\bknot\b", name, re.I):
            for ring in extract_polygon_rings(pm):
                poly = ring_to_poly(ring)
                if poly is not None:
                    polys.append(poly)

    if not points:
        raise ValueError("No forecast points found. (No Point placemarks detected.)")

    swath = None
    if polys:
        merged = unary_union(polys)
        if simplify_tol and simplify_tol > 0:
            merged = merged.simplify(simplify_tol, preserve_topology=True)
        swath = merged

    return build_clean_kml(points, swath)

# ---------------- Streamlit UI ----------------
st.set_page_config(page_title="JTWC KMZ Cleaner", layout="centered")
st.title("JTWC KMZ Cleaner (Raw KMZ → Clean KML)")
st.write("Upload a raw JTWC KMZ and download a cleaned KML (track + points + merged 34kt swath).")

raw = st.file_uploader("Raw JTWC KMZ", type=["kmz"])
tol = st.slider("Swath simplify tolerance (degrees)", 0.0, 0.1, 0.02, 0.005)

if raw:
    if st.button("Convert"):
        try:
            out_kml = convert_raw_jtwc_kmz_to_clean_kml(raw.getvalue(), simplify_tol=float(tol))
            st.success("Conversion complete.")
            st.download_button(
                "Download cleaned KML",
                data=out_kml,
                file_name="cleaned.kml",
                mime="application/vnd.google-earth.kml+xml",
            )
        except Exception as e:
            st.error(f"Conversion failed: {e}")
else:
    st.info("Upload a raw JTWC KMZ to enable conversion.")
