import io
import re
import zipfile
import streamlit as st
from lxml import etree
from shapely.geometry import Polygon, LineString
from shapely.ops import unary_union

KML_NS = "http://www.opengis.net/kml/2.2"
GX_NS = "http://www.google.com/kml/ext/2.2"
NS = {"kml": KML_NS, "gx": GX_NS}


# -------------------------
# KMZ helpers
# -------------------------
def read_kmz(kmz_bytes: bytes) -> tuple[bytes, dict[str, bytes]]:
    """
    Read KMZ and return:
      - KML bytes from doc.kml (or first .kml found)
      - assets dict for all other entries
    """
    assets: dict[str, bytes] = {}
    with zipfile.ZipFile(io.BytesIO(kmz_bytes), "r") as z:
        names = z.namelist()
        kml_name = "doc.kml" if "doc.kml" in names else next((n for n in names if n.lower().endswith(".kml")), None)
        if not kml_name:
            raise ValueError("No .kml found inside KMZ.")

        kml_bytes = z.read(kml_name)

        for n in names:
            if n == kml_name:
                continue
            assets[n] = z.read(n)

    return kml_bytes, assets


def write_kmz(kml_bytes: bytes, assets: dict[str, bytes]) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("doc.kml", kml_bytes)
        for path, blob in assets.items():
            z.writestr(path, blob)
    return out.getvalue()


# -------------------------
# KML helpers (lxml)
# -------------------------
def parse_kml(kml_bytes: bytes) -> etree._Element:
    parser = etree.XMLParser(recover=True, huge_tree=True, remove_blank_text=True)
    return etree.fromstring(kml_bytes, parser=parser)


def find_document(root: etree._Element) -> etree._Element:
    doc = root.find(".//{%s}Document" % KML_NS)
    if doc is None:
        raise ValueError("No <Document> element found in KML.")
    return doc


def get_text(el) -> str:
    if el is None or el.text is None:
        return ""
    return el.text.strip()


def is_forecast_folder(name: str) -> bool:
    return "forecast" in name.strip().lower()


def pick_styles(template_doc: etree._Element) -> tuple[str | None, str | None, str | None]:
    """
    Heuristic: pick the first StyleMap/Style that contains:
      - IconStyle => point
      - LineStyle without PolyStyle => line
      - PolyStyle => polygon
    Returns styleUrl strings like "#id" or None.
    """
    point = line = poly = None

    # Prefer StyleMap (often referenced), then Style
    candidates = list(template_doc.findall("./{%s}StyleMap" % KML_NS)) + list(template_doc.findall("./{%s}Style" % KML_NS))

    def has(el: etree._Element, tag: str) -> bool:
        return el.find(".//{%s}%s" % (KML_NS, tag)) is not None

    for el in candidates:
        sid = el.get("id")
        if not sid:
            continue
        url = f"#{sid}"

        if point is None and has(el, "IconStyle"):
            point = url
        if line is None and has(el, "LineStyle") and not has(el, "PolyStyle"):
            line = url
        if poly is None and has(el, "PolyStyle"):
            poly = url

        if point and line and poly:
            break

    return point, line, poly


def extract_point(pm: etree._Element) -> tuple[float, float] | None:
    coord_el = pm.find(".//{%s}Point/{%s}coordinates" % (KML_NS, KML_NS))
    if coord_el is None:
        return None
    s = get_text(coord_el)
    if not s:
        return None
    lon, lat, *_ = s.split(",")
    return float(lon), float(lat)


def extract_polygon_rings(pm: etree._Element) -> list[list[tuple[float, float]]]:
    """
    Extract outer rings from polygons under this placemark.
    """
    rings: list[list[tuple[float, float]]] = []
    path = ".//{%s}Polygon//{%s}outerBoundaryIs//{%s}LinearRing/{%s}coordinates" % (KML_NS, KML_NS, KML_NS, KML_NS)
    for coords_el in pm.findall(path):
        s = get_text(coords_el)
        if not s:
            continue
        pts: list[tuple[float, float]] = []
        for triplet in s.split():
            lon, lat, *_ = triplet.split(",")
            pts.append((float(lon), float(lat)))
        if len(pts) >= 4:
            rings.append(pts)
    return rings


def unwrap_ring_longitudes(ring: list[tuple[float, float]]) -> list[tuple[float, float]]:
    """
    Best-effort dateline unwrap:
    choose lon, lon+360, lon-360 to minimize jumps.
    """
    if not ring:
        return ring
    out = [ring[0]]
    prev_lon = ring[0][0]
    for lon, lat in ring[1:]:
        candidates = [lon, lon + 360.0, lon - 360.0]
        best = min(candidates, key=lambda x: abs(x - prev_lon))
        out.append((best, lat))
        prev_lon = best
    return out


def ring_to_polygon(ring: list[tuple[float, float]]) -> Polygon | None:
    r = unwrap_ring_longitudes(ring)
    if r[0] != r[-1]:
        r = r + [r[0]]
    try:
        poly = Polygon(r)
        if not poly.is_valid or poly.is_empty:
            poly = poly.buffer(0)
        return poly if (poly and not poly.is_empty) else None
    except Exception:
        return None


def remove_non_style_children(doc: etree._Element) -> None:
    """
    Keep Style/StyleMap and a small set of metadata nodes; remove everything else.
    """
    keep_localnames = {
        "name",
        "open",
        "Style",
        "StyleMap",
        "Schema",
        "ExtendedData",
        "Snippet",
        "description",
        "LookAt",
        "Camera",
        "TimeSpan",
        "TimeStamp",
    }
    for child in list(doc):
        # lxml tag includes namespace, use localname
        local = etree.QName(child).localname
        if local not in keep_localnames:
            doc.remove(child)


def kml_el(tag: str) -> etree._Element:
    return etree.Element("{%s}%s" % (KML_NS, tag))


def kml_sub(parent: etree._Element, tag: str, text: str | None = None) -> etree._Element:
    el = etree.SubElement(parent, "{%s}%s" % (KML_NS, tag))
    if text is not None:
        el.text = text
    return el


# -------------------------
# Core conversion
# -------------------------
def convert_jtwc_kmz(raw_kmz: bytes, template_kmz: bytes, simplify_tol: float = 0.02) -> bytes:
    raw_kml_bytes, _ = read_kmz(raw_kmz)
    tmpl_kml_bytes, tmpl_assets = read_kmz(template_kmz)

    raw_root = parse_kml(raw_kml_bytes)
    tmpl_root = parse_kml(tmpl_kml_bytes)

    raw_doc = find_document(raw_root)
    tmpl_doc = find_document(tmpl_root)

    point_style, line_style, poly_style = pick_styles(tmpl_doc)

    # Find forecast folder in raw
    forecast = None
    for f in raw_doc.findall(".//{%s}Folder" % KML_NS):
        name_el = f.find("./{%s}name" % KML_NS)
        if is_forecast_folder(get_text(name_el)):
            forecast = f
            break
    if forecast is None:
        forecast = raw_doc  # fallback

    points: list[tuple[float, float, str]] = []
    polys: list[Polygon] = []

    for pm in forecast.findall(".//{%s}Placemark" % KML_NS):
        name = get_text(pm.find("./{%s}name" % KML_NS))

        pt = extract_point(pm)
        if pt:
            lon, lat = pt
            points.append((lon, lat, name or "Forecast Point"))
            continue

        # 34kt polygons (name usually contains "34 KT")
        if re.search(r"\b34\b", name) and re.search(r"\bkt\b|\bknot\b", name, re.I):
            for ring in extract_polygon_rings(pm):
                poly = ring_to_polygon(ring)
                if poly is not None:
                    polys.append(poly)

    if not points:
        raise ValueError("No forecast Point placemarks found in the raw KMZ.")

    # Track (LineString)
    _ = LineString([(lon, lat) for lon, lat, _label in points])

    # Merge swath polygons
    swath = None
    if polys:
        merged = unary_union(polys)
        if simplify_tol and simplify_tol > 0:
            merged = merged.simplify(simplify_tol, preserve_topology=True)
        swath = merged

    # Clean template doc, then inject our output folder
    remove_non_style_children(tmpl_doc)

    folder = kml_sub(tmpl_doc, "Folder")
    kml_sub(folder, "name", "Forecast")

    # Track placemark
    pm_track = kml_sub(folder, "Placemark")
    kml_sub(pm_track, "name", "Storm Track")
    if line_style:
        kml_sub(pm_track, "styleUrl", line_style)

    ls = kml_sub(pm_track, "LineString")
    kml_sub(ls, "tessellate", "1")
    kml_sub(ls, "coordinates", " ".join(f"{lon},{lat},0" for lon, lat, _label in points))

    # Points placemarks
    for lon, lat, label in points:
        pm = kml_sub(folder, "Placemark")
        kml_sub(pm, "name", label)
        if point_style:
            kml_sub(pm, "styleUrl", point_style)
        p = kml_sub(pm, "Point")
        kml_sub(p, "coordinates", f"{lon},{lat},0")

    # Swath placemark
    if swath is not None and (not swath.is_empty):
        pm_sw = kml_sub(folder, "Placemark")
        kml_sub(pm_sw, "name", "34 knot Danger Swath")
        if poly_style:
            kml_sub(pm_sw, "styleUrl", poly_style)

        polys_to_write = []
        if swath.geom_type == "Polygon":
            polys_to_write = [swath]
        elif swath.geom_type == "MultiPolygon":
            polys_to_write = list(swath.geoms)

        if len(polys_to_write) == 1:
            poly_el = kml_sub(pm_sw, "Polygon")
            kml_sub(poly_el, "tessellate", "1")
            ob = kml_sub(poly_el, "outerBoundaryIs")
            lr = kml_sub(ob, "LinearRing")
            kml_sub(lr, "coordinates", " ".join(f"{x},{y},0" for x, y in polys_to_write[0].exterior.coords))
        elif len(polys_to_write) > 1:
            mg = kml_sub(pm_sw, "MultiGeometry")
            for poly in polys_to_write:
                poly_el = kml_sub(mg, "Polygon")
                kml_sub(poly_el, "tessellate", "1")
                ob = kml_sub(poly_el, "outerBoundaryIs")
                lr = kml_sub(ob, "LinearRing")
                kml_sub(lr, "coordinates", " ".join(f"{x},{y},0" for x, y in poly.exterior.coords))

    # Serialize (pretty_print False to reduce diffs)
    out_kml_bytes = etree.tostring(tmpl_root, xml_declaration=True, encoding="UTF-8", pretty_print=False)

    # Output KMZ with template assets
    return write_kmz(out_kml_bytes, tmpl_assets)


# -------------------------
# Streamlit UI
# -------------------------
st.set_page_config(page_title="JTWC KMZ Cleaner", layout="centered")
st.title("JTWC KMZ Cleaner")
st.write("Upload a raw JTWC KMZ and a template KMZ (for styling/icons), then download the cleaned KMZ.")

template = st.file_uploader("Template KMZ (styling) — e.g., Urmil.kmz", type=["kmz"])
raw = st.file_uploader("Raw JTWC KMZ — e.g., sh2326.kmz", type=["kmz"])

tol = st.slider("Swath simplify tolerance (degrees)", 0.0, 0.1, 0.02, 0.005)

if template and raw:
    if st.button("Convert"):
        try:
            out_bytes = convert_jtwc_kmz(
                raw_kmz=raw.getvalue(),
                template_kmz=template.getvalue(),
                simplify_tol=float(tol),
            )
            st.success("Conversion complete.")
            st.download_button(
                "Download cleaned KMZ",
                data=out_bytes,
                file_name="cleaned.kmz",
                mime="application/vnd.google-earth.kmz",
            )
        except Exception as e:
            st.error(f"Conversion failed: {e}")
else:
    st.info("Upload both a Template KMZ and a Raw JTWC KMZ to enable conversion.")
