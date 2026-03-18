import io
import re
import zipfile
import streamlit as st
import xml.etree.ElementTree as ET

from shapely.geometry import Polygon, LineString
from shapely.ops import unary_union


# -------------------------
# KML namespaces
# -------------------------
KML_NS = "http://www.opengis.net/kml/2.2"
GX_NS = "http://www.google.com/kml/ext/2.2"
NS = {"kml": KML_NS, "gx": GX_NS}

ET.register_namespace("", KML_NS)
ET.register_namespace("gx", GX_NS)


# -------------------------
# Utility helpers
# -------------------------
def _text(el: ET.Element | None) -> str:
    return (el.text or "").strip() if el is not None else ""


def _read_kmz(kmz_bytes: bytes) -> tuple[str, dict[str, bytes]]:
    """
    Read a KMZ (zip) and return:
      - KML text from doc.kml (or first .kml found)
      - assets dict for all other entries (icons/images/etc.)
    """
    assets: dict[str, bytes] = {}

    with zipfile.ZipFile(io.BytesIO(kmz_bytes), "r") as z:
        names = z.namelist()

        kml_name = "doc.kml" if "doc.kml" in names else next(
            (n for n in names if n.lower().endswith(".kml")), None
        )
        if not kml_name:
            raise ValueError("No .kml found inside the KMZ.")

        kml_text = z.read(kml_name).decode("utf-8", errors="replace")

        for n in names:
            if n == kml_name:
                continue
            assets[n] = z.read(n)

    return kml_text, assets


def _write_kmz(doc_kml_text: str, assets: dict[str, bytes]) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("doc.kml", doc_kml_text)
        for path, blob in assets.items():
            z.writestr(path, blob)
    return out.getvalue()


def _parse_kml(kml_text: str) -> ET.Element:
    return ET.fromstring(kml_text)


def _find_document(root: ET.Element) -> ET.Element:
    doc = root.find(".//kml:Document", NS)
    if doc is None:
        raise ValueError("No <Document> element found in KML.")
    return doc


def _is_forecast_folder(name: str) -> bool:
    return "forecast" in name.strip().lower()


def _extract_point(pm: ET.Element) -> tuple[float, float] | None:
    coord_el = pm.find(".//kml:Point/kml:coordinates", NS)
    if coord_el is None or not _text(coord_el):
        return None
    lon, lat, *_ = _text(coord_el).split(",")
    return float(lon), float(lat)


def _extract_polygon_rings(pm: ET.Element) -> list[list[tuple[float, float]]]:
    """
    Extract outer rings from any Polygon(s) in the placemark.
    Returns list of rings, each ring is list[(lon, lat)].
    """
    rings: list[list[tuple[float, float]]] = []
    for coords_el in pm.findall(
        ".//kml:Polygon//kml:outerBoundaryIs//kml:LinearRing/kml:coordinates", NS
    ):
        if not _text(coords_el):
            continue
        pts: list[tuple[float, float]] = []
        for triplet in _text(coords_el).split():
            lon, lat, *_ = triplet.split(",")
            pts.append((float(lon), float(lat)))
        if len(pts) >= 4:
            rings.append(pts)
    return rings


def _unwrap_ring_longitudes(ring: list[tuple[float, float]]) -> list[tuple[float, float]]:
    """
    Best-effort unwrap for dateline crossing:
    shift each longitude by +/-360 to minimize jumps.
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


def _ring_to_polygon(ring: list[tuple[float, float]]) -> Polygon | None:
    ring2 = _unwrap_ring_longitudes(ring)
    if ring2[0] != ring2[-1]:
        ring2 = ring2 + [ring2[0]]
    try:
        poly = Polygon(ring2)
        if not poly.is_valid or poly.is_empty:
            poly = poly.buffer(0)
        return poly if (poly and not poly.is_empty) else None
    except Exception:
        return None


def _pick_styles(template_doc: ET.Element) -> tuple[str | None, str | None, str | None]:
    """
    Heuristically select style/styleMap ids for:
      - points (IconStyle)
      - lines (LineStyle without PolyStyle)
      - polygons (PolyStyle)
    Returns styleUrls like "#someId" or None.
    """
    point = line = poly = None
    candidates = template_doc.findall("./kml:StyleMap", NS) + template_doc.findall("./kml:Style", NS)

    def has(el: ET.Element, tag: str) -> bool:
        return el.find(f".//kml:{tag}", NS) is not None

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


# -------------------------
# Core conversion
# -------------------------
def convert_jtwc_kmz(raw_kmz: bytes, template_kmz: bytes, simplify_tol: float = 0.02) -> bytes:
    raw_kml, _ = _read_kmz(raw_kmz)
    tmpl_kml, tmpl_assets = _read_kmz(template_kmz)

    raw_root = _parse_kml(raw_kml)
    tmpl_root = _parse_kml(tmpl_kml)

    raw_doc = _find_document(raw_root)
    tmpl_doc = _find_document(tmpl_root)

    point_style, line_style, poly_style = _pick_styles(tmpl_doc)

    # Find forecast folder
    forecast = None
    for f in raw_doc.findall(".//kml:Folder", NS):
        name = _text(f.find("./kml:name", NS))
        if _is_forecast_folder(name):
            forecast = f
            break
    if forecast is None:
        forecast = raw_doc  # fallback

    # Extract forecast points and 34kt polygons
    points: list[tuple[float, float, str]] = []
    polys: list[Polygon] = []

    for pm in forecast.findall(".//kml:Placemark", NS):
        name = _text(pm.find("./kml:name", NS))

        pt = _extract_point(pm)
        if pt:
            lon, lat = pt
            points.append((lon, lat, name or "Forecast Point"))
            continue

        # Identify 34kt wind radius features by name (JTWC usually includes "34 KT")
        if re.search(r"\b34\b", name) and re.search(r"\bkt\b|\bknot\b", name, re.I):
            for ring in _extract_polygon_rings(pm):
                poly = _ring_to_polygon(ring)
                if poly is not None:
                    polys.append(poly)

    if not points:
        raise ValueError("No forecast Point placemarks found in the raw KMZ.")

    # Build track line
    _ = LineString([(lon, lat) for lon, lat, _label in points])

    # Merge polygons
    swath = None
    if polys:
        merged = unary_union(polys)
        if simplify_tol and simplify_tol > 0:
            merged = merged.simplify(simplify_tol, preserve_topology=True)
        swath = merged

    # Remove non-style children from template doc, then add our cleaned folder
    keep_tags = {
        f"{{{KML_NS}}}name",
        f"{{{KML_NS}}}open",
        f"{{{KML_NS}}}Style",
        f"{{{KML_NS}}}StyleMap",
        f"{{{KML_NS}}}Schema",
        f"{{{KML_NS}}}ExtendedData",
        f"{{{KML_NS}}}Snippet",
        f"{{{KML_NS}}}description",
        f"{{{KML_NS}}}LookAt",
        f"{{{KML_NS}}}Camera",
        f"{{{KML_NS}}}TimeSpan",
        f"{{{KML_NS}}}TimeStamp",
    }

    for ch in list(tmpl_doc):
        if ch.tag not in keep_tags:
            tmpl_doc.remove(ch)

    folder = ET.SubElement(tmpl_doc, f"{{{KML_NS}}}Folder")
    ET.SubElement(folder, f"{{{KML_NS}}}name").text = "Forecast"

    # Track placemark
    pm_track = ET.SubElement(folder, f"{{{KML_NS}}}Placemark")
    ET.SubElement(pm_track, f"{{{KML_NS}}}name").text = "Storm Track"
    if line_style:
        ET.SubElement(pm_track, f"{{{KML_NS}}}styleUrl").text = line_style

    ls = ET.SubElement(pm_track, f"{{{KML_NS}}}LineString")
    ET.SubElement(ls, f"{{{KML_NS}}}tessellate").text = "1"
    ET.SubElement(ls, f"{{{KML_NS}}}coordinates").text = " ".join(
        f"{lon},{lat},0" for lon, lat, _label in points
    )

    # Point placemarks
    for lon, lat, label in points:
        pm = ET.SubElement(folder, f"{{{KML_NS}}}Placemark")
        ET.SubElement(pm, f"{{{KML_NS}}}name").text = label
        if point_style:
            ET.SubElement(pm, f"{{{KML_NS}}}styleUrl").text = point_style

        p = ET.SubElement(pm, f"{{{KML_NS}}}Point")
        ET.SubElement(p, f"{{{KML_NS}}}coordinates").text = f"{lon},{lat},0"

    # Swath placemark
    if swath is not None and (not swath.is_empty):
        pm_sw = ET.SubElement(folder, f"{{{KML_NS}}}Placemark")
        ET.SubElement(pm_sw, f"{{{KML_NS}}}name").text = "34 knot Danger Swath"
        if poly_style:
            ET.SubElement(pm_sw, f"{{{KML_NS}}}styleUrl").text = poly_style

        polys_to_write = []
        if swath.geom_type == "Polygon":
            polys_to_write = [swath]
        elif swath.geom_type == "MultiPolygon":
            polys_to_write = list(swath.geoms)

        if len(polys_to_write) == 1:
            poly_el = ET.SubElement(pm_sw, f"{{{KML_NS}}}Polygon")
            ET.SubElement(poly_el, f"{{{KML_NS}}}tessellate").text = "1"
            ob = ET.SubElement(poly_el, f"{{{KML_NS}}}outerBoundaryIs")
            lr = ET.SubElement(ob, f"{{{KML_NS}}}LinearRing")
            ET.SubElement(lr, f"{{{KML_NS}}}coordinates").text = " ".join(
                f"{x},{y},0" for x, y in polys_to_write[0].exterior.coords
            )
        elif len(polys_to_write) > 1:
            mg = ET.SubElement(pm_sw, f"{{{KML_NS}}}MultiGeometry")
            for poly in polys_to_write:
                poly_el = ET.SubElement(mg, f"{{{KML_NS}}}Polygon")
                ET.SubElement(poly_el, f"{{{KML_NS}}}tessellate").text = "1"
                ob = ET.SubElement(poly_el, f"{{{KML_NS}}}outerBoundaryIs")
                lr = ET.SubElement(ob, f"{{{KML_NS}}}LinearRing")
                ET.SubElement(lr, f"{{{KML_NS}}}coordinates").text = " ".join(
                    f"{x},{y},0" for x, y in poly.exterior.coords
                )

    # Serialize
    out_kml = ET.tostring(tmpl_root, encoding="utf-8", xml_declaration=True).decode(
        "utf-8", errors="replace"
    )

    # Output KMZ (template assets preserved)
    return _write_kmz(out_kml, tmpl_assets)


# -------------------------
# Streamlit UI
# -------------------------
st.set_page_config(page_title="JTWC KMZ Cleaner", layout="centered")
st.title("JTWC KMZ Cleaner")
st.write("Upload a raw JTWC KMZ and a template KMZ (for styling), then download the cleaned KMZ.")

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
