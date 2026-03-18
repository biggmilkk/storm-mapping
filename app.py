import io
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple

import streamlit as st
from lxml import etree
from shapely.geometry import Polygon
from shapely.ops import unary_union
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo

from timezonefinder import TimezoneFinder


# -------------------------
# Constants / Namespaces
# -------------------------
KML_NS = "http://www.opengis.net/kml/2.2"
NSMAP = {None: KML_NS}  # default namespace

TF = TimezoneFinder()


def q(tag: str) -> str:
    return f"{{{KML_NS}}}{tag}"


def txt(el) -> str:
    return (el.text or "").strip() if el is not None and el.text else ""


# -------------------------
# KMZ / KML helpers
# -------------------------
def read_kmz_kml_bytes(kmz_bytes: bytes) -> bytes:
    with zipfile.ZipFile(io.BytesIO(kmz_bytes), "r") as z:
        names = z.namelist()
        kml_name = "doc.kml" if "doc.kml" in names else next(
            (n for n in names if n.lower().endswith(".kml")), None
        )
        if not kml_name:
            raise ValueError("No .kml found inside KMZ.")
        return z.read(kml_name)


def write_kmz(kml_bytes: bytes) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("doc.kml", kml_bytes)
    return out.getvalue()


def parse_kml(kml_bytes: bytes) -> etree._Element:
    parser = etree.XMLParser(recover=True, huge_tree=True, remove_blank_text=True)
    return etree.fromstring(kml_bytes, parser=parser)


# -------------------------
# Extraction helpers
# -------------------------
def is_forecast_folder(name: str) -> bool:
    return "forecast" in (name or "").strip().lower()


def extract_point(pm: etree._Element) -> Optional[Tuple[float, float]]:
    coord_el = pm.find(".//" + q("Point") + "/" + q("coordinates"))
    if coord_el is None:
        return None
    s = txt(coord_el)
    if not s:
        return None
    lon, lat, *_ = s.split(",")
    return float(lon), float(lat)


def extract_polygon_rings(pm: etree._Element) -> List[List[Tuple[float, float]]]:
    rings: List[List[Tuple[float, float]]] = []
    path = (
        ".//" + q("Polygon") + "//" + q("outerBoundaryIs") + "//" +
        q("LinearRing") + "/" + q("coordinates")
    )
    for coords_el in pm.findall(path):
        s = txt(coords_el)
        if not s:
            continue
        pts: List[Tuple[float, float]] = []
        for triplet in s.split():
            lon, lat, *_ = triplet.split(",")
            pts.append((float(lon), float(lat)))
        if len(pts) >= 4:
            rings.append(pts)
    return rings


def unwrap_ring(ring: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
    # Best-effort dateline unwrap to reduce polygon self-intersections.
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


def ring_to_poly(ring: List[Tuple[float, float]]) -> Optional[Polygon]:
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


# -------------------------
# Agency selection (Option 2)
# -------------------------
def in_box(lon360: float, lat: float, lon_min: float, lon_max: float, lat_min: float, lat_max: float) -> bool:
    return (lon_min <= lon360 <= lon_max) and (lat_min <= lat <= lat_max)


def pick_agency(lon: float, lat: float) -> str:
    """
    Option 2:
      - BOM for Australia region: lat -40..0, lon 90E..160E
      - IMD for Indian Ocean-ish: lat -40..30, lon 30E..110E (excluding BOM)
      - JTWC otherwise (Pacific fallback)
    """
    lon360 = lon % 360.0
    if in_box(lon360, lat, 90.0, 160.0, -40.0, 0.0):
        return "BOM"
    if in_box(lon360, lat, 30.0, 110.0, -40.0, 30.0):
        return "IMD"
    return "JTWC"


# -------------------------
# Classification (matrix)
# -------------------------
def classify_wind(knots: int, agency: str) -> str:
    if agency == "JTWC":
        if knots < 34:
            return "Tropical Depression"
        if 34 <= knots <= 63:
            return "Tropical Storm"
        if 64 <= knots <= 129:
            return "Typhoon"
        return "Super Typhoon"

    if agency == "BOM":
        if knots < 34:
            return "Tropical Low"
        if 34 <= knots <= 47:
            return "Tropical Cyclone (1)"
        if 48 <= knots <= 63:
            return "Tropical Cyclone (2)"
        if 64 <= knots <= 85:
            return "Severe Tropical Cyclone (3)"
        if 86 <= knots <= 107:
            return "Severe Tropical Cyclone (4)"
        if 108 <= knots <= 119:
            return "Severe Tropical Cyclone (5)"
        return "Severe Tropical Cyclone (5)"

    # IMD
    if knots < 28:
        return "Depression"
    if 28 <= knots <= 33:
        return "Deep Depression"
    if 34 <= knots <= 47:
        return "Cyclonic Storm"
    if 48 <= knots <= 63:
        return "Severe Cyclonic Storm"
    if 64 <= knots <= 89:
        return "Very Severe Cyclonic Storm"
    if 90 <= knots <= 119:
        return "Extremely Severe Cyclonic Storm"
    return "Super Cyclonic Storm"


# -------------------------
# Time parsing (robust)
# -------------------------
# (A) JTWC DTG style: "240300Z FEB 2026"
DTG_RE = re.compile(r"\b(\d{2})(\d{2})(\d{2})Z\s+([A-Z]{3})\s+(\d{4})\b", re.IGNORECASE)
MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# (B) Anchor format embedded in some point names: "26022400Z" => YYMMDDHHZ
ANCHOR_YYMMDDHH_RE = re.compile(r"\b(\d{8})Z\b")

# (C) Forecast point format: "19/12Z - 115 knots"
FORECAST_NAME_RE = re.compile(
    r"(?P<day>\d{1,2})\s*/\s*(?P<hour>\d{2})Z.*?(?P<knots>\d{1,3})\s*knots?",
    re.IGNORECASE
)


def parse_dtg_anywhere(names: List[str]) -> Optional[datetime]:
    for s in names:
        m = DTG_RE.search(s or "")
        if not m:
            continue
        dd = int(m.group(1))
        hh = int(m.group(2))
        mm = int(m.group(3))
        mon = MONTHS.get(m.group(4).upper(), None)
        yyyy = int(m.group(5))
        if mon is None:
            continue
        try:
            return datetime(yyyy, mon, dd, hh, mm, tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def parse_anchor_yyMMddhh(names: List[str]) -> Optional[datetime]:
    candidates: List[datetime] = []
    for s in names:
        m = ANCHOR_YYMMDDHH_RE.search(s or "")
        if not m:
            continue
        raw = m.group(1)
        yy = int(raw[0:2])
        mon = int(raw[2:4])
        dd = int(raw[4:6])
        hh = int(raw[6:8])
        yyyy = 2000 + yy
        try:
            candidates.append(datetime(yyyy, mon, dd, hh, 0, tzinfo=timezone.utc))
        except ValueError:
            continue
    return min(candidates) if candidates else None


def parse_forecast_day_hour_knots(name: str) -> Optional[Tuple[int, int, int]]:
    m = FORECAST_NAME_RE.search(name or "")
    if not m:
        return None
    return int(m.group("day")), int(m.group("hour")), int(m.group("knots"))


def infer_forecast_datetimes(
    forecast_points_in_order: List[Tuple[str, float, float, int, int, int]],
    reference_utc: Optional[datetime]
) -> List[datetime]:
    """
    Infer full UTC datetimes for each forecast point (month rollover aware).
    """
    if reference_utc is None:
        reference_utc = datetime.now(timezone.utc)

    year = reference_utc.year
    month = reference_utc.month

    out: List[datetime] = []
    prev: Optional[datetime] = None

    for _label, _lon, _lat, day, hour, _knots in forecast_points_in_order:
        cand = None
        y, mth = year, month

        # ensure valid date
        for _ in range(14):
            try:
                cand = datetime(y, mth, day, hour, 0, tzinfo=timezone.utc)
                break
            except ValueError:
                dt_tmp = datetime(y, mth, 1, tzinfo=timezone.utc) + relativedelta(months=+1)
                y, mth = dt_tmp.year, dt_tmp.month

        if cand is None:
            cand = datetime(reference_utc.year, reference_utc.month, 1, tzinfo=timezone.utc)

        # enforce monotonic non-decreasing by rolling month forward if needed
        if prev is not None and cand < prev:
            y, mth = cand.year, cand.month
            for _ in range(14):
                dt_tmp = datetime(y, mth, 1, tzinfo=timezone.utc) + relativedelta(months=+1)
                y, mth = dt_tmp.year, dt_tmp.month
                try:
                    cand2 = datetime(y, mth, day, hour, 0, tzinfo=timezone.utc)
                except ValueError:
                    continue
                if cand2 >= prev:
                    cand = cand2
                    break

        out.append(cand)
        prev = cand
        year, month = cand.year, cand.month

    return out


# -------------------------
# Timezone (abbreviation required)
# -------------------------
def tz_abbrev_for_point(lat: float, lon: float, agency: str, utc_dt: datetime) -> Tuple[ZoneInfo, str]:
    """
    Returns (tzinfo, abbreviation) where abbreviation is required.
    - IMD: force Asia/Kolkata => IST
    - Else: timezonefinder -> IANA timezone -> tzname() abbreviation at that datetime
    Raises ValueError if abbreviation cannot be resolved.
    """
    if agency == "IMD":
        tzinfo = ZoneInfo("Asia/Kolkata")
        abbr = utc_dt.astimezone(tzinfo).tzname()
        if not abbr:
            raise ValueError("Could not resolve timezone abbreviation for IMD/IST.")
        return tzinfo, abbr

    tzname = TF.timezone_at(lat=lat, lng=lon)
    if not tzname:
        raise ValueError("Could not determine timezone for this location (timezonefinder returned None).")

    tzinfo = ZoneInfo(tzname)
    abbr = utc_dt.astimezone(tzinfo).tzname()
    # Some zones can return None/empty in edge cases; enforce abbreviation.
    if not abbr:
        raise ValueError(f"Could not resolve timezone abbreviation for timezone '{tzname}'.")
    return tzinfo, abbr


def knots_to_kph_mph(knots: int) -> Tuple[int, int]:
    kph = knots * 1.852
    mph = knots * 1.15078
    return int(round(kph)), int(round(mph))


def build_point_description(category: str, knots: int, utc_dt: datetime, lat: float, lon: float, agency: str) -> str:
    tzinfo, abbr = tz_abbrev_for_point(lat, lon, agency, utc_dt)
    local_dt = utc_dt.astimezone(tzinfo)

    kph, mph = knots_to_kph_mph(knots)
    time_str = local_dt.strftime("%H:%M")
    month_day = local_dt.strftime("%B %d").replace(" 0", " ")

    # EXACT requested format (no storm name prefix, no HTML formatting)
    return (
        f"{category}: The forecast center of circulation with a maximum sustained wind speed of "
        f"{knots} knots / {kph} kph / {mph} mph as of {time_str} {abbr} {month_day}."
    )


SWATH_DESCRIPTION = "Forecast Impact Zone: The area in which impacts from the tropical system are likely to be felt."


# -------------------------
# Output KML builder
# -------------------------
@dataclass
class ForecastPointOut:
    name: str
    lon: float
    lat: float
    utc_dt: datetime
    knots: int
    description: str


def build_clean_kml(
    doc_title: str,
    points: List[ForecastPointOut],
    swath_geom
) -> bytes:
    kml = etree.Element(q("kml"), nsmap=NSMAP)
    doc = etree.SubElement(kml, q("Document"))
    etree.SubElement(doc, q("name")).text = doc_title

    # Simple embedded styles (no template required)
    # KML colors are aabbggrr (alpha, blue, green, red)
    style_point = etree.SubElement(doc, q("Style"), id="ptStyle")
    iconstyle = etree.SubElement(style_point, q("IconStyle"))
    etree.SubElement(iconstyle, q("scale")).text = "1.1"
    icon = etree.SubElement(iconstyle, q("Icon"))
    etree.SubElement(icon, q("href")).text = "http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png"

    style_line = etree.SubElement(doc, q("Style"), id="lineStyle")
    linestyle = etree.SubElement(style_line, q("LineStyle"))
    etree.SubElement(linestyle, q("color")).text = "ff00ffff"
    etree.SubElement(linestyle, q("width")).text = "3"

    style_poly = etree.SubElement(doc, q("Style"), id="polyStyle")
    polystyle = etree.SubElement(style_poly, q("PolyStyle"))
    etree.SubElement(polystyle, q("color")).text = "4d0000ff"
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
    etree.SubElement(ls, q("coordinates")).text = " ".join(
        f"{p.lon},{p.lat},0" for p in points
    )

    # Points (with single description)
    for p in points:
        pm = etree.SubElement(folder, q("Placemark"))
        etree.SubElement(pm, q("name")).text = p.name
        etree.SubElement(pm, q("styleUrl")).text = "#ptStyle"
        desc = etree.SubElement(pm, q("description"))
        desc.text = etree.CDATA(p.description)  # one box only
        pt = etree.SubElement(pm, q("Point"))
        etree.SubElement(pt, q("coordinates")).text = f"{p.lon},{p.lat},0"

    # Swath (with fixed description)
    if swath_geom is not None and (not swath_geom.is_empty):
        pm_sw = etree.SubElement(folder, q("Placemark"))
        etree.SubElement(pm_sw, q("name")).text = "34 knot Danger Swath"
        etree.SubElement(pm_sw, q("styleUrl")).text = "#polyStyle"
        desc = etree.SubElement(pm_sw, q("description"))
        desc.text = etree.CDATA(SWATH_DESCRIPTION)

        def write_poly(parent, poly):
            poly_el = etree.SubElement(parent, q("Polygon"))
            etree.SubElement(poly_el, q("tessellate")).text = "1"
            ob = etree.SubElement(poly_el, q("outerBoundaryIs"))
            lr = etree.SubElement(ob, q("LinearRing"))
            etree.SubElement(lr, q("coordinates")).text = " ".join(
                f"{x},{y},0" for x, y in poly.exterior.coords
            )

        if swath_geom.geom_type == "Polygon":
            write_poly(pm_sw, swath_geom)
        elif swath_geom.geom_type == "MultiPolygon":
            mg = etree.SubElement(pm_sw, q("MultiGeometry"))
            for poly in swath_geom.geoms:
                write_poly(mg, poly)

    return etree.tostring(kml, xml_declaration=True, encoding="UTF-8", pretty_print=False)


# -------------------------
# Main conversion
# -------------------------
def convert_raw_jtwc_kmz(raw_kmz: bytes, simplify_tol: float = 0.02) -> bytes:
    raw_kml = read_kmz_kml_bytes(raw_kmz)
    root = parse_kml(raw_kml)

    doc = root.find(".//" + q("Document"))
    if doc is None:
        raise ValueError("No <Document> found in KML.")

    # Collect all placemark names for time anchoring
    all_names: List[str] = []
    for pm in doc.findall(".//" + q("Placemark")):
        all_names.append(txt(pm.find("./" + q("name"))))

    reference_utc = parse_dtg_anywhere(all_names) or parse_anchor_yyMMddhh(all_names)

    # Find forecast folder
    forecast = None
    for f in doc.findall(".//" + q("Folder")):
        nm = txt(f.find("./" + q("name")))
        if is_forecast_folder(nm):
            forecast = f
            break
    if forecast is None:
        forecast = doc

    # Extract forecast center points
    raw_forecast_points: List[Tuple[str, float, float, int, int, int]] = []
    for pm in forecast.findall(".//" + q("Placemark")):
        name = txt(pm.find("./" + q("name")))
        pt = extract_point(pm)
        if not pt:
            continue
        parsed = parse_forecast_day_hour_knots(name)
        if not parsed:
            continue
        day, hour, knots = parsed
        lon, lat = pt
        raw_forecast_points.append((name, lon, lat, day, hour, knots))

    if not raw_forecast_points:
        raise ValueError("No forecast center points found. Expected 'DD/HHZ - N knots' labels.")

    # Infer full UTC datetimes for each forecast point (month rollover)
    inferred_utcs = infer_forecast_datetimes(raw_forecast_points, reference_utc)

    # Build output points (description only)
    points_out: List[ForecastPointOut] = []
    for (name, lon, lat, _day, _hour, knots), utc_dt in zip(raw_forecast_points, inferred_utcs):
        agency = pick_agency(lon, lat)
        category = classify_wind(knots, agency)
        description = build_point_description(category, knots, utc_dt, lat, lon, agency)
        points_out.append(
            ForecastPointOut(
                name=name,
                lon=lon,
                lat=lat,
                utc_dt=utc_dt,
                knots=knots,
                description=description,
            )
        )

    # Merge 34kt polygons
    polys: List[Polygon] = []
    for pm in forecast.findall(".//" + q("Placemark")):
        name = txt(pm.find("./" + q("name")))
        if re.search(r"\b34\b", name) and re.search(r"\bkt\b|\bknot\b", name, re.I):
            for ring in extract_polygon_rings(pm):
                poly = ring_to_poly(ring)
                if poly is not None:
                    polys.append(poly)

    swath = None
    if polys:
        merged = unary_union(polys)
        if simplify_tol and simplify_tol > 0:
            merged = merged.simplify(simplify_tol, preserve_topology=True)
        swath = merged

    # Document title can be generic (you didn't ask for storm name here)
    doc_title = "Cleaned Forecast"

    return build_clean_kml(doc_title, points_out, swath)


# -------------------------
# Streamlit UI
# -------------------------
st.set_page_config(page_title="JTWC KMZ Cleaner", layout="centered")
st.title("JTWC KMZ Cleaner (Raw KMZ → Cleaned KML/KMZ)")
st.write(
    "Upload a raw JTWC KMZ. The app will infer full dates from internal anchors, classify using IMD/BOM/JTWC rules, "
    "use real timezone abbreviations (AEST/AEDT/etc.), and write a single description line per point."
)

raw = st.file_uploader("Raw JTWC KMZ", type=["kmz"])
tol = st.slider("Swath simplify tolerance", 0.0, 0.1, 0.02, 0.005)
output_as_kmz = st.toggle("Download as KMZ (instead of KML)", value=False)

if raw:
    if st.button("Convert"):
        try:
            out_kml = convert_raw_jtwc_kmz(raw.getvalue(), simplify_tol=float(tol))
            if output_as_kmz:
                out_bytes = write_kmz(out_kml)
                st.download_button(
                    "Download cleaned KMZ",
                    data=out_bytes,
                    file_name="cleaned.kmz",
                    mime="application/vnd.google-earth.kmz",
                )
            else:
                st.download_button(
                    "Download cleaned KML",
                    data=out_kml,
                    file_name="cleaned.kml",
                    mime="application/vnd.google-earth.kml+xml",
                )
            st.success("Conversion complete.")
        except Exception as e:
            st.error(f"Conversion failed: {e}")
else:
    st.info("Upload a raw JTWC KMZ to enable conversion.")
