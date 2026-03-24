import io
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List, Tuple, Dict

import streamlit as st
from lxml import etree
from dateutil.relativedelta import relativedelta
from dateutil import parser as dtparser
from zoneinfo import ZoneInfo
from timezonefinder import TimezoneFinder


# =========================
# App branding
# =========================
APP_NAME = "StormTrack Mapper"
APP_DESC = "Convert JTWC or NHC storm KMZ files into clean, analyst-ready KML for alert mapping."

# Fixed descriptions (JTWC & NHC outputs)
TRACK_DESCRIPTION = "Forecast Track: The forecast track of the system's center of circulation."
IMPACT_DESCRIPTION = "Forecast Impact Zone: The area in which impacts from the tropical system are likely to be felt."


# =========================
# Constants / Namespaces
# =========================
KML_NS_22 = "http://www.opengis.net/kml/2.2"
NSMAP_22 = {None: KML_NS_22}
TF = TimezoneFinder()

MONTH_DOT = {
    1: "Jan.", 2: "Feb.", 3: "Mar.", 4: "Apr.", 5: "May.", 6: "Jun.",
    7: "Jul.", 8: "Aug.", 9: "Sep.", 10: "Oct.", 11: "Nov.", 12: "Dec."
}


# =========================
# Shared helpers
# =========================
def q(ns: str, tag: str) -> str:
    return f"{{{ns}}}{tag}"


def txt(el) -> str:
    return (el.text or "").strip() if el is not None and el.text else ""


def normalize_lon_180(lon: float) -> float:
    return ((lon + 180.0) % 360.0) - 180.0


def norm_name(s: str) -> str:
    return (s or "").strip().lower()


def knots_to_kph_mph(knots: int) -> Tuple[int, int]:
    return int(round(knots * 1.852)), int(round(knots * 1.15078))


def format_month_day(dt_obj: datetime) -> str:
    return dt_obj.strftime("%B %d").replace(" 0", " ")


def format_month_day_dot(dt_obj: datetime) -> str:
    return f"{MONTH_DOT.get(dt_obj.month, dt_obj.strftime('%b.'))} {dt_obj.day}"


def safe_filename(stem: str, fallback: str = "output") -> str:
    stem = re.sub(r"[^A-Za-z0-9._ -]+", "", stem or "").strip()
    stem = re.sub(r"\s+", " ", stem)[:140]
    return stem if stem else fallback


# =========================
# Timezone (nearest abbreviation by location)
# =========================
INDIAN_OCEAN_FALLBACK_ZONES = [
    "Indian/Antananarivo",
    "Indian/Reunion",
    "Indian/Mauritius",
    "Indian/Mayotte",
    "Indian/Mahe",
    "Asia/Dubai",
    "Africa/Nairobi",
]
AU_FALLBACK_ZONES = [
    "Australia/Brisbane",
    "Australia/Sydney",
    "Australia/Melbourne",
    "Australia/Hobart",
    "Australia/Adelaide",
    "Australia/Darwin",
    "Australia/Perth",
]
PACIFIC_FALLBACK_ZONES = [
    "Asia/Manila",
    "Asia/Tokyo",
    "Asia/Seoul",
    "Asia/Taipei",
    "Asia/Shanghai",
    "Pacific/Guam",
    "Pacific/Port_Moresby",
    "Pacific/Noumea",
    "Pacific/Fiji",
    "Pacific/Honolulu",
]
AMERICAS_FALLBACK_ZONES = [
    "America/Puerto_Rico",
    "America/New_York",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
    "America/Halifax",
    "America/Toronto",
    "America/Jamaica",
]


def is_bad_abbrev(abbr: Optional[str]) -> bool:
    if not abbr:
        return True
    a = abbr.strip()
    if re.fullmatch(r"[+-]\d{1,2}(:\d{2})?", a):
        return True
    if a.upper().startswith(("UTC", "GMT")) and re.search(r"[+-]\d", a):
        return True
    return False


def tzinfo_and_abbr_from_location(lat: float, lon: float, dt_local_naive: datetime, fallback_group: str) -> Tuple[ZoneInfo, str]:
    lon_norm = normalize_lon_180(lon)

    tzname = TF.timezone_at(lat=lat, lng=lon_norm)
    if tzname:
        try:
            tzi = ZoneInfo(tzname)
            local_dt = dt_local_naive.replace(tzinfo=tzi)
            abbr = local_dt.tzname()
            if not is_bad_abbrev(abbr):
                return tzi, abbr
        except Exception:
            pass

    if fallback_group == "BOM":
        candidates = AU_FALLBACK_ZONES
    elif fallback_group == "IMD":
        candidates = INDIAN_OCEAN_FALLBACK_ZONES
    elif fallback_group == "NHC":
        candidates = AMERICAS_FALLBACK_ZONES
    else:
        candidates = PACIFIC_FALLBACK_ZONES

    approx_off = round(lon_norm / 15.0)
    best = None
    for z in candidates:
        try:
            tzi = ZoneInfo(z)
            local_dt = dt_local_naive.replace(tzinfo=tzi)
            abbr = local_dt.tzname()
            if is_bad_abbrev(abbr):
                continue
            off_hours = (local_dt.utcoffset().total_seconds() / 3600.0) if local_dt.utcoffset() else 0.0
            score = abs(off_hours - approx_off)
            cand = (score, tzi, abbr)
            if best is None or cand[0] < best[0]:
                best = cand
        except Exception:
            continue

    if best is not None:
        _, tzi, abbr = best
        return tzi, abbr

    return ZoneInfo("UTC"), "UTC"


# =========================
# KMZ / KML helpers
# =========================
def read_kmz_kml_bytes(kmz_bytes: bytes) -> bytes:
    with zipfile.ZipFile(io.BytesIO(kmz_bytes), "r") as z:
        names = z.namelist()
        kml_name = "doc.kml" if "doc.kml" in names else next((n for n in names if n.lower().endswith(".kml")), None)
        if not kml_name:
            raise ValueError("No .kml found inside KMZ.")
        return z.read(kml_name)


def parse_xml_bytes(xml_bytes: bytes) -> etree._Element:
    parser = etree.XMLParser(recover=True, huge_tree=True, remove_blank_text=True)
    return etree.fromstring(xml_bytes, parser=parser)


def load_kmz_root(kmz_bytes: bytes) -> Tuple[etree._Element, str]:
    kml_bytes = read_kmz_kml_bytes(kmz_bytes)
    root = parse_xml_bytes(kml_bytes)
    ns = root.nsmap.get(None, KML_NS_22)
    return root, ns


def get_doc(root: etree._Element, ns: str, label: str) -> etree._Element:
    doc = root.find(".//" + q(ns, "Document"))
    if doc is None:
        raise ValueError(f"{label}: No <Document> found in KML.")
    return doc


# ======================================================================================
# CATEGORY LABELS (latest table)
# ======================================================================================
def classify_wind_table(knots: int, agency: str) -> str:
    # JTWC
    if agency == "JTWC":
        if knots < 34:
            return "Tropical Depression"
        if 34 <= knots <= 63:
            return "Tropical Storm"
        if 64 <= knots <= 129:
            return "Typhoon"
        return "Super Typhoon"  # >=130

    # IMD
    if agency == "IMD":
        if knots < 33:
            return "Depression"
        if knots == 33:
            return "Deep Depression"
        if 34 <= knots <= 54:
            return "Cyclonic Storm"
        if 55 <= knots <= 63:
            return "Severe Cyclonic Storm"
        if 64 <= knots <= 95:
            return "Very Severe Cyclonic Storm"
        if 96 <= knots <= 129:
            return "Extremely Severe Cyclonic Storm"
        return "Super Cyclonic Storm"  # >=130

    # BOM/FMS
    if knots < 33:
        return "Tropical Disturbance"
    if knots == 33:
        return "Tropical Depression"
    if 34 <= knots <= 37:
        return "Tropical Low"
    if 38 <= knots <= 54:
        return "Category 1 Tropical Cyclone"
    if 55 <= knots <= 63:
        return "Category 2 Tropical Cyclone"
    if 64 <= knots <= 95:
        return "Category 3 Severe Tropical Cyclone"
    if 96 <= knots <= 112:
        return "Category 4 Severe Tropical Cyclone"
    return "Category 5 Severe Tropical Cyclone"  # >=113


def classify_wind_nhc(knots: int) -> str:
    # NHC / Saffir–Simpson in knots (1-min)
    if knots < 34:
        return "Tropical Depression"
    if 34 <= knots <= 63:
        return "Tropical Storm"
    if 64 <= knots <= 82:
        return "Category 1 Hurricane"
    if 83 <= knots <= 95:
        return "Category 2 Hurricane"
    if 96 <= knots <= 112:
        return "Category 3 Hurricane"
    if 113 <= knots <= 136:
        return "Category 4 Hurricane"
    return "Category 5 Hurricane"  # >=137


# ======================================================================================
# JTWC CONVERTER (current)
# ======================================================================================
def jtwc_is_forecast_folder(name: str) -> bool:
    return "forecast" in (name or "").strip().lower()


def jtwc_extract_point(pm: etree._Element, ns: str) -> Optional[Tuple[float, float]]:
    coord_el = pm.find(".//" + q(ns, "Point") + "/" + q(ns, "coordinates"))
    if coord_el is None:
        return None
    s = txt(coord_el)
    if not s:
        return None
    lon, lat, *_ = s.split(",")
    return float(lon), float(lat)


def jtwc_extract_danger_swath_geometry(forecast_folder: etree._Element, ns: str) -> Optional[etree._Element]:
    for pm in forecast_folder.findall(".//" + q(ns, "Placemark")):
        name = norm_name(txt(pm.find("./" + q(ns, "name"))))
        if name == "34 knot danger swath":
            mg = pm.find(".//" + q(ns, "MultiGeometry"))
            if mg is not None:
                return etree.fromstring(etree.tostring(mg))
            poly = pm.find(".//" + q(ns, "Polygon"))
            if poly is not None:
                return etree.fromstring(etree.tostring(poly))
            return None
    return None


def jtwc_pick_agency_option2(lon: float, lat: float) -> str:
    lon360 = lon % 360.0

    def in_box(lon360_, lat_, lon_min, lon_max, lat_min, lat_max):
        return lon_min <= lon360_ <= lon_max and lat_min <= lat_ <= lat_max

    if in_box(lon360, lat, 90.0, 160.0, -40.0, 0.0):
        return "BOM"
    if in_box(lon360, lat, 30.0, 110.0, -40.0, 30.0):
        return "IMD"
    return "JTWC"


DTG_RE = re.compile(r"\b(\d{2})(\d{2})(\d{2})Z\s+([A-Z]{3})\s+(\d{4})\b", re.IGNORECASE)
MONTHS = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
ANCHOR_YYMMDDHH_RE = re.compile(r"\b(\d{8})Z\b")
FORECAST_NAME_RE_JTWC = re.compile(r"(?P<day>\d{1,2})\s*/\s*(?P<hour>\d{2})Z.*?(?P<knots>\d{1,3})\s*knots?", re.IGNORECASE)

WARNING_RE_JTWC = re.compile(
    r"\bTROPICAL\s+(?:CYCLONE|STORM|DEPRESSION)\s+(\d{1,2}[A-Z])\s+\(([^)]+)\).*?\bWARNING\b",
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


def parse_forecast_day_hour_knots_jtwc(name: str) -> Optional[Tuple[int, int, int]]:
    m = FORECAST_NAME_RE_JTWC.search(name or "")
    if not m:
        return None
    return int(m.group("day")), int(m.group("hour")), int(m.group("knots"))


def infer_forecast_datetimes_jtwc(
    forecast_points_in_order: List[Tuple[str, float, float, int, int, int]],
    reference_utc: Optional[datetime]
) -> List[datetime]:
    if reference_utc is None:
        reference_utc = datetime.now(timezone.utc)

    year = reference_utc.year
    month = reference_utc.month
    out: List[datetime] = []
    prev: Optional[datetime] = None

    for _label, _lon, _lat, day, hour, _knots in forecast_points_in_order:
        cand = None
        y, mth = year, month

        for _ in range(14):
            try:
                cand = datetime(y, mth, day, hour, 0, tzinfo=timezone.utc)
                break
            except ValueError:
                dt_tmp = datetime(y, mth, 1, tzinfo=timezone.utc) + relativedelta(months=+1)
                y, mth = dt_tmp.year, dt_tmp.month

        if cand is None:
            cand = datetime(reference_utc.year, reference_utc.month, 1, tzinfo=timezone.utc)

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


def parse_jtwc_storm_id_name(names: List[str]) -> Tuple[Optional[str], Optional[str]]:
    for s in names:
        m = WARNING_RE_JTWC.search(s or "")
        if m:
            return m.group(1).upper().strip(), m.group(2).upper().strip()
    return None, None


@dataclass
class OutPoint:
    name: str
    lon: float
    lat: float
    description: str


def build_clean_kml_simple(
    doc_title: str,
    points: List[OutPoint],
    impact_geom: Optional[etree._Element],
    extra_lines: Optional[List[Tuple[str, str, List[Tuple[float, float]]]]] = None,
) -> bytes:
    extra_lines = extra_lines or []

    kml = etree.Element(q(KML_NS_22, "kml"), nsmap=NSMAP_22)
    doc = etree.SubElement(kml, q(KML_NS_22, "Document"))
    etree.SubElement(doc, q(KML_NS_22, "name")).text = doc_title

    style_point = etree.SubElement(doc, q(KML_NS_22, "Style"), id="ptStyle")
    iconstyle = etree.SubElement(style_point, q(KML_NS_22, "IconStyle"))
    etree.SubElement(iconstyle, q(KML_NS_22, "scale")).text = "1.1"
    icon = etree.SubElement(iconstyle, q(KML_NS_22, "Icon"))
    etree.SubElement(icon, q(KML_NS_22, "href")).text = "http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png"

    style_line = etree.SubElement(doc, q(KML_NS_22, "Style"), id="lineStyle")
    linestyle = etree.SubElement(style_line, q(KML_NS_22, "LineStyle"))
    etree.SubElement(linestyle, q(KML_NS_22, "color")).text = "ff00ffff"
    etree.SubElement(linestyle, q(KML_NS_22, "width")).text = "3"

    style_poly = etree.SubElement(doc, q(KML_NS_22, "Style"), id="polyStyle")
    polystyle = etree.SubElement(style_poly, q(KML_NS_22, "PolyStyle"))
    etree.SubElement(polystyle, q(KML_NS_22, "color")).text = "4d0000ff"
    linestyle2 = etree.SubElement(style_poly, q(KML_NS_22, "LineStyle"))
    etree.SubElement(linestyle2, q(KML_NS_22, "color")).text = "ff0000ff"
    etree.SubElement(linestyle2, q(KML_NS_22, "width")).text = "2"

    folder = etree.SubElement(doc, q(KML_NS_22, "Folder"))
    etree.SubElement(folder, q(KML_NS_22, "name")).text = "Forecast"

    # Storm Track
    pm_track = etree.SubElement(folder, q(KML_NS_22, "Placemark"))
    etree.SubElement(pm_track, q(KML_NS_22, "name")).text = "Storm Track"
    etree.SubElement(pm_track, q(KML_NS_22, "styleUrl")).text = "#lineStyle"
    d = etree.SubElement(pm_track, q(KML_NS_22, "description"))
    d.text = etree.CDATA(TRACK_DESCRIPTION)
    ls = etree.SubElement(pm_track, q(KML_NS_22, "LineString"))
    etree.SubElement(ls, q(KML_NS_22, "tessellate")).text = "1"
    etree.SubElement(ls, q(KML_NS_22, "coordinates")).text = " ".join(f"{p.lon},{p.lat},0" for p in points)

    # Warning lines (optional, NHC)
    for warn_name, warn_desc, coords in extra_lines:
        pmw = etree.SubElement(folder, q(KML_NS_22, "Placemark"))
        etree.SubElement(pmw, q(KML_NS_22, "name")).text = warn_name
        etree.SubElement(pmw, q(KML_NS_22, "styleUrl")).text = "#lineStyle"
        dw = etree.SubElement(pmw, q(KML_NS_22, "description"))
        dw.text = etree.CDATA(warn_desc)
        lsw = etree.SubElement(pmw, q(KML_NS_22, "LineString"))
        etree.SubElement(lsw, q(KML_NS_22, "tessellate")).text = "1"
        etree.SubElement(lsw, q(KML_NS_22, "coordinates")).text = " ".join(f"{lon},{lat},0" for lon, lat in coords)

    # Forecast points
    for p in points:
        pm = etree.SubElement(folder, q(KML_NS_22, "Placemark"))
        etree.SubElement(pm, q(KML_NS_22, "name")).text = p.name
        etree.SubElement(pm, q(KML_NS_22, "styleUrl")).text = "#ptStyle"
        desc = etree.SubElement(pm, q(KML_NS_22, "description"))
        desc.text = etree.CDATA(p.description)
        pt = etree.SubElement(pm, q(KML_NS_22, "Point"))
        etree.SubElement(pt, q(KML_NS_22, "coordinates")).text = f"{p.lon},{p.lat},0"

    # Impact zone polygon
    if impact_geom is not None:
        pm_sw = etree.SubElement(folder, q(KML_NS_22, "Placemark"))
        etree.SubElement(pm_sw, q(KML_NS_22, "name")).text = "Impact Zone"
        etree.SubElement(pm_sw, q(KML_NS_22, "styleUrl")).text = "#polyStyle"
        desc = etree.SubElement(pm_sw, q(KML_NS_22, "description"))
        desc.text = etree.CDATA(IMPACT_DESCRIPTION)
        pm_sw.append(impact_geom)

    return etree.tostring(kml, xml_declaration=True, encoding="UTF-8", pretty_print=False)


def convert_jtwc_kmz(raw_kmz: bytes) -> Tuple[bytes, str]:
    raw_kml = read_kmz_kml_bytes(raw_kmz)
    root = parse_xml_bytes(raw_kml)
    ns = root.nsmap.get(None, KML_NS_22)

    doc = root.find(".//" + q(ns, "Document"))
    if doc is None:
        raise ValueError("JTWC: No <Document> found.")

    all_names = [txt(pm.find("./" + q(ns, "name"))) for pm in doc.findall(".//" + q(ns, "Placemark"))]
    storm_id, storm_name = parse_jtwc_storm_id_name(all_names)
    reference_utc = parse_dtg_anywhere(all_names) or parse_anchor_yyMMddhh(all_names)

    forecast = None
    for f in doc.findall(".//" + q(ns, "Folder")):
        nm = txt(f.find("./" + q(ns, "name")))
        if jtwc_is_forecast_folder(nm):
            forecast = f
            break
    if forecast is None:
        forecast = doc

    raw_forecast_points: List[Tuple[str, float, float, int, int, int]] = []
    for pm in forecast.findall(".//" + q(ns, "Placemark")):
        name = txt(pm.find("./" + q(ns, "name")))
        pt = jtwc_extract_point(pm, ns)
        if not pt:
            continue
        parsed = parse_forecast_day_hour_knots_jtwc(name)
        if not parsed:
            continue
        day, hour, knots = parsed
        lon, lat = pt
        raw_forecast_points.append((name, lon, lat, day, hour, knots))

    if not raw_forecast_points:
        raise ValueError("JTWC: No forecast points found (expected 'DD/HHZ - N knots').")

    inferred_utcs = infer_forecast_datetimes_jtwc(raw_forecast_points, reference_utc)

    out_points: List[OutPoint] = []
    for (name, lon, lat, _d, _h, knots), utc_dt in zip(raw_forecast_points, inferred_utcs):
        agency = jtwc_pick_agency_option2(lon, lat)
        category = classify_wind_table(knots, agency)

        dt_local_naive = utc_dt.replace(tzinfo=None)
        tzinfo, abbr = tzinfo_and_abbr_from_location(lat, lon, dt_local_naive, agency)
        local_dt = utc_dt.astimezone(tzinfo)

        kph, mph = knots_to_kph_mph(knots)
        time_str = local_dt.strftime("%H:%M")
        month_day = format_month_day(local_dt)

        desc = (
            f"{category}: The forecast center of circulation with a maximum sustained wind speed of "
            f"{knots} knots / {kph} kph / {mph} mph as of {time_str} {abbr} {month_day}."
        )

        out_points.append(OutPoint(name=name, lon=lon, lat=lat, description=desc))

    impact_geom = jtwc_extract_danger_swath_geometry(forecast, ns)

    first_label = raw_forecast_points[0][0]
    m = FORECAST_NAME_RE_JTWC.search(first_label)
    d_h = f"{int(m.group('day')):02d}/{int(m.group('hour')):02d}Z" if m else ""
    parts = [p for p in [storm_id, storm_name, d_h, "Cleaned Forecast"] if p]
    file_stem = " ".join(parts).strip() or "output"

    out_kml = build_clean_kml_simple("Cleaned Forecast", out_points, impact_geom)
    return out_kml, file_stem


# ======================================================================================
# NHC CONVERTER
# ======================================================================================

VALID_AT_RE = re.compile(r"Valid at:\s*(.+?)\s*</td>", re.IGNORECASE)
MAX_WIND_RE = re.compile(r"Maximum Wind:\s*([0-9]{1,3})\s*knots", re.IGNORECASE)


def parse_nhc_track_desc(desc_html: str) -> Tuple[Optional[datetime], Optional[int], Optional[str]]:
    """
    Returns (local_naive_datetime, max_wind_knots, storm_descriptor)
    storm_descriptor example from HTML: 'Tropical Storm Melissa (AL132025)'
    """
    if not desc_html:
        return None, None, None

    # storm descriptor
    storm_desc = None
    m0 = re.search(r"<h2>\s*([^<]+)\s*</h2>", desc_html, re.IGNORECASE)
    if m0:
        storm_desc = re.sub(r"\s+", " ", m0.group(1).strip())

    # Valid at
    m = VALID_AT_RE.search(desc_html)
    dt_local = None
    if m:
        raw = re.sub(r"\s+", " ", m.group(1).strip())
        try:
            dt_local = dtparser.parse(raw, fuzzy=True).replace(tzinfo=None)
        except Exception:
            dt_local = None

    # Max wind
    m2 = MAX_WIND_RE.search(desc_html)
    knots = int(m2.group(1)) if m2 else None

    return dt_local, knots, storm_desc


def parse_coords_list(coord_text: str) -> List[Tuple[float, float]]:
    coords: List[Tuple[float, float]] = []
    for tok in (coord_text or "").split():
        parts = tok.split(",")
        if len(parts) >= 2:
            coords.append((float(parts[0]), float(parts[1])))
    return coords


def linestring_to_polygon_geom(line_coords: List[Tuple[float, float]]) -> etree._Element:
    """
    Convert a LineString coordinate list into a Polygon outer ring.
    - Ensures ring is closed.
    - Uses KML 2.2 namespace.
    """
    if len(line_coords) < 4:
        raise ValueError("TOA 34 contour is too short to form a polygon.")

    ring = list(line_coords)
    if ring[0] != ring[-1]:
        ring.append(ring[0])

    poly = etree.Element(q(KML_NS_22, "Polygon"))
    etree.SubElement(poly, q(KML_NS_22, "tessellate")).text = "1"
    ob = etree.SubElement(poly, q(KML_NS_22, "outerBoundaryIs"))
    lr = etree.SubElement(ob, q(KML_NS_22, "LinearRing"))
    ce = etree.SubElement(lr, q(KML_NS_22, "coordinates"))
    ce.text = " ".join(f"{lon},{lat},0" for lon, lat in ring)
    return poly


def extract_best_linestring(doc: etree._Element, ns: str) -> Optional[List[Tuple[float, float]]]:
    """
    Select the LineString with the most coordinate points.
    """
    best = None
    best_n = -1
    for pm in doc.findall(".//" + q(ns, "Placemark")):
        coords = pm.findtext(".//" + q(ns, "LineString") + "/" + q(ns, "coordinates")) or ""
        pts = parse_coords_list(coords)
        if len(pts) > best_n:
            best_n = len(pts)
            best = pts
    return best if best_n > 0 else None


def convert_nhc(track_kmz: bytes, toa34_kmz: bytes, ww_kmz: Optional[bytes]) -> Tuple[bytes, str]:
    # Required TRACK
    track_root, track_ns = load_kmz_root(track_kmz)
    track_doc = get_doc(track_root, track_ns, "NHC TRACK")

    # Required TOA34
    toa_root, toa_ns = load_kmz_root(toa34_kmz)
    toa_doc = get_doc(toa_root, toa_ns, "NHC TOA 34")

    # Parse points from TRACK (Point placemarks)
    track_pms = track_doc.findall(".//" + q(track_ns, "Placemark"))
    raw_pts: List[Tuple[float, float, str]] = []
    for pm in track_pms:
        coord = pm.findtext(".//" + q(track_ns, "Point") + "/" + q(track_ns, "coordinates"))
        if not coord:
            continue
        lon, lat, *_ = coord.split(",")
        desc = pm.findtext(q(track_ns, "description")) or ""
        raw_pts.append((float(lon), float(lat), desc))

    if not raw_pts:
        raise ValueError("NHC: No track points found in TRACK KMZ.")

    parsed_points: List[Tuple[float, float, datetime, int, str]] = []
    for lon, lat, desc_html in raw_pts:
        dt_local_naive, knots, storm_desc = parse_nhc_track_desc(desc_html)
        if dt_local_naive is None or knots is None:
            continue
        parsed_points.append((lon, lat, dt_local_naive, knots, storm_desc or ""))

    if not parsed_points:
        raise ValueError("NHC: Could not parse any point times/winds from TRACK descriptions.")

    # Determine storm ID + name for filename from descriptor / doc title
    # storm_desc like: "Tropical Storm Melissa (AL132025)"
    storm_id = None
    storm_name = None
    first_storm_desc = parsed_points[0][4]
    if first_storm_desc:
        m = re.search(r"\(([A-Z]{2}\d{6})\)", first_storm_desc)
        if m:
            storm_id = m.group(1)
        m2 = re.search(r"\b(?:Tropical Storm|Hurricane|Tropical Depression)\s+([A-Za-z0-9_-]+)\s*\(", first_storm_desc, re.IGNORECASE)
        if m2:
            storm_name = m2.group(1).upper()

    if not storm_id:
        # fallback: doc title might contain AL132025
        doc_title = track_doc.findtext(q(track_ns, "name")) or ""
        m = re.search(r"\b([A-Z]{2}\d{6})\b", doc_title)
        if m:
            storm_id = m.group(1)

    # Build output points + descriptions (timezone by location)
    out_points: List[OutPoint] = []
    for idx, (lon, lat, dt_local_naive, knots, _sd) in enumerate(parsed_points):
        tzinfo, abbr = tzinfo_and_abbr_from_location(lat, lon, dt_local_naive, "NHC")
        local_dt = dt_local_naive.replace(tzinfo=tzinfo)

        category = classify_wind_nhc(knots)
        kph, mph = knots_to_kph_mph(knots)
        center_phrase = "estimated center of circulation" if idx == 0 else "forecast center of circulation"

        desc = (
            f"{category}: The {center_phrase} with a wind speed of "
            f"{knots} knots / {kph} kph / {mph} mph as of {local_dt.strftime('%H:%M')} {abbr} {format_month_day(local_dt)}."
        )
        out_points.append(OutPoint(name=f"Point {idx+1}", lon=lon, lat=lat, description=desc))

    # TOA 34 required: convert best LineString into Polygon
    best_ls = extract_best_linestring(toa_doc, toa_ns)
    if not best_ls:
        raise ValueError("NHC: Could not find a TOA 34 LineString in TOA KMZ.")
    impact_geom = linestring_to_polygon_geom(best_ls)

    # Wind warnings (optional): keep ALL placemarks/lines for now (no dedupe/selection)
    warning_lines: List[Tuple[str, str, List[Tuple[float, float]]]] = []
    if ww_kmz:
        ww_root, ww_ns = load_kmz_root(ww_kmz)
        ww_doc = get_doc(ww_root, ww_ns, "NHC WW")
        adv_dt_local_naive = parsed_points[0][2]

        for pm in ww_doc.findall(".//" + q(ww_ns, "Placemark")):
            warn_name = (pm.findtext(q(ww_ns, "name")) or "").strip()
            coords = pm.findtext(".//" + q(ww_ns, "LineString") + "/" + q(ww_ns, "coordinates")) or ""
            pts = parse_coords_list(coords)
            if not pts or not warn_name:
                continue

            mid_lon, mid_lat = pts[len(pts)//2]
            tzinfo, abbr = tzinfo_and_abbr_from_location(mid_lat, mid_lon, adv_dt_local_naive, "NHC")
            adv_local = adv_dt_local_naive.replace(tzinfo=tzinfo)
            warn_desc = f"{warn_name}: Advisory in effect as of {adv_local.strftime('%H:%M')} {abbr} {format_month_day_dot(adv_local)}."
            warning_lines.append((warn_name, warn_desc, pts))

    # Output filename should follow SAME format as JTWC cleaned file
    # We'll use: "{storm_id} {storm_name} {DD/HHZ} Cleaned Forecast"
    first_dt = parsed_points[0][2]  # local naive dt
    dd = f"{first_dt.day:02d}"
    hh = f"{first_dt.hour:02d}"
    d_h = f"{dd}/{hh}Z"
    parts = [p for p in [storm_id, storm_name, d_h, "Cleaned Forecast"] if p]
    file_stem = " ".join(parts).strip() or "output"

    out_kml = build_clean_kml_simple("Cleaned Forecast", out_points, impact_geom, extra_lines=warning_lines)
    return out_kml, file_stem


# ======================================================================================
# Streamlit UI
# ======================================================================================
def reset_output_state():
    st.session_state.out_kml = None
    st.session_state.out_name = None
    st.session_state.last_upload_sig = None
    st.session_state.uploader_key = st.session_state.get("uploader_key", 0) + 1


st.set_page_config(page_title=APP_NAME, layout="centered")
st.markdown(f"## {APP_NAME}")
st.caption(APP_DESC)

# Download button green
st.markdown(
    """
    <style>
    div[data-testid="stDownloadButton"] button {
        background-color: #16a34a !important;
        color: white !important;
        border: 1px solid #15803d !important;
    }
    div[data-testid="stDownloadButton"] button:hover {
        background-color: #15803d !important;
        border-color: #166534 !important;
    }
    div[data-testid="stDownloadButton"] button:active {
        background-color: #166534 !important;
        border-color: #14532d !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)

if "out_kml" not in st.session_state:
    st.session_state.out_kml = None
if "out_name" not in st.session_state:
    st.session_state.out_name = None
if "last_upload_sig" not in st.session_state:
    st.session_state.last_upload_sig = None
if "uploader_key" not in st.session_state:
    st.session_state.uploader_key = 0

source = st.radio("Source", ["JTWC", "NHC"], horizontal=True)
st.divider()

left, center, right = st.columns([1, 2, 1])

with center:
    if source == "JTWC":
        raw = st.file_uploader(
            "Upload raw JTWC KMZ",
            type=["kmz"],
            key=f"uploader_{st.session_state.uploader_key}_jtwc",
        )

        if raw is not None:
            upload_sig = ("JTWC", raw.name, raw.size)
            if st.session_state.last_upload_sig != upload_sig:
                st.session_state.last_upload_sig = upload_sig
                st.session_state.out_kml = None
                st.session_state.out_name = None

        if raw is None:
            st.info("Upload a raw JTWC KMZ to begin.")
        else:
            if st.session_state.out_kml is None:
                st.write(f"Selected file: **{raw.name}**")
                if st.button("Convert", type="primary", use_container_width=True):
                    with st.spinner("Converting…"):
                        try:
                            out_kml, stem = convert_jtwc_kmz(raw.getvalue())
                            st.session_state.out_kml = out_kml
                            st.session_state.out_name = f"{safe_filename(stem, 'output')}.kml"
                            st.success("Conversion complete.")
                            st.rerun()
                        except Exception as e:
                            st.session_state.out_kml = None
                            st.session_state.out_name = None
                            st.error(f"Conversion failed: {e}")
            else:
                st.write(f"Output file: **{st.session_state.out_name}**")
                st.download_button(
                    "Download KML",
                    data=st.session_state.out_kml,
                    file_name=st.session_state.out_name,
                    mime="application/vnd.google-earth.kml+xml",
                    use_container_width=True,
                )
                if st.button("Convert another file", use_container_width=True):
                    reset_output_state()
                    st.rerun()

    else:
        st.markdown("### NHC inputs")
        st.caption("TRACK and TOA 34 are required. Wind warnings are optional.")

        track = st.file_uploader(
            "TRACK KMZ — required",
            type=["kmz"],
            key=f"uploader_{st.session_state.uploader_key}_nhc_track",
        )
        toa = st.file_uploader(
            "TOA 34 KMZ — required",
            type=["kmz"],
            key=f"uploader_{st.session_state.uploader_key}_nhc_toa",
        )
        ww = st.file_uploader(
            "Wind Warnings KMZ — optional",
            type=["kmz"],
            key=f"uploader_{st.session_state.uploader_key}_nhc_ww",
        )

        sig_parts = ["NHC"]
        if track: sig_parts += [track.name, track.size]
        if toa: sig_parts += [toa.name, toa.size]
        if ww: sig_parts += [ww.name, ww.size]
        upload_sig = tuple(sig_parts) if len(sig_parts) > 1 else None

        if upload_sig and st.session_state.last_upload_sig != upload_sig:
            st.session_state.last_upload_sig = upload_sig
            st.session_state.out_kml = None
            st.session_state.out_name = None

        if track is None or toa is None:
            st.info("Upload both TRACK and TOA 34 KMZ files to begin.")
        else:
            if st.session_state.out_kml is None:
                if st.button("Convert", type="primary", use_container_width=True):
                    with st.spinner("Converting…"):
                        try:
                            out_kml, stem = convert_nhc(
                                track.getvalue(),
                                toa.getvalue(),
                                ww.getvalue() if ww else None,
                            )
                            st.session_state.out_kml = out_kml
                            st.session_state.out_name = f"{safe_filename(stem, 'output')}.kml"
                            st.success("Conversion complete.")
                            st.rerun()
                        except Exception as e:
                            st.session_state.out_kml = None
                            st.session_state.out_name = None
                            st.error(f"Conversion failed: {e}")
            else:
                st.write(f"Output file: **{st.session_state.out_name}**")
                st.download_button(
                    "Download KML",
                    data=st.session_state.out_kml,
                    file_name=st.session_state.out_name,
                    mime="application/vnd.google-earth.kml+xml",
                    use_container_width=True,
                )
                if st.button("Convert another file", use_container_width=True):
                    reset_output_state()
                    st.rerun()
