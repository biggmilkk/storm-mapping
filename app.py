import io
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List, Tuple

import streamlit as st
from lxml import etree
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo
from timezonefinder import TimezoneFinder


# -------------------------
# App branding
# -------------------------
APP_NAME = "StormTrack Mapper"
APP_DESC = (
    "Convert raw JTWC storm KMZ into clean KML for alert mapping."
)

TRACK_DESCRIPTION = "Forecast Track: The forecast track of the system's center of circulation."
SWATH_DESCRIPTION = "Forecast Impact Zone: The area in which impacts from the tropical system are likely to be felt."


# -------------------------
# Constants / Namespaces
# -------------------------
KML_NS = "http://www.opengis.net/kml/2.2"
NSMAP = {None: KML_NS}
TF = TimezoneFinder()


def q(tag: str) -> str:
    return f"{{{KML_NS}}}{tag}"


def txt(el) -> str:
    return (el.text or "").strip() if el is not None and el.text else ""


def normalize_lon_180(lon: float) -> float:
    """Normalize longitude to [-180, 180)."""
    return ((lon + 180.0) % 360.0) - 180.0


def norm_name(s: str) -> str:
    return (s or "").strip().lower()


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


def extract_danger_swath_geometry(forecast_folder: etree._Element) -> Optional[etree._Element]:
    """
    Finds the Placemark named exactly '34 knot Danger Swath' (case-insensitive),
    and returns a COPY of its geometry element (Polygon or MultiGeometry).
    """
    for pm in forecast_folder.findall(".//" + q("Placemark")):
        name = norm_name(txt(pm.find("./" + q("name"))))
        if name == "34 knot danger swath":
            mg = pm.find(".//" + q("MultiGeometry"))
            if mg is not None:
                return etree.fromstring(etree.tostring(mg))
            poly = pm.find(".//" + q("Polygon"))
            if poly is not None:
                return etree.fromstring(etree.tostring(poly))
            return None
    return None


# -------------------------
# Agency selection (Option 2) — for matrix only
# -------------------------
def in_box(lon360: float, lat: float, lon_min: float, lon_max: float, lat_min: float, lat_max: float) -> bool:
    return (lon_min <= lon360 <= lon_max) and (lat_min <= lat <= lat_max)


def pick_agency(lon: float, lat: float) -> str:
    """
    Option 2:
      - BOM/FMS for Australia region: lat -40..0, lon 90E..160E
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
# Classification (1-minute table naming)
# -------------------------
def classify_wind(knots: int, agency: str) -> str:
    """
    Category names and thresholds aligned to the provided 1-minute sustained wind table.

    Agencies used by this tool:
      - JTWC (Pacific)
      - IMD  (Indian Ocean)
      - BOM/FMS (Australia region)
    """

    # JTWC (1-minute)
    if agency == "JTWC":
        if knots < 34:
            return "Tropical Depression"
        if 34 <= knots <= 63:
            return "Tropical Storm"
        if 64 <= knots <= 129:
            return "Typhoon"
        return "Super Typhoon"  # >=130

    # IMD (1-minute table wording)
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

    # BOM/FMS (Australia region) — table wording
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


# -------------------------
# Storm ID + name parsing (for filename)
# -------------------------
WARNING_RE = re.compile(
    r"\bTROPICAL\s+(?:CYCLONE|STORM|DEPRESSION)\s+(\d{1,2}[A-Z])\s+\(([^)]+)\).*?\bWARNING\b",
    re.IGNORECASE
)


def parse_storm_id_name(all_names: List[str]) -> Tuple[Optional[str], Optional[str]]:
    for s in all_names:
        m = WARNING_RE.search(s or "")
        if m:
            return m.group(1).upper().strip(), m.group(2).upper().strip()
    return None, None


# -------------------------
# Time parsing (robust)
# -------------------------
DTG_RE = re.compile(r"\b(\d{2})(\d{2})(\d{2})Z\s+([A-Z]{3})\s+(\d{4})\b", re.IGNORECASE)
MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
ANCHOR_YYMMDDHH_RE = re.compile(r"\b(\d{8})Z\b")
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


# -------------------------
# Timezone abbreviations (location-based)
# -------------------------
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


def is_bad_abbrev(abbr: Optional[str]) -> bool:
    if not abbr:
        return True
    a = abbr.strip()
    if re.fullmatch(r"[+-]\d{1,2}(:\d{2})?", a):
        return True
    if a.upper().startswith(("UTC", "GMT")) and re.search(r"[+-]\d", a):
        return True
    return False


def tz_abbrev_for_point(lat: float, lon: float, utc_dt: datetime, agency_for_fallbacks: str) -> Tuple[ZoneInfo, str]:
    lon_norm = normalize_lon_180(lon)

    tzname = TF.timezone_at(lat=lat, lng=lon_norm)
    if tzname:
        try:
            tzi = ZoneInfo(tzname)
            abbr = utc_dt.astimezone(tzi).tzname()
            if not is_bad_abbrev(abbr):
                return tzi, abbr
        except Exception:
            pass

    if agency_for_fallbacks == "BOM":
        candidates = AU_FALLBACK_ZONES
    elif agency_for_fallbacks == "IMD":
        candidates = INDIAN_OCEAN_FALLBACK_ZONES
    else:
        candidates = PACIFIC_FALLBACK_ZONES

    approx_off = round(lon_norm / 15.0)
    best = None
    for z in candidates:
        try:
            tzi = ZoneInfo(z)
            local = utc_dt.astimezone(tzi)
            abbr = local.tzname()
            if is_bad_abbrev(abbr):
                continue
            off_hours = local.utcoffset().total_seconds() / 3600.0 if local.utcoffset() else 0.0
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


def knots_to_kph_mph(knots: int) -> Tuple[int, int]:
    return int(round(knots * 1.852)), int(round(knots * 1.15078))


def build_point_description(category: str, knots: int, utc_dt: datetime, lat: float, lon: float, agency: str) -> str:
    tzinfo, abbr = tz_abbrev_for_point(lat, lon, utc_dt, agency)
    local_dt = utc_dt.astimezone(tzinfo)
    kph, mph = knots_to_kph_mph(knots)
    time_str = local_dt.strftime("%H:%M")
    month_day = local_dt.strftime("%B %d").replace(" 0", " ")
    return (
        f"{category}: The forecast center of circulation with a maximum sustained wind speed of "
        f"{knots} knots / {kph} kph / {mph} mph as of {time_str} {abbr} {month_day}."
    )


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


def build_clean_kml(doc_title: str, points: List[ForecastPointOut], swath_geom: Optional[etree._Element]) -> bytes:
    kml = etree.Element(q("kml"), nsmap=NSMAP)
    doc = etree.SubElement(kml, q("Document"))
    etree.SubElement(doc, q("name")).text = doc_title

    # Styles
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

    # Storm Track (fixed description)
    pm_track = etree.SubElement(folder, q("Placemark"))
    etree.SubElement(pm_track, q("name")).text = "Storm Track"
    etree.SubElement(pm_track, q("styleUrl")).text = "#lineStyle"
    desc_track = etree.SubElement(pm_track, q("description"))
    desc_track.text = etree.CDATA(TRACK_DESCRIPTION)
    ls = etree.SubElement(pm_track, q("LineString"))
    etree.SubElement(ls, q("tessellate")).text = "1"
    etree.SubElement(ls, q("coordinates")).text = " ".join(f"{p.lon},{p.lat},0" for p in points)

    # Forecast points
    for p in points:
        pm = etree.SubElement(folder, q("Placemark"))
        etree.SubElement(pm, q("name")).text = p.name
        etree.SubElement(pm, q("styleUrl")).text = "#ptStyle"
        desc = etree.SubElement(pm, q("description"))
        desc.text = etree.CDATA(p.description)
        pt = etree.SubElement(pm, q("Point"))
        etree.SubElement(pt, q("coordinates")).text = f"{p.lon},{p.lat},0"

    # 34 knot Danger Swath (copied from source)
    if swath_geom is not None:
        pm_sw = etree.SubElement(folder, q("Placemark"))
        etree.SubElement(pm_sw, q("name")).text = "34 knot Danger Swath"
        etree.SubElement(pm_sw, q("styleUrl")).text = "#polyStyle"
        desc = etree.SubElement(pm_sw, q("description"))
        desc.text = etree.CDATA(SWATH_DESCRIPTION)
        pm_sw.append(swath_geom)

    return etree.tostring(kml, xml_declaration=True, encoding="UTF-8", pretty_print=False)


# -------------------------
# Conversion
# -------------------------
def convert_raw_jtwc_kmz(raw_kmz: bytes) -> Tuple[bytes, str]:
    raw_kml = read_kmz_kml_bytes(raw_kmz)
    root = parse_kml(raw_kml)

    doc = root.find(".//" + q("Document"))
    if doc is None:
        raise ValueError("No <Document> found in KML.")

    all_names: List[str] = [txt(pm.find("./" + q("name"))) for pm in doc.findall(".//" + q("Placemark"))]
    storm_id, storm_name = parse_storm_id_name(all_names)
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

    inferred_utcs = infer_forecast_datetimes(raw_forecast_points, reference_utc)

    points_out: List[ForecastPointOut] = []
    for (name, lon, lat, _d, _h, knots), utc_dt in zip(raw_forecast_points, inferred_utcs):
        agency = pick_agency(lon, lat)
        category = classify_wind(knots, agency)
        desc = build_point_description(category, knots, utc_dt, lat, lon, agency)
        points_out.append(ForecastPointOut(name=name, lon=lon, lat=lat, utc_dt=utc_dt, knots=knots, description=desc))

    # Copy existing "34 knot Danger Swath" geometry only
    swath_geom = extract_danger_swath_geometry(forecast)

    # Preferred filename: "27P NARELLE 18/12Z Cleaned Forecast"
    first_label = raw_forecast_points[0][0]
    m = FORECAST_NAME_RE.search(first_label)
    d_h = f"{int(m.group('day')):02d}/{int(m.group('hour')):02d}Z" if m else ""
    parts = [p for p in [storm_id, storm_name, d_h, "Cleaned Forecast"] if p]
    file_stem = " ".join(parts).strip() or "Cleaned Forecast"

    out_kml = build_clean_kml("Cleaned Forecast", points_out, swath_geom)
    return out_kml, file_stem


# -------------------------
# Streamlit UI — “one action” flow
# -------------------------
st.set_page_config(page_title=APP_NAME, layout="centered")
st.markdown(f"## {APP_NAME}")
st.write(APP_DESC)

raw = st.file_uploader("Upload raw JTWC KMZ", type=["kmz"])

if "out_kml" not in st.session_state:
    st.session_state.out_kml = None
    st.session_state.out_name = None

col1, col2, col3 = st.columns([1, 2, 1])

if raw:
    with col2:
        if st.button("Convert & Prepare Download", use_container_width=True):
            try:
                out_kml, file_stem = convert_raw_jtwc_kmz(raw.getvalue())
                safe = re.sub(r"[^A-Za-z0-9._ -]+", "", file_stem).strip()
                safe = re.sub(r"\s+", " ", safe)[:140] if safe else "cleaned"
                st.session_state.out_kml = out_kml
                st.session_state.out_name = f"{safe}.kml"
                st.success("Prepared. Click below to download.")
            except Exception as e:
                st.session_state.out_kml = None
                st.session_state.out_name = None
                st.error(f"Conversion failed: {e}")

    if st.session_state.out_kml and st.session_state.out_name:
        with col2:
            st.download_button(
                "Download KML",
                data=st.session_state.out_kml,
                file_name=st.session_state.out_name,
                mime="application/vnd.google-earth.kml+xml",
                use_container_width=True,
            )
else:
    st.info("Upload a raw JTWC KMZ to begin.")
