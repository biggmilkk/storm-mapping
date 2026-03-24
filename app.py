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
# Timezone (location lookup) + JTWC carry-forward support
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
    "America/Nassau",
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


def tzinfo_and_abbr_try(lat: float, lon: float, dt_utc: datetime) -> Optional[Tuple[ZoneInfo, str]]:
    lon_norm = normalize_lon_180(lon)
    tzname = TF.timezone_at(lat=lat, lng=lon_norm) or TF.closest_timezone_at(lat=lat, lng=lon_norm)
    if tzname:
        try:
            tzi = ZoneInfo(tzname)
            abbr = dt_utc.astimezone(tzi).tzname()
            if not is_bad_abbrev(abbr):
                return tzi, abbr
        except Exception:
            pass
    return None


def tzinfo_and_abbr_fallback_from_group(dt_utc: datetime, lon: float, group: str) -> Tuple[ZoneInfo, str]:
    lon_norm = normalize_lon_180(lon)

    if group == "BOM":
        candidates = AU_FALLBACK_ZONES
    elif group == "IMD":
        candidates = INDIAN_OCEAN_FALLBACK_ZONES
    elif group == "NHC":
        candidates = AMERICAS_FALLBACK_ZONES
    else:
        candidates = PACIFIC_FALLBACK_ZONES

    approx_off = round(lon_norm / 15.0)
    best = None
    for z in candidates:
        try:
            tzi = ZoneInfo(z)
            local = dt_utc.astimezone(tzi)
            abbr = local.tzname()
            if is_bad_abbrev(abbr):
                continue
            off_hours = (local.utcoffset().total_seconds() / 3600.0) if local.utcoffset() else 0.0
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
# CATEGORY LABELS
# ======================================================================================
def classify_wind_table(knots: int, agency: str) -> str:
    if agency == "JTWC":
        if knots < 34:
            return "Tropical Depression"
        if 34 <= knots <= 63:
            return "Tropical Storm"
        if 64 <= knots <= 129:
            return "Typhoon"
        return "Super Typhoon"
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
        return "Super Cyclonic Storm"
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
    return "Category 5 Severe Tropical Cyclone"


def classify_wind_nhc(knots: int) -> str:
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
    return "Category 5 Hurricane"


# ======================================================================================
# JTWC CONVERTER (timezone carry-forward)
# ======================================================================================
def jtwc_is_forecast_folder(name: str) -> bool:
    return "forecast" in (name or "").strip().lower()


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


def build_clean_kml_simple(doc_title: str, points: List[OutPoint], impact_geom: Optional[etree._Element]) -> bytes:
    kml = etree.Element(q(KML_NS_22, "kml"), nsmap=NSMAP_22)
    doc = etree.SubElement(kml, q(KML_NS_22, "Document"))
    etree.SubElement(doc, q(KML_NS_22, "name")).text = doc_title

    folder = etree.SubElement(doc, q(KML_NS_22, "Folder"))
    etree.SubElement(folder, q(KML_NS_22, "name")).text = "Forecast"

    pm_track = etree.SubElement(folder, q(KML_NS_22, "Placemark"))
    etree.SubElement(pm_track, q(KML_NS_22, "name")).text = "Storm Track"
    d = etree.SubElement(pm_track, q(KML_NS_22, "description"))
    d.text = etree.CDATA(TRACK_DESCRIPTION)
    ls = etree.SubElement(pm_track, q(KML_NS_22, "LineString"))
    etree.SubElement(ls, q(KML_NS_22, "tessellate")).text = "1"
    etree.SubElement(ls, q(KML_NS_22, "coordinates")).text = " ".join(f"{p.lon},{p.lat},0" for p in points)

    for p in points:
        pm = etree.SubElement(folder, q(KML_NS_22, "Placemark"))
        etree.SubElement(pm, q(KML_NS_22, "name")).text = p.name
        desc = etree.SubElement(pm, q(KML_NS_22, "description"))
        desc.text = etree.CDATA(p.description)
        pt = etree.SubElement(pm, q(KML_NS_22, "Point"))
        etree.SubElement(pt, q(KML_NS_22, "coordinates")).text = f"{p.lon},{p.lat},0"

    if impact_geom is not None:
        pm_sw = etree.SubElement(folder, q(KML_NS_22, "Placemark"))
        etree.SubElement(pm_sw, q(KML_NS_22, "name")).text = "Impact Zone"
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
        coord = pm.findtext(".//" + q(ns, "Point") + "/" + q(ns, "coordinates")) or ""
        if not coord:
            continue
        parsed = parse_forecast_day_hour_knots_jtwc(name)
        if not parsed:
            continue
        lon, lat, *_ = coord.split(",")
        day, hour, knots = parsed
        raw_forecast_points.append((name, float(lon), float(lat), day, hour, knots))

    if not raw_forecast_points:
        raise ValueError("JTWC: No forecast points found (expected 'DD/HHZ - N knots').")

    inferred_utcs = infer_forecast_datetimes_jtwc(raw_forecast_points, reference_utc)

    last_tzinfo: Optional[ZoneInfo] = None
    last_abbr: Optional[str] = None

    out_points: List[OutPoint] = []
    for (name, lon, lat, _d, _h, knots), utc_dt in zip(raw_forecast_points, inferred_utcs):
        agency = jtwc_pick_agency_option2(lon, lat)
        category = classify_wind_table(knots, agency)

        found = tzinfo_and_abbr_try(lat, lon, utc_dt)

        if found is not None:
            tzinfo, abbr = found
            last_tzinfo, last_abbr = tzinfo, abbr
        else:
            if last_tzinfo is not None and last_abbr is not None:
                tzinfo, abbr = last_tzinfo, last_abbr
            else:
                tzinfo, abbr = tzinfo_and_abbr_fallback_from_group(utc_dt, lon, agency)
                last_tzinfo, last_abbr = tzinfo, abbr

        local_dt = utc_dt.astimezone(tzinfo)
        kph, mph = knots_to_kph_mph(knots)

        desc = (
            f"{category}: The forecast center of circulation with a maximum sustained wind speed of "
            f"{knots} knots / {kph} kph / {mph} mph as of {local_dt.strftime('%H:%M')} {abbr} {format_month_day(local_dt)}."
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
# NHC CONVERTER (TOA optional)
# ======================================================================================
NHC_TZ_ABBREV_TO_IANA: Dict[str, str] = {
    "EDT": "America/New_York",
    "EST": "America/New_York",
    "CDT": "America/Chicago",
    "CST": "America/Chicago",
    "MDT": "America/Denver",
    "MST": "America/Denver",
    "PDT": "America/Los_Angeles",
    "PST": "America/Los_Angeles",
    "AST": "America/Puerto_Rico",
    "ADT": "America/Halifax",
    "AKDT": "America/Anchorage",
    "AKST": "America/Anchorage",
    "HST": "Pacific/Honolulu",
}

VALID_AT_LINE_RE = re.compile(r"Valid at:\s*([^<]+)", re.IGNORECASE)
VALID_AT_TZ_RE = re.compile(
    r"(?P<time>\d{1,2}:\d{2}\s*[AP]M)\s+(?P<tz>[A-Z]{2,4})\s+(?P<date>[A-Za-z]+\s+\d{1,2},\s*\d{4})",
    re.IGNORECASE
)
MAX_WIND_RE = re.compile(r"Maximum Wind:\s*([0-9]{1,3})\s*knots", re.IGNORECASE)


def parse_nhc_track_desc(desc_html: str) -> Tuple[Optional[datetime], Optional[int], Optional[str], Optional[str]]:
    if not desc_html:
        return None, None, None, None

    storm_desc = None
    m0 = re.search(r"<h2>\s*([^<]+)\s*</h2>", desc_html, re.IGNORECASE)
    if m0:
        storm_desc = re.sub(r"\s+", " ", m0.group(1).strip())

    dt_local = None
    tz_abbrev = None
    mline = VALID_AT_LINE_RE.search(desc_html)
    if mline:
        raw_line = re.sub(r"\s+", " ", mline.group(1).strip())
        mtz = VALID_AT_TZ_RE.search(raw_line)
        if mtz:
            tz_abbrev = mtz.group("tz").upper()
            try:
                dt_local = dtparser.parse(f"{mtz.group('time')} {mtz.group('date')}", fuzzy=True).replace(tzinfo=None)
            except Exception:
                dt_local = None
        else:
            try:
                dt_local = dtparser.parse(raw_line, fuzzy=True).replace(tzinfo=None)
            except Exception:
                dt_local = None

    m2 = MAX_WIND_RE.search(desc_html)
    knots = int(m2.group(1)) if m2 else None

    return dt_local, knots, storm_desc, tz_abbrev


def parse_coords_list(coord_text: str) -> List[Tuple[float, float]]:
    coords: List[Tuple[float, float]] = []
    for tok in (coord_text or "").split():
        parts = tok.split(",")
        if len(parts) >= 2:
            coords.append((float(parts[0]), float(parts[1])))
    return coords


def extract_best_linestring(doc: etree._Element, ns: str) -> Optional[List[Tuple[float, float]]]:
    best = None
    best_n = -1
    for pm in doc.findall(".//" + q(ns, "Placemark")):
        coords = pm.findtext(".//" + q(ns, "LineString") + "/" + q(ns, "coordinates")) or ""
        pts = parse_coords_list(coords)
        if len(pts) > best_n:
            best_n = len(pts)
            best = pts
    return best if best_n > 0 else None


def linestring_to_polygon_geom(line_coords: List[Tuple[float, float]]) -> etree._Element:
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


def build_nhc_kml(
    track_points: List[Tuple[float, float, datetime, int, str]],
    toa_polygon: Optional[etree._Element],
    toa_folder_name: Optional[str],
    ww_folder_name: Optional[str],
    ww_lines: List[Tuple[str, str, List[Tuple[float, float]]]],
) -> bytes:
    kml = etree.Element(q(KML_NS_22, "kml"), nsmap=NSMAP_22)
    doc = etree.SubElement(kml, q(KML_NS_22, "Document"))
    etree.SubElement(doc, q(KML_NS_22, "name")).text = "Untitled map"

    # Optional TOA folder
    if toa_polygon is not None and toa_folder_name:
        f_toa = etree.SubElement(doc, q(KML_NS_22, "Folder"))
        etree.SubElement(f_toa, q(KML_NS_22, "name")).text = toa_folder_name

        pm_toa = etree.SubElement(f_toa, q(KML_NS_22, "Placemark"))
        etree.SubElement(pm_toa, q(KML_NS_22, "name")).text = ""
        d_toa = etree.SubElement(pm_toa, q(KML_NS_22, "description"))
        d_toa.text = etree.CDATA(IMPACT_DESCRIPTION)
        pm_toa.append(toa_polygon)

    # Forecast Track folder
    f_track = etree.SubElement(doc, q(KML_NS_22, "Folder"))
    etree.SubElement(f_track, q(KML_NS_22, "name")).text = "Forecast Track"

    pm_line = etree.SubElement(f_track, q(KML_NS_22, "Placemark"))
    etree.SubElement(pm_line, q(KML_NS_22, "name")).text = ""
    d_line = etree.SubElement(pm_line, q(KML_NS_22, "description"))
    d_line.text = etree.CDATA(TRACK_DESCRIPTION)

    ls = etree.SubElement(pm_line, q(KML_NS_22, "LineString"))
    etree.SubElement(ls, q(KML_NS_22, "tessellate")).text = "1"
    etree.SubElement(ls, q(KML_NS_22, "coordinates")).text = "\n".join(
        f"{lon},{lat},0" for lon, lat, _, _, _ in track_points
    )

    for lon, lat, dt_local_naive, knots, tz_abbrev in track_points:
        category = classify_wind_nhc(knots)
        kph, mph = knots_to_kph_mph(knots)

        desc_text = (
            f"{category}: The forecast center of circulation with a wind speed of "
            f"{knots} knots / {kph} kph / {mph} mph at {dt_local_naive.strftime('%H:%M')} {tz_abbrev} {format_month_day_dot(dt_local_naive)}."
        )

        pm = etree.SubElement(f_track, q(KML_NS_22, "Placemark"))
        etree.SubElement(pm, q(KML_NS_22, "name")).text = ""
        d = etree.SubElement(pm, q(KML_NS_22, "description"))
        d.text = etree.CDATA(desc_text)
        pt = etree.SubElement(pm, q(KML_NS_22, "Point"))
        etree.SubElement(pt, q(KML_NS_22, "coordinates")).text = f"{lon},{lat},0"

    if ww_folder_name and ww_lines:
        f_ww = etree.SubElement(doc, q(KML_NS_22, "Folder"))
        etree.SubElement(f_ww, q(KML_NS_22, "name")).text = ww_folder_name

        for warn_name, warn_desc, coords in ww_lines:
            pmw = etree.SubElement(f_ww, q(KML_NS_22, "Placemark"))
            etree.SubElement(pmw, q(KML_NS_22, "name")).text = warn_name
            dw = etree.SubElement(pmw, q(KML_NS_22, "description"))
            dw.text = etree.CDATA(warn_desc)
            lsw = etree.SubElement(pmw, q(KML_NS_22, "LineString"))
            etree.SubElement(lsw, q(KML_NS_22, "tessellate")).text = "1"
            etree.SubElement(lsw, q(KML_NS_22, "coordinates")).text = "\n".join(
                f"{lon},{lat},0" for lon, lat in coords
            )

    return etree.tostring(kml, xml_declaration=True, encoding="UTF-8", pretty_print=False)


def convert_nhc(track_kmz: bytes, toa34_kmz: Optional[bytes], ww_kmz: Optional[bytes]) -> Tuple[bytes, str]:
    track_root, track_ns = load_kmz_root(track_kmz)
    track_doc = get_doc(track_root, track_ns, "NHC TRACK")

    # Parse TRACK points
    raw_pts: List[Tuple[float, float, str]] = []
    for pm in track_doc.findall(".//" + q(track_ns, "Placemark")):
        coord = pm.findtext(".//" + q(track_ns, "Point") + "/" + q(track_ns, "coordinates"))
        if not coord:
            continue
        lon, lat, *_ = coord.split(",")
        desc = pm.findtext(q(track_ns, "description")) or ""
        raw_pts.append((float(lon), float(lat), desc))

    if not raw_pts:
        raise ValueError("NHC: No track points found in TRACK KMZ.")

    track_points: List[Tuple[float, float, datetime, int, str]] = []
    first_storm_desc = None

    for lon, lat, desc_html in raw_pts:
        dt_local_naive, knots, storm_desc, tz_abbrev = parse_nhc_track_desc(desc_html)
        if dt_local_naive is None or knots is None:
            continue
        tz_abbrev = (tz_abbrev or "UTC").upper()
        track_points.append((lon, lat, dt_local_naive, knots, tz_abbrev))
        if not first_storm_desc and storm_desc:
            first_storm_desc = storm_desc

    if not track_points:
        raise ValueError("NHC: Could not parse any point times/winds from TRACK descriptions.")

    # Optional TOA
    toa_polygon = None
    toa_folder_name = None
    if toa34_kmz:
        toa_root, toa_ns = load_kmz_root(toa34_kmz)
        toa_doc = get_doc(toa_root, toa_ns, "NHC TOA 34")
        toa_folder_name = toa_doc.findtext(q(toa_ns, "name")) or "Earliest-Reasonable Time of Arrival"
        best_ls = extract_best_linestring(toa_doc, toa_ns)
        if best_ls:
            toa_polygon = linestring_to_polygon_geom(best_ls)

    # Storm ID + name for filename
    storm_id = None
    storm_name = None
    if first_storm_desc:
        m = re.search(r"\(([A-Z]{2}\d{6})\)", first_storm_desc)
        if m:
            storm_id = m.group(1)
        m2 = re.search(
            r"\b(?:Tropical Storm|Hurricane|Tropical Depression|Potential Tropical Cyclone)\s+([A-Za-z0-9_-]+)\s*\(",
            first_storm_desc,
            re.IGNORECASE
        )
        if m2:
            storm_name = m2.group(1).upper()

    if not storm_id:
        doc_title = track_doc.findtext(q(track_ns, "name")) or ""
        m = re.search(r"\b([A-Z]{2}\d{6})\b", doc_title)
        if m:
            storm_id = m.group(1)

    # Filename time from first point using tz abbrev mapping (best effort)
    first_local_naive = track_points[0][2]
    tz_abbrev = (track_points[0][4] or "UTC").upper()
    utc_dt_for_name = None

    if tz_abbrev in NHC_TZ_ABBREV_TO_IANA:
        try:
            z = ZoneInfo(NHC_TZ_ABBREV_TO_IANA[tz_abbrev])
            utc_dt_for_name = first_local_naive.replace(tzinfo=z).astimezone(timezone.utc)
        except Exception:
            utc_dt_for_name = None

    if utc_dt_for_name is None:
        utc_dt_for_name = first_local_naive.replace(tzinfo=timezone.utc)

    d_h = f"{utc_dt_for_name.day:02d}/{utc_dt_for_name.hour:02d}Z"
    parts = [p for p in [storm_id, storm_name, d_h, "Cleaned Forecast"] if p]
    file_stem = " ".join(parts).strip() or "output"

    # WW optional (kept as-is for now)
    ww_folder_name = None
    ww_lines: List[Tuple[str, str, List[Tuple[float, float]]]] = []
    if ww_kmz:
        ww_root, ww_ns = load_kmz_root(ww_kmz)
        ww_doc = get_doc(ww_root, ww_ns, "NHC WW")
        ww_folder_name = ww_doc.findtext(q(ww_ns, "name")) or "Watch/Warnings"

        adv_local_naive = track_points[0][2]
        adv_tz_abbrev = (track_points[0][4] or "UTC").upper()

        for pm in ww_doc.findall(".//" + q(ww_ns, "Placemark")):
            warn_name = (pm.findtext(q(ww_ns, "name")) or "").strip()
            coords = pm.findtext(".//" + q(ww_ns, "LineString") + "/" + q(ww_ns, "coordinates")) or ""
            pts = parse_coords_list(coords)
            if not pts or not warn_name:
                continue
            warn_desc = f"{warn_name}: Advisory in place as of {adv_local_naive.strftime('%H:%M')} {adv_tz_abbrev} {format_month_day_dot(adv_local_naive)}."
            ww_lines.append((warn_name, warn_desc, pts))

    out_kml = build_nhc_kml(
        track_points=track_points,
        toa_polygon=toa_polygon,
        toa_folder_name=toa_folder_name,
        ww_folder_name=ww_folder_name,
        ww_lines=ww_lines,
    )
    return out_kml, file_stem


# ======================================================================================
# Streamlit UI (left-aligned, full-width)
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

if source == "JTWC":
    raw = st.file_uploader("Upload raw JTWC KMZ", type=["kmz"], key=f"uploader_{st.session_state.uploader_key}_jtwc")

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

    track = st.file_uploader("Upload TRACK.kmz (required)", type=["kmz"], key=f"uploader_{st.session_state.uploader_key}_nhc_track")
    toa = st.file_uploader("Upload Earliest Reasonable TOA 34.kmz", type=["kmz"], key=f"uploader_{st.session_state.uploader_key}_nhc_toa")
    ww = st.file_uploader("Upload WW.kmz", type=["kmz"], key=f"uploader_{st.session_state.uploader_key}_nhc_ww")

    sig_parts = ["NHC"]
    if track: sig_parts += [track.name, track.size]
    if toa: sig_parts += [toa.name, toa.size]
    if ww: sig_parts += [ww.name, ww.size]
    upload_sig = tuple(sig_parts) if len(sig_parts) > 1 else None

    if upload_sig and st.session_state.last_upload_sig != upload_sig:
        st.session_state.last_upload_sig = upload_sig
        st.session_state.out_kml = None
        st.session_state.out_name = None

    if track is None:
        st.info("Upload the NHC TRACK KMZ to begin.")
    else:
        if st.session_state.out_kml is None:
            if st.button("Convert", type="primary", use_container_width=True):
                with st.spinner("Converting…"):
                    try:
                        out_kml, stem = convert_nhc(
                            track.getvalue(),
                            toa.getvalue() if toa else None,
                            ww.getvalue() if ww else None,
                        )
                        st.session_state.out_kml = out_kml
                        st.session_state.out_name = f"{safe_filename(stem, 'output')}.kml"
                        st.success("Conversion complete.")
                        st.rerun()
                    except Exception as e:
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
