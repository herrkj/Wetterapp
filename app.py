import os
import io
import re
import csv
import math
import zipfile
import sqlite3
import datetime as dt
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict

import requests
from flask import Flask, request, render_template, Response

PORT = int(os.environ.get("PORT", "10000"))
USER_AGENT = os.environ.get("USER_AGENT", "dwd-hdd-app/1.0 (contact: you@example.com)")
FALLBACK_DAYS = int(os.environ.get("FALLBACK_DAYS", "14"))
REQ_TIMEOUT = float(os.environ.get("REQ_TIMEOUT", "20"))
CACHE_DB = os.environ.get("CACHE_DB", "cache.sqlite3")

DWD_BASE = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl"
DWD_RECENT = f"{DWD_BASE}/recent"
DWD_HIST = f"{DWD_BASE}/historical"
DWD_STATION_LIST = f"{DWD_RECENT}/KL_Tageswerte_Beschreibung_Stationen.txt"

def http_get(url: str) -> bytes:
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.content

def ensure_cache():
    con = sqlite3.connect(CACHE_DB)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS plz_cache(
            plz TEXT PRIMARY KEY,
            lat REAL,
            lon REAL,
            display TEXT,
            updated_at TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS blob_cache(
            key TEXT PRIMARY KEY,
            payload BLOB,
            updated_at TEXT
        )
    """)
    con.commit()
    con.close()

def blob_get(key: str) -> Optional[bytes]:
    con = sqlite3.connect(CACHE_DB)
    cur = con.cursor()
    cur.execute("SELECT payload FROM blob_cache WHERE key=?", (key,))
    row = cur.fetchone()
    con.close()
    return row[0] if row else None

def blob_set(key: str, payload: bytes):
    con = sqlite3.connect(CACHE_DB)
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO blob_cache(key,payload,updated_at) VALUES(?,?,?)",
        (key, payload, dt.datetime.utcnow().isoformat()),
    )
    con.commit()
    con.close()

def plz_cache_get(plz: str) -> Optional[Tuple[float, float, str]]:
    con = sqlite3.connect(CACHE_DB)
    cur = con.cursor()
    cur.execute("SELECT lat,lon,display FROM plz_cache WHERE plz=?", (plz,))
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    return float(row[0]), float(row[1]), row[2] or ""

def plz_cache_set(plz: str, lat: float, lon: float, display: str):
    con = sqlite3.connect(CACHE_DB)
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO plz_cache(plz,lat,lon,display,updated_at) VALUES(?,?,?,?,?)",
        (plz, lat, lon, display, dt.datetime.utcnow().isoformat()),
    )
    con.commit()
    con.close()

def parse_plz_list(text: str) -> List[str]:
    raw = (text or "").replace("\r", "\n").replace(",", "\n")
    out = []
    for line in raw.split("\n"):
        digits = "".join(ch for ch in line.strip() if ch.isdigit())
        if digits:
            out.append(digits)
    seen = set()
    uniq = []
    for p in out:
        if p not in seen:
            uniq.append(p); seen.add(p)
    return uniq

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dl/2)**2
    return 2*R*math.asin(math.sqrt(a))

@dataclass
class Station:
    id: int
    from_date: str
    to_date: str
    height: int
    lat: float
    lon: float
    name: str
    state: str

def parse_station_list_fixed_width(text: str) -> List[Station]:
    stations: List[Station] = []
    for ln in text.splitlines():
        if not ln.strip():
            continue
        if "Stations_id" in ln or "Stations_ID" in ln or ln.strip().startswith("-----"):
            continue
        try:
            sid = int(ln[0:5].strip())
            from_d = ln[6:15].strip()
            to_d = ln[16:25].strip()
            height = int((ln[26:31].strip() or "0"))
            lat = float(ln[32:41].strip().replace(",", "."))
            lon = float(ln[42:51].strip().replace(",", "."))
            name = ln[52:101].strip()
            state = ln[102:].strip() if len(ln) > 102 else ""
            stations.append(Station(sid, from_d, to_d, height, lat, lon, name, state))
        except Exception:
            continue
    return stations

def geocode_plz_nominatim(plz: str) -> Tuple[float, float, str]:
    cached = plz_cache_get(plz)
    if cached:
        return cached
    url = "https://nominatim.openstreetmap.org/search"
    params = {"format": "json", "country": "Germany", "postalcode": plz, "limit": 1}
    r = requests.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    js = r.json()
    if not js:
        raise ValueError("PLZ nicht gefunden (Nominatim)")
    lat = float(js[0]["lat"]); lon = float(js[0]["lon"])
    display = js[0].get("display_name", "")
    plz_cache_set(plz, lat, lon, display)
    return lat, lon, display

def load_stations() -> List[Station]:
    key = "stations_list_v1"
    cached = blob_get(key)
    if cached:
        text = cached.decode("latin1", errors="replace")
        st = parse_station_list_fixed_width(text)
        if st:
            return st
    content = http_get(DWD_STATION_LIST)
    text = content.decode("latin1", errors="replace")
    st = parse_station_list_fixed_width(text)
    if not st:
        if "<html" in text.lower():
            raise ValueError("DWD Stationen-Liste: HTML statt TXT erhalten.")
        raise ValueError("Keine Stationen aus DWD Liste geparst.")
    blob_set(key, content)
    return st

def nearest_station(lat: float, lon: float, stations: List[Station]) -> Station:
    best = None
    best_d = 1e18
    for s in stations:
        d = haversine_km(lat, lon, s.lat, s.lon)
        if d < best_d:
            best_d = d
            best = s
    return best

def find_station_zip_url(station_id: int) -> Optional[str]:
    sid = f"{station_id:05d}"
    recent = f"{DWD_RECENT}/tageswerte_KL_{sid}_akt.zip"
    try:
        r = requests.head(recent, headers={"User-Agent": USER_AGENT}, timeout=REQ_TIMEOUT, allow_redirects=True)
        if r.status_code == 200:
            return recent
    except Exception:
        pass
    hist = f"{DWD_HIST}/tageswerte_KL_{sid}_hist.zip"
    try:
        r = requests.head(hist, headers={"User-Agent": USER_AGENT}, timeout=REQ_TIMEOUT, allow_redirects=True)
        if r.status_code == 200:
            return hist
    except Exception:
        pass
    return None

def parse_dwd_daily_zip(zip_bytes: bytes) -> List[Dict[str, str]]:
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    members = [n for n in zf.namelist() if n.lower().endswith(".txt") or n.lower().endswith(".csv")]
    if not members:
        raise ValueError("DWD ZIP: keine CSV/TXT Datei gefunden")
    members.sort(key=lambda n: (0 if "produkt_klima_tag" in n.lower() else 1, len(n)))
    name = members[0]
    raw = zf.read(name)
    text = raw.decode("latin1", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    rows = []
    for row in reader:
        norm = {(k or "").strip().upper(): (v or "").strip() for k, v in (row or {}).items()}
        if any(norm.values()):
            rows.append(norm)
    return rows

def safe_float(x: str) -> Optional[float]:
    s = (x or "").strip()
    if not s:
        return None
    s = s.replace(",", ".")
    try:
        val = float(s)
    except Exception:
        return None
    if abs(val) > 100:
        val = val / 10.0
    return val

def find_tmk_for_day(rows: List[Dict[str, str]], target: dt.date, fallback_days: int) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    if not rows:
        return None, None, "Leere DWD-Datei"
    date_key = None
    for cand in ["MESS_DATUM", "MESS_DATUM_BEGINN", "DATE", "DATUM"]:
        if cand in rows[0]:
            date_key = cand
            break
    tmk_key = None
    for cand in ["TMK", "TMEAN", "TG"]:
        if cand in rows[0]:
            tmk_key = cand
            break
    if not date_key or not tmk_key:
        raise ValueError("DWD Datei: Spalten MESS_DATUM/TMK fehlen.")
    def norm_date(s: str) -> Optional[str]:
        s = (s or "").strip()
        if re.fullmatch(r"\d{8}", s):
            return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return s
        return None
    by_date = {}
    for r in rows:
        ds = norm_date(r.get(date_key, ""))
        if ds:
            by_date[ds] = r
    for back in range(0, fallback_days + 1):
        d = target - dt.timedelta(days=back)
        ds = d.isoformat()
        r = by_date.get(ds)
        if r:
            tmk = safe_float(r.get(tmk_key, ""))
            if tmk is None or tmk <= -900:
                continue
            return tmk, ds, None
    return None, None, "Keine Daten im Zeitraum"

app = Flask(__name__)
ensure_cache()

@app.route("/", methods=["GET"])
def index():
    plz_text = request.args.get("plz_list", "")
    day_str = request.args.get("day", "")
    tbase_str = request.args.get("tbase", "18")
    if not day_str:
        day_str = (dt.date.today() - dt.timedelta(days=5)).isoformat()

    try:
        target_day = dt.date.fromisoformat(day_str)
    except Exception:
        target_day = None

    try:
        tbase = float(str(tbase_str).replace(",", "."))
    except Exception:
        tbase = 18.0

    plzs = parse_plz_list(plz_text)
    rows_out = []

    if request.args and plzs:
        if target_day is None:
            for plz in plzs:
                rows_out.append({"plz": plz, "coords":"—","station":"—","used_day":"—","tmk":"—","tbase":f"{tbase:.2f}","hdd":"—","status":"Ungültiges Datum. Bitte YYYY-MM-DD."})
        elif target_day > dt.date.today():
            for plz in plzs:
                rows_out.append({"plz": plz, "coords":"—","station":"—","used_day":"—","tmk":"—","tbase":f"{tbase:.2f}","hdd":"—","status":"Datum liegt in der Zukunft – keine DWD-Daten."})
        else:
            try:
                stations = load_stations()
            except Exception as e:
                for plz in plzs:
                    rows_out.append({"plz": plz, "coords":"—","station":"—","used_day":"—","tmk":"—","tbase":f"{tbase:.2f}","hdd":"—","status":f"Stationen-Fehler: {e}"})
                return render_template("index.html", plz_list=plz_text, day=day_str, tbase=tbase, rows=rows_out)

            for plz in plzs:
                try:
                    lat, lon, _ = geocode_plz_nominatim(plz)
                    st = nearest_station(lat, lon, stations)
                    url = find_station_zip_url(st.id)
                    if not url:
                        raise ValueError("DWD ZIP für Station nicht gefunden (recent/historical).")
                    key = f"zip:{url}"
                    zbytes = blob_get(key)
                    if not zbytes:
                        zbytes = http_get(url)
                        blob_set(key, zbytes)
                    dwd_rows = parse_dwd_daily_zip(zbytes)
                    tmk, used_day, err = find_tmk_for_day(dwd_rows, target_day, FALLBACK_DAYS)
                    if tmk is None:
                        rows_out.append({"plz": plz, "coords":f"{lat:.5f}, {lon:.5f}", "station":f"{st.id} – {st.name}", "used_day":"—","tmk":"—","tbase":f"{tbase:.2f}","hdd":"—","status":f"Keine DWD-TMK gefunden (Fallback {FALLBACK_DAYS} Tage). Letzter Fehler: {err}"})
                    else:
                        hdd = max(0.0, tbase - float(tmk))
                        rows_out.append({"plz": plz, "coords":f"{lat:.5f}, {lon:.5f}", "station":f"{st.id} – {st.name}", "used_day":used_day,"tmk":f"{tmk:.1f}","tbase":f"{tbase:.2f}","hdd":f"{hdd:.1f}","status":"OK"})
                except Exception as e:
                    rows_out.append({"plz": plz, "coords":"—","station":"—","used_day":"—","tmk":"—","tbase":f"{tbase:.2f}","hdd":"—","status":f"Fehler: {e}"})

    return render_template("index.html", plz_list=plz_text, day=day_str, tbase=tbase, rows=rows_out)

@app.route("/export.csv", methods=["GET"])
def export_csv():
    plz_text = request.args.get("plz_list", "")
    day_str = request.args.get("day", (dt.date.today() - dt.timedelta(days=5)).isoformat())
    tbase_str = request.args.get("tbase", "18")
    plzs = parse_plz_list(plz_text)
    try:
        target_day = dt.date.fromisoformat(day_str)
    except Exception:
        target_day = None
    try:
        tbase = float(str(tbase_str).replace(",", "."))
    except Exception:
        tbase = 18.0

    output = io.StringIO()
    w = csv.writer(output, delimiter=";")
    w.writerow(["PLZ","Koordinaten","DWD-Station","Datum genutzt","TMK","Tbase","HDD","Status"])

    if not plzs:
        return Response(output.getvalue(), mimetype="text/csv", headers={"Content-Disposition":"attachment; filename=hdd_export.csv"})

    try:
        stations = load_stations()
    except Exception as e:
        for plz in plzs:
            w.writerow([plz,"—","—","—","—",f"{tbase:.2f}","—",f"Stationen-Fehler: {e}"])
        return Response(output.getvalue(), mimetype="text/csv", headers={"Content-Disposition":"attachment; filename=hdd_export.csv"})

    for plz in plzs:
        try:
            if target_day is None:
                raise ValueError("Ungültiges Datum")
            if target_day > dt.date.today():
                raise ValueError("Datum liegt in der Zukunft")
            lat, lon, _ = geocode_plz_nominatim(plz)
            st = nearest_station(lat, lon, stations)
            url = find_station_zip_url(st.id)
            if not url:
                raise ValueError("DWD ZIP nicht gefunden")
            key = f"zip:{url}"
            zbytes = blob_get(key)
            if not zbytes:
                zbytes = http_get(url)
                blob_set(key, zbytes)
            dwd_rows = parse_dwd_daily_zip(zbytes)
            tmk, used_day, err = find_tmk_for_day(dwd_rows, target_day, FALLBACK_DAYS)
            if tmk is None:
                w.writerow([plz,f"{lat:.5f}, {lon:.5f}",f"{st.id} – {st.name}","—","—",f"{tbase:.2f}","—",f"Keine DWD-TMK. {err}"])
            else:
                hdd = max(0.0, tbase - float(tmk))
                w.writerow([plz,f"{lat:.5f}, {lon:.5f}",f"{st.id} – {st.name}",used_day,f"{tmk:.1f}",f"{tbase:.2f}",f"{hdd:.1f}","OK"])
        except Exception as e:
            w.writerow([plz,"—","—","—","—",f"{tbase:.2f}","—",f"Fehler: {e}"])

    return Response(output.getvalue(), mimetype="text/csv", headers={"Content-Disposition":"attachment; filename=hdd_export.csv"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
