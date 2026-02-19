from __future__ import annotations

import csv
import datetime as dt
import io
import math
import os
import re
import sqlite3
import zipfile
from dataclasses import dataclass
from typing import Optional, Tuple, List

import requests
from flask import Flask, Response, render_template_string, request, url_for

APP_TITLE = "DWD Heizgradtage (HDD) – Anti-500 PRO"
DEFAULT_TBASE = 18.0
FALLBACK_DAYS = int(os.environ.get("FALLBACK_DAYS", "7"))

HTTP_TIMEOUT = 35
HEADERS = {"User-Agent": "dwd-hdd-app/1.0"}

DWD_BASE = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl"
DWD_STATIONS_RECENT = f"{DWD_BASE}/recent/KL_Tageswerte_Beschreibung_Stationen.txt"
DWD_STATIONS_HIST = f"{DWD_BASE}/historical/KL_Tageswerte_Beschreibung_Stationen.txt"
DWD_RECENT_ZIP = f"{DWD_BASE}/recent/tageswerte_KL_{{sid:05d}}_akt.zip"
DWD_HIST_DIR = f"{DWD_BASE}/historical/"

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

DB_PATH = os.environ.get("DWD_HDD_DB", "cache.sqlite3")

HTML = r'''
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <title>{{title}}</title>
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;margin:28px;max-width:1100px}
    input,textarea,button{padding:10px;font-size:16px;width:100%;box-sizing:border-box}
    textarea{min-height:120px}
    .row{margin:12px 0}
    .box{padding:14px;background:#f6f6f6;border-radius:10px}
    table{border-collapse:collapse;width:100%;margin-top:12px}
    th,td{border-bottom:1px solid #ddd;padding:8px;text-align:left;vertical-align:top}
    th{background:#fafafa}
    .muted{opacity:.75}
    .ok{color:#0a7a0a}
    .bad{color:#b00020}
    .pill{display:inline-block;padding:2px 8px;border-radius:999px;background:#eee;font-size:13px}
    .btnrow{display:flex;gap:10px;flex-wrap:wrap;margin-top:8px}
    .btnrow a, .btnrow button{width:auto}
  </style>
</head>
<body>
  <h1>{{title}}</h1>

  <div class="box">
    <p style="margin-top:0;">
      <b>Anti-500 PRO:</b> Keine Server-500-Fehler – stattdessen saubere Statusmeldungen pro PLZ.
      <span class="pill">Auto-Datum-Fallback bis {{fallback_days}} Tage</span>
    </p>
    <p class="muted" style="margin-bottom:0;">
      Formel: <code>HDD = max(0, Tbase − TMK)</code> &nbsp; (TMK = Tagesmitteltemperatur, DWD daily/kl).
    </p>
  </div>

  <form method="get" style="margin-top:16px;">
    <div class="row">
      <label><b>PLZ-Liste</b> <span class="muted">(eine pro Zeile oder Komma)</span></label><br>
      <textarea name="plz_list" placeholder="z.B.&#10;10115&#10;20095&#10;80331">{{plz_list}}</textarea>
      <div class="muted" style="margin-top:6px;">Tipp: Du kannst direkt aus Excel einfügen (CR/LF wird bereinigt).</div>
    </div>

    <div class="row">
      <label><b>Datum</b> (YYYY-MM-DD)</label><br>
      <input name="day" value="{{day}}" required>
      <div class="muted" style="margin-top:6px;">Wenn DWD für dieses Datum noch keine TMK hat, wird automatisch rückwärts gesucht.</div>
    </div>

    <div class="row">
      <label><b>Tbase (°C)</b></label><br>
      <input name="tbase" value="{{tbase}}" type="number" step="0.1" required>
    </div>

    <div class="btnrow">
      <button type="submit">Berechnen</button>
      {% if rows %}
        <a href="{{csv_url}}"><button type="button">CSV exportieren</button></a>
      {% endif %}
    </div>
  </form>

  {% if rows %}
    <div class="box" style="margin-top:16px;">
      <h2 style="margin-top:0;">Ergebnisse</h2>
      <table>
        <thead>
          <tr>
            <th>PLZ</th>
            <th>Koordinaten</th>
            <th>DWD-Station</th>
            <th>Datum genutzt</th>
            <th>TMK (°C)</th>
            <th>Tbase</th>
            <th>HDD</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
        {% for r in rows %}
          <tr>
            <td>{{r.plz}}</td>
            <td>{{r.lat}}, {{r.lon}}<br><span class="muted">{{r.place}}</span></td>
            <td>{{r.station_id}}<br><span class="muted">{{r.station_name}}</span></td>
            <td>{{r.used_day}}</td>
            <td>{{r.tmk}}</td>
            <td>{{r.tbase}}</td>
            <td><b>{{r.hdd}}</b></td>
            <td>{% if r.ok %}<span class="ok">OK</span>{% else %}<span class="bad">{{r.status}}</span>{% endif %}</td>
          </tr>
        {% endfor %}
        </tbody>
      </table>
    </div>
  {% endif %}
</body>
</html>
'''

@dataclass
class Station:
    sid: int
    name: str
    lat: float
    lon: float

@dataclass
class Row:
    plz: str
    lat: str
    lon: str
    place: str
    station_id: str
    station_name: str
    used_day: str
    tmk: str
    tbase: str
    hdd: str
    ok: bool
    status: str

def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("""CREATE TABLE IF NOT EXISTS plz_cache(
        plz TEXT PRIMARY KEY,
        lat REAL, lon REAL, place TEXT,
        ts INTEGER
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS blob_cache(
        key TEXT PRIMARY KEY,
        content BLOB,
        ts INTEGER
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS hist_zip_map(
        sid INTEGER PRIMARY KEY,
        zip_name TEXT,
        ts INTEGER
    )""")
    return conn

def now_ts() -> int:
    return int(dt.datetime.utcnow().timestamp())

def cache_get(key: str) -> Optional[bytes]:
    conn = db()
    row = conn.execute("SELECT content FROM blob_cache WHERE key=?", (key,)).fetchone()
    return row[0] if row else None

def cache_set(key: str, content: bytes):
    conn = db()
    conn.execute("INSERT OR REPLACE INTO blob_cache(key, content, ts) VALUES (?, ?, ?)",
                 (key, content, now_ts()))
    conn.commit()

def http_get(url: str, params: Optional[dict] = None) -> requests.Response:
    r = requests.get(url, params=params, headers=HEADERS, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r

def parse_plz_list(text: str) -> List[str]:
    raw = (text or "").replace("\r", "\n").replace("\t", "\n").replace(",", "\n")
    plz = []
    for line in raw.splitlines():
        s = "".join(c for c in line.strip() if c.isdigit())
        if s:
            plz.append(s)
    seen, out = set(), []
    for p in plz:
        if p not in seen:
            out.append(p); seen.add(p)
    return out

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def geocode_plz(plz: str) -> Tuple[float, float, str]:
    conn = db()
    row = conn.execute("SELECT lat, lon, place, ts FROM plz_cache WHERE plz=?", (plz,)).fetchone()
    if row and (now_ts() - int(row[3]) < 30*24*3600):
        return float(row[0]), float(row[1]), row[2] or ""

    params = {"format":"jsonv2", "countrycodes":"de", "postalcode":plz, "limit":1, "addressdetails":1}
    js = []
    try:
        js = http_get(NOMINATIM_URL, params=params).json()
    except Exception:
        js = []
    if not js:
        js = http_get(NOMINATIM_URL, params={"format":"jsonv2", "q": f"{plz} Deutschland", "limit":1}).json()
        if not js:
            raise ValueError("PLZ konnte nicht geocodiert werden (Nominatim).")

    item = js[0]
    lat, lon = float(item["lat"]), float(item["lon"])
    addr = item.get("address") or {}
    place = addr.get("city") or addr.get("town") or addr.get("village") or addr.get("county") or ""
    state = addr.get("state") or ""
    label = (f"{place}, {state}".strip(", ").strip()) if (place or state) else (item.get("display_name") or "")

    conn.execute("INSERT OR REPLACE INTO plz_cache(plz, lat, lon, place, ts) VALUES (?, ?, ?, ?, ?)",
                 (plz, lat, lon, label, now_ts()))
    conn.commit()
    return lat, lon, label

def load_stations() -> List[Station]:
    key = "stations_daily_kl_v1"
    cached = cache_get(key)
    if cached:
        try:
            out = []
            for line in cached.decode("utf-8").splitlines():
                sid, name, lat, lon = line.split("\t")
                out.append(Station(int(sid), name, float(lat), float(lon)))
            if out:
                return out
        except Exception:
            pass

    txt = None
    for url in (DWD_STATIONS_RECENT, DWD_STATIONS_HIST):
        try:
            txt = http_get(url).text
            break
        except Exception:
            continue
    if not txt:
        raise ValueError("DWD Stationsliste nicht verfügbar.")

    stations: List[Station] = []
    for line in txt.splitlines():
        if not line.strip() or len(line) < 120:
            continue
        sid_raw = line[0:5].strip()
        if not sid_raw.isdigit():
            continue
        try:
            sid = int(sid_raw)
            name = line[61:102].strip()
            lat = float(line[102:110].strip())
            lon = float(line[110:119].strip())
            stations.append(Station(sid, name, lat, lon))
        except Exception:
            continue

    if not stations:
        raise ValueError("Keine Stationen aus DWD Liste geparst.")

    payload = "\n".join([f"{s.sid}\t{s.name}\t{s.lat}\t{s.lon}" for s in stations]).encode("utf-8")
    cache_set(key, payload)
    return stations

def nearest_station(lat: float, lon: float, stations: List[Station]) -> Tuple[Station, float]:
    best = None
    best_d = 1e18
    for s in stations:
        d = haversine_km(lat, lon, s.lat, s.lon)
        if d < best_d:
            best_d = d
            best = s
    if best is None:
        raise ValueError("Keine Station gefunden.")
    return best, best_d

def resolve_hist_zip_name(sid: int) -> Optional[str]:
    conn = db()
    row = conn.execute("SELECT zip_name, ts FROM hist_zip_map WHERE sid=?", (sid,)).fetchone()
    if row and (now_ts() - int(row[1]) < 180*24*3600):
        return row[0]

    html = http_get(DWD_HIST_DIR).text
    m = re.search(rf"tageswerte_KL_{sid:05d}_[0-9]{{8}}_[0-9]{{8}}_hist\.zip", html)
    if not m:
        return None
    name = m.group(0)
    conn.execute("INSERT OR REPLACE INTO hist_zip_map(sid, zip_name, ts) VALUES (?, ?, ?)",
                 (sid, name, now_ts()))
    conn.commit()
    return name

def fetch_station_zip(sid: int) -> bytes:
    try:
        return http_get(DWD_RECENT_ZIP.format(sid=sid)).content
    except Exception:
        pass
    name = resolve_hist_zip_name(sid)
    if not name:
        raise ValueError("DWD ZIP nicht gefunden (recent/historical).")
    return http_get(DWD_HIST_DIR + name).content

def read_tmk_from_zip(zip_bytes: bytes, yyyymmdd: str) -> float:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        prod = next((n for n in z.namelist() if n.lower().startswith("produkt_") and n.lower().endswith(".txt")), None)
        if not prod:
            prod = next((n for n in z.namelist() if n.lower().endswith(".txt")), None)
        if not prod:
            raise ValueError("DWD ZIP: keine Daten-Datei gefunden.")
        raw = z.read(prod).decode("latin-1", errors="replace").splitlines()

    header = raw[0].split(";")
    if "MESS_DATUM" not in header or "TMK" not in header:
        raise ValueError("DWD Datei: Spalten MESS_DATUM/TMK fehlen.")
    idx_date = header.index("MESS_DATUM")
    idx_tmk = header.index("TMK")

    for line in raw[1:]:
        cols = line.split(";")
        if len(cols) <= max(idx_date, idx_tmk):
            continue
        if cols[idx_date].strip() != yyyymmdd:
            continue
        v = cols[idx_tmk].strip()
        if not v or v.startswith("-999"):
            raise ValueError("DWD: TMK nicht verfügbar.")
        return float(v)
    raise ValueError("DWD: Datum nicht im Stationsdatensatz (noch nicht publiziert?).")

def compute_one(plz: str, req_day: dt.date, tbase: float, stations: List[Station]) -> Row:
    try:
        lat, lon, place = geocode_plz(plz)
        st, _dist = nearest_station(lat, lon, stations)
        zip_bytes = fetch_station_zip(st.sid)

        last_err = None
        for back in range(0, FALLBACK_DAYS + 1):
            day = req_day - dt.timedelta(days=back)
            yyyymmdd = day.strftime("%Y%m%d")
            try:
                tmk = read_tmk_from_zip(zip_bytes, yyyymmdd)
                hdd = max(0.0, round(tbase - tmk, 2))
                return Row(plz, f"{lat:.5f}", f"{lon:.5f}", place or "—",
                           str(st.sid), st.name, day.isoformat(),
                           f"{tmk:.2f}", f"{tbase:.2f}", f"{hdd:.2f}",
                           True, "OK")
            except Exception as e:
                last_err = str(e)
                continue

        return Row(plz, f"{lat:.5f}", f"{lon:.5f}", place or "—", str(st.sid), st.name,
                   "—", "—", f"{tbase:.2f}", "—", False,
                   f"Keine DWD-TMK gefunden (Fallback {FALLBACK_DAYS} Tage). Letzter Fehler: {last_err}")
    except Exception as e:
        return Row(plz, "—", "—", "—", "—", "—", "—", "—", f"{tbase:.2f}", "—", False, str(e))

app = Flask(__name__)

@app.route("/", methods=["GET"])
def index():
    plz_text = (request.args.get("plz_list") or "").strip()
    day_str = (request.args.get("day") or (dt.date.today() - dt.timedelta(days=3)).isoformat()).strip()
    tbase_str = (request.args.get("tbase") or str(DEFAULT_TBASE)).strip().replace(",", ".")

    rows: List[Row] = []
    try:
        day = dt.date.fromisoformat(day_str)
    except Exception:
        day = dt.date.today() - dt.timedelta(days=3)
        day_str = day.isoformat()

    try:
        tbase = float(tbase_str)
    except Exception:
        tbase = DEFAULT_TBASE
        tbase_str = str(DEFAULT_TBASE)

    if plz_text:
        plzs = parse_plz_list(plz_text)
        try:
            stations = load_stations()
        except Exception as e:
            for p in plzs:
                rows.append(Row(p, "—", "—", "—", "—", "—", "—", "—", f"{tbase:.2f}", "—", False, str(e)))
        else:
            for p in plzs:
                rows.append(compute_one(p, day, tbase, stations))

    csv_url = url_for("export_csv", plz_list=plz_text, day=day_str, tbase=tbase_str) if rows else ""
    return render_template_string(HTML, title=APP_TITLE, plz_list=plz_text, day=day_str,
                                  tbase=tbase_str, rows=rows, csv_url=csv_url, fallback_days=FALLBACK_DAYS)

@app.route("/export.csv", methods=["GET"])
def export_csv():
    plz_text = (request.args.get("plz_list") or "").strip()
    day_str = (request.args.get("day") or "").strip()
    tbase_str = (request.args.get("tbase") or str(DEFAULT_TBASE)).strip().replace(",", ".")

    try:
        day = dt.date.fromisoformat(day_str)
    except Exception:
        return Response("Invalid day. Use YYYY-MM-DD.", status=400)
    try:
        tbase = float(tbase_str)
    except Exception:
        return Response("Invalid tbase.", status=400)

    plzs = parse_plz_list(plz_text)
    if not plzs:
        return Response("No PLZ.", status=400)

    stations = load_stations()
    out = io.StringIO()
    w = csv.writer(out, delimiter=";")
    w.writerow(["plz","lat","lon","place","station_id","station_name","used_day","tmk_c","tbase_c","hdd","status"])
    for p in plzs:
        r = compute_one(p, day, tbase, stations)
        w.writerow([r.plz,r.lat,r.lon,r.place,r.station_id,r.station_name,r.used_day,r.tmk,r.tbase,r.hdd,r.status])

    data = out.getvalue().encode("utf-8")
    return Response(data, mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f"attachment; filename=hdd_dwd_{day.isoformat()}.csv"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
