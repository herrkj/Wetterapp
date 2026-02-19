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
from typing import Optional

import requests
from flask import Flask, Response, render_template_string, request, url_for

# -----------------------------
# Konfiguration (ASCII only für HTTP Header!)
# -----------------------------
APP_NAME = "DWD Heizgradtage (HDD) - PLZ-scharf"
TIMEZONE_HINT = "Europe/Berlin (DWD-Tageswerte sind tagesbezogen; Datum als YYYY-MM-DD)"

DWD_BASE = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl"
DWD_RECENT_STATIONS = f"{DWD_BASE}/recent/KL_Tageswerte_Beschreibung_Stationen.txt"
DWD_HIST_STATIONS = f"{DWD_BASE}/historical/KL_Tageswerte_Beschreibung_Stationen.txt"
DWD_RECENT_ZIP = f"{DWD_BASE}/recent/tageswerte_KL_{{sid:05d}}_akt.zip"
DWD_HIST_INDEX = f"{DWD_BASE}/historical/"

# PLZ -> Koordinaten via Nominatim (OSM) ohne API Key
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

DB_PATH = os.environ.get("DWD_HDD_DB", "cache.sqlite3")
HTTP_TIMEOUT = 25

HTML = """
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <title>{{title}}</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; margin: 28px; max-width: 1100px; }
    input, button, textarea { padding: 10px; font-size: 16px; width: 100%; box-sizing: border-box; }
    textarea { min-height: 120px; }
    .row { margin: 12px 0; }
    .box { padding: 14px; background: #f6f6f6; border-radius: 10px; }
    code { background: #eee; padding: 2px 6px; border-radius: 6px; }
    .muted { opacity: 0.75; }
    table { border-collapse: collapse; width: 100%; margin-top: 12px; }
    th, td { border-bottom: 1px solid #ddd; padding: 8px; text-align: left; vertical-align: top; }
    th { background: #fafafa; }
    .btnrow { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 8px; }
    .btnrow a, .btnrow button { width: auto; text-decoration: none; }
    .pill { display:inline-block; padding: 2px 8px; border-radius: 999px; background:#eee; font-size: 13px; }
    .ok { color: #0a7a0a; }
    .bad { color: #b00020; }
    .small { font-size: 13px; }
  </style>
</head>
<body>
  <h1>{{title}}</h1>

  <div class="box">
    <p style="margin-top:0;">
      <b>Ziel:</b> tägliche <b>Heizgradtage (HDD)</b> je deutscher PLZ.
      <span class="pill">PLZ → Koordinaten → nächste DWD-Station → TMK → HDD</span>
    </p>
    <p class="muted" style="margin-bottom:0;">
      Berechnung: <code>HDD = max(0, Tbase − Tmean)</code><br>
      DWD: Datensatz „daily/kl“, Spalte <b>TMK</b> (Tagesmitteltemperatur, 2m). Datum: <b>YYYY-MM-DD</b>.
    </p>
  </div>

  <form method="get" style="margin-top:16px;">
    <div class="row">
      <label><b>PLZ-Liste</b> <span class="muted">(eine pro Zeile oder Komma)</span></label><br>
      <textarea name="plz_list" placeholder="z.B.&#10;10115&#10;20095&#10;80331">{{plz_list}}</textarea>
      <div class="muted small">
        Hinweis: PLZ→Koordinaten via Nominatim (OpenStreetMap). Für Produktivbetrieb besser lokale PLZ-Koordinaten-Tabelle.
      </div>
    </div>

    <div class="row">
      <label><b>Datum</b> (YYYY-MM-DD)</label><br>
      <input name="day" value="{{day}}" required>
      <div class="muted small">{{tz_hint}}</div>
      <div class="muted small">Tipp: Wenn du Fehler bekommst, nimm ein Datum 2–5 Tage in der Vergangenheit.</div>
    </div>

    <div class="row">
      <label><b>Basistemperatur Tbase (°C)</b></label><br>
      <input name="tbase" value="{{tbase}}" type="number" step="0.1" required>
      <div class="muted small">Oft 18,0 °C (je nach Definition/Standard).</div>
    </div>

    <div class="btnrow">
      <button type="submit">Berechnen</button>
      {% if rows and not fatal_error %}
        <a href="{{export_url}}"><button type="button">CSV exportieren</button></a>
      {% endif %}
    </div>
  </form>

  {% if fatal_error %}
    <div class="box" style="margin-top:16px;">
      <b class="bad">Fehler:</b> {{fatal_error}}
    </div>
  {% endif %}

  {% if rows and not fatal_error %}
    <div class="box" style="margin-top:16px;">
      <h2 style="margin-top:0;">Ergebnisse</h2>
      <p class="muted small" style="margin-top:0;">
        DWD-Quelle: CDC OpenData daily/kl (TMK). Stationen per Luftlinie (Haversine).
      </p>

      <table>
        <thead>
          <tr>
            <th>PLZ</th>
            <th>Koordinaten</th>
            <th>DWD Station</th>
            <th>Distanz</th>
            <th>Datum</th>
            <th>TMK / Tmean (°C)</th>
            <th>Tbase (°C)</th>
            <th>HDD</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
        {% for r in rows %}
          <tr>
            <td>{{r.plz}}</td>
            <td>{{r.lat}}, {{r.lon}}<br><span class="muted small">{{r.place}}</span></td>
            <td>{{r.station_id}}<br><span class="muted small">{{r.station_name}}</span></td>
            <td>{{r.dist_km}} km</td>
            <td>{{r.day}}</td>
            <td>{{r.tmean}}</td>
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
"""

@dataclass
class Station:
    sid: int
    von: int
    bis: int
    lat: float
    lon: float
    name: str

@dataclass
class RowResult:
    plz: str
    lat: str
    lon: str
    place: str
    station_id: str
    station_name: str
    dist_km: str
    day: str
    tmean: str
    tbase: str
    hdd: str
    ok: bool
    status: str

def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS plz_cache(
            plz TEXT PRIMARY KEY,
            lat REAL,
            lon REAL,
            place TEXT,
            ts INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS blob_cache(
            key TEXT PRIMARY KEY,
            content BLOB,
            ts INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hist_zip_map(
            sid INTEGER PRIMARY KEY,
            zip_name TEXT,
            ts INTEGER
        )
    """)
    return conn

def cache_get(conn: sqlite3.Connection, key: str) -> Optional[bytes]:
    cur = conn.execute("SELECT content FROM blob_cache WHERE key=?", (key,))
    row = cur.fetchone()
    return row[0] if row else None

def cache_set(conn: sqlite3.Connection, key: str, content: bytes):
    conn.execute("INSERT OR REPLACE INTO blob_cache(key, content, ts) VALUES (?, ?, ?)",
                 (key, content, int(dt.datetime.utcnow().timestamp())))
    conn.commit()

def parse_plz_list(text: str) -> list[str]:
    # Robust gegen Copy&Paste aus Excel/Outlook/URL Parametern (CRLF, Tabs, Spaces)
    raw_text = (text or "").replace("", "
").replace("	", "
").replace(",", "
")
    raw = raw_text.splitlines()
    plz = []
    for line in raw:
        s = "".join(ch for ch in line.strip() if ch.isdigit())
        if s:
            plz.append(s)
    seen, out = set(), []
    for p in plz:
        if p not in seen:
            out.append(p); seen.add(p)
    return out


def ymd_to_int(d: dt.date) -> int:
    return d.year * 10000 + d.month * 100 + d.day

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2*R*math.asin(math.sqrt(a))

def http_get(url: str, params: Optional[dict] = None) -> requests.Response:
    headers = {"User-Agent": "dwd-hdd-app/1.0"}  # ASCII only
    r = requests.get(url, params=params, headers=headers, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r

def safe_int_yyyymmdd(s: str, default: int) -> int:
    s = (s or "").strip()
    if s.isdigit() and len(s) == 8:
        return int(s)
    return default

def geocode_plz(plz: str) -> tuple[float, float, str]:
    conn = db()
    cur = conn.execute("SELECT lat, lon, place, ts FROM plz_cache WHERE plz=?", (plz,))
    row = cur.fetchone()
    now = int(dt.datetime.utcnow().timestamp())
    if row and (now - int(row[3])) < 30*24*3600:
        return float(row[0]), float(row[1]), row[2] or ""

    params = {"format":"jsonv2","countrycodes":"de","postalcode":plz,"addressdetails":1,"limit":1}
    r = http_get(NOMINATIM_URL, params=params)
    data = r.json()
    if not data:
        params2 = {"format":"jsonv2","q":f"{plz} Deutschland","limit":1}
        data = http_get(NOMINATIM_URL, params=params2).json()
        if not data:
            raise ValueError("PLZ konnte nicht geocodiert werden (keine Koordinaten).")

    item = data[0]
    lat, lon = float(item["lat"]), float(item["lon"])
    addr = item.get("address") or {}
    place = addr.get("city") or addr.get("town") or addr.get("village") or addr.get("county") or ""
    state = addr.get("state") or ""
    label = (f"{place}, {state}".strip(", ").strip()) if (place or state) else (item.get("display_name") or "")
    conn.execute("INSERT OR REPLACE INTO plz_cache(plz, lat, lon, place, ts) VALUES (?,?,?,?,?)",
                 (plz, lat, lon, label, now))
    conn.commit()
    return lat, lon, label

def load_dwd_stations() -> list[Station]:
    conn = db()
    key = "dwd_stations_daily_kl_v2"
    cached = cache_get(conn, key)
    if cached:
        try:
            out=[]
            for line in cached.decode("utf-8").splitlines():
                sid, von, bis, lat, lon, name = line.split("\t")
                out.append(Station(int(sid), int(von), int(bis), float(lat), float(lon), name))
            return out
        except Exception:
            pass

    try:
        txt = http_get(DWD_RECENT_STATIONS).text
    except Exception:
        txt = http_get(DWD_HIST_STATIONS).text

    stations=[]
    for line in txt.splitlines()[1:]:
        if not line.strip():
            continue
        parts=line.split()
        if len(parts) < 7:
            continue
        sid_raw = parts[0].strip()
        if not sid_raw.isdigit():
            continue
        sid=int(sid_raw)
        von=safe_int_yyyymmdd(parts[1], 0)
        bis=safe_int_yyyymmdd(parts[2], 99991231)
        lat=float(parts[4]); lon=float(parts[5]); name=parts[6]
        stations.append(Station(sid, von, bis, lat, lon, name))

    payload="\n".join([f"{s.sid}\t{s.von}\t{s.bis}\t{s.lat}\t{s.lon}\t{s.name}" for s in stations]).encode("utf-8")
    cache_set(conn, key, payload)
    return stations

def nearest_station_for_date(lat: float, lon: float, day_int: int, stations: list[Station]) -> tuple[Station, float]:
    best=None; best_d=1e18
    for s in stations:
        if s.von and day_int < s.von: 
            continue
        if s.bis and day_int > s.bis: 
            continue
        d=haversine_km(lat, lon, s.lat, s.lon)
        if d < best_d:
            best_d=d; best=s
    if not best:
        for s in stations:
            d=haversine_km(lat, lon, s.lat, s.lon)
            if d < best_d:
                best_d=d; best=s
    if not best:
        raise ValueError("Keine DWD-Station gefunden.")
    return best, best_d

def try_fetch_recent_zip(sid: int) -> Optional[bytes]:
    try:
        return http_get(DWD_RECENT_ZIP.format(sid=sid)).content
    except Exception:
        return None

def resolve_historical_zip_name(sid: int) -> Optional[str]:
    conn=db()
    row=conn.execute("SELECT zip_name, ts FROM hist_zip_map WHERE sid=?", (sid,)).fetchone()
    now=int(dt.datetime.utcnow().timestamp())
    if row and (now-int(row[1])) < 90*24*3600:
        return row[0]
    html=http_get(DWD_HIST_INDEX).text
    m=re.search(rf"tageswerte_KL_{sid:05d}_[0-9]{{8}}_[0-9]{{8}}_hist\.zip", html)
    if not m:
        return None
    name=m.group(0)
    conn.execute("INSERT OR REPLACE INTO hist_zip_map(sid, zip_name, ts) VALUES (?,?,?)", (sid, name, now))
    conn.commit()
    return name

def try_fetch_historical_zip(sid: int) -> Optional[bytes]:
    name=resolve_historical_zip_name(sid)
    if not name:
        return None
    try:
        return http_get(f"{DWD_HIST_INDEX}{name}").content
    except Exception:
        return None

def read_tmk_from_zip(zip_bytes: bytes, yyyymmdd: int) -> float:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        prod =_toggle = None
        prod = next((n for n in z.namelist() if n.lower().startswith("produkt_") and n.lower().endswith(".txt")), None)
        if not prod:
            raise ValueError("ZIP enthält keine produkt_*.txt Datei.")
        raw=z.read(prod)
        lines=raw.decode("latin-1", errors="replace").splitlines()

    header=lines[0].split(";")
    if "MESS_DATUM" not in header or "TMK" not in header:
        raise ValueError("Spalten MESS_DATUM/TMK nicht gefunden.")
    idx_date=header.index("MESS_DATUM"); idx_tmk=header.index("TMK")
    target=str(yyyymmdd)

    for line in lines[1:]:
        cols=line.split(";")
        if len(cols) <= max(idx_date, idx_tmk):
            continue
        if cols[idx_date].strip() != target:
            continue
        v=cols[idx_tmk].strip()
        if (not v) or v.startswith("-999") or v.startswith("-"):
            raise ValueError("DWD: TMK nicht verfügbar für dieses Datum.")
        return float(v)

    raise ValueError("DWD: kein Datensatz für dieses Datum (Verzögerung/Station offline).")

def compute_hdd(tmean: float, tbase: float) -> float:
    return max(0.0, round(tbase - tmean, 2))

app = Flask(__name__)

def compute_for_plz(plz: str, day: dt.date, tbase: float, stations: list[Station]) -> RowResult:
    try:
        lat, lon, place = geocode_plz(plz)
        day_int = ymd_to_int(day)
        st, dist = nearest_station_for_date(lat, lon, day_int, stations)

        zip_bytes = try_fetch_recent_zip(st.sid); source="recent"
        if not zip_bytes:
            zip_bytes = try_fetch_historical_zip(st.sid); source="historical"
        if not zip_bytes:
            raise ValueError("DWD ZIP nicht abrufbar (recent/historical).")

        tmean = read_tmk_from_zip(zip_bytes, day_int)
        hdd = compute_hdd(tmean, tbase)

        return RowResult(plz, f"{lat:.5f}", f"{lon:.5f}", place or "—",
                         str(st.sid), f"{st.name} ({source})", f"{dist:.1f}",
                         day.isoformat(), f"{tmean:.2f}", f"{tbase:.2f}", f"{hdd:.2f}",
                         True, "OK")
    except Exception as e:
        return RowResult(plz, "—", "—", "—", "—", "—", "—",
                         day.isoformat(), "—", f"{tbase:.2f}", "—",
                         False, str(e))

@app.route("/", methods=["GET"])
def index():
    plz_list_text=(request.args.get("plz_list") or "").strip()
    default_day=(dt.date.today()-dt.timedelta(days=3)).isoformat()
    day_str=(request.args.get("day") or default_day).strip()
    tbase_str=(request.args.get("tbase") or "18.0").strip()

    rows=[]
    fatal_error=None

    if plz_list_text:
        try:
            day=dt.date.fromisoformat(day_str)
        except Exception:
            fatal_error="Datum muss im Format YYYY-MM-DD sein."
            day=dt.date.today()

        try:
            tbase=float(tbase_str.replace(",", "."))
        except Exception:
            fatal_error=fatal_error or "Tbase muss eine Zahl sein (z.B. 18.0)."
            tbase=18.0

        if not fatal_error:
            plz_list=parse_plz_list(plz_list_text)
            if not plz_list:
                fatal_error="Bitte mindestens eine PLZ angeben."
            else:
                stations=load_dwd_stations()
                for plz in plz_list:
                    rows.append(compute_for_plz(plz, day, tbase, stations))

    export_url=None
    if rows and not fatal_error:
        export_url=url_for("export_csv", plz_list=plz_list_text, day=day_str, tbase=tbase_str)

    return render_template_string(HTML, title=APP_NAME, tz_hint=TIMEZONE_HINT,
                                  plz_list=plz_list_text, day=day_str, tbase=tbase_str,
                                  rows=rows, fatal_error=fatal_error, export_url=export_url)

@app.route("/export.csv", methods=["GET"])
def export_csv():
    plz_list_text=(request.args.get("plz_list") or "").strip()
    day_str=(request.args.get("day") or "").strip()
    tbase_str=(request.args.get("tbase") or "18.0").strip()

    try:
        day=dt.date.fromisoformat(day_str)
    except Exception:
        return Response("Invalid day. Use YYYY-MM-DD.", status=400)

    try:
        tbase=float(tbase_str.replace(",", "."))
    except Exception:
        return Response("Invalid tbase.", status=400)

    plz_list=parse_plz_list(plz_list_text)
    if not plz_list:
        return Response("No PLZ.", status=400)

    stations=load_dwd_stations()

    output=io.StringIO()
    w=csv.writer(output, delimiter=";")
    w.writerow(["plz","lat","lon","place","station_id","station_name","dist_km","day","tmk_tmean_c","tbase_c","hdd","status"])
    for plz in plz_list:
        r=compute_for_plz(plz, day, tbase, stations)
        w.writerow([r.plz,r.lat,r.lon,r.place,r.station_id,r.station_name,r.dist_km,r.day,r.tmean,r.tbase,r.hdd,r.status])

    data=output.getvalue().encode("utf-8")
    return Response(data, mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f"attachment; filename=hdd_dwd_{day.isoformat()}.csv"})

if __name__ == "__main__":
    port=int(os.environ.get("PORT","5000"))
    app.run(host="0.0.0.0", port=port)
