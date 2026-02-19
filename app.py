
# FIXED VERSION – DWD HDD APP
# (see comments in chat; this version skips '--------' station IDs safely)

from __future__ import annotations
import csv, datetime as dt, io, math, os, re, sqlite3, zipfile
from dataclasses import dataclass
from typing import Optional
import requests
from flask import Flask, Response, render_template_string, request, url_for

APP_NAME = "DWD Heizgradtage (HDD) – PLZ-scharf"
TIMEZONE_HINT = "Europe/Berlin (DWD Tageswerte, daily/kl)"
HTTP_TIMEOUT = 25

DWD_BASE = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl"
DWD_STATIONS = f"{DWD_BASE}/recent/KL_Tageswerte_Beschreibung_Stationen.txt"
DWD_RECENT_ZIP = f"{DWD_BASE}/recent/tageswerte_KL_{{sid:05d}}_akt.zip"

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
DB_PATH = "cache.sqlite3"

HTML = "<h1>DWD HDD App läuft ✅</h1><p>Wenn du das siehst, ist der Server gestartet.</p>"

@dataclass
class Station:
    sid: int
    lat: float
    lon: float
    name: str

def http_get(url, params=None):
    r = requests.get(url, params=params, headers={"User-Agent": "dwd-hdd-app"}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r

def load_dwd_stations():
    txt = http_get(DWD_STATIONS).text
    stations = []
    for line in txt.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 6:
            continue
        sid_raw = parts[0].strip()
        if not sid_raw.isdigit():
            continue   # <<< FIX
        sid = int(sid_raw)
        lat = float(parts[4])
        lon = float(parts[5])
        name = parts[6] if len(parts) > 6 else ""
        stations.append(Station(sid, lat, lon, name))
    return stations

app = Flask(__name__)

@app.route("/")
def index():
    return render_template_string(HTML)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
