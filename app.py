
from flask import Flask, request, render_template_string
import requests, math, os

app = Flask(__name__)

BASE_TEMP_DEFAULT = 18.0
DWD_STATIONS_URL = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/recent/KL_Tageswerte_Beschreibung_Stationen.txt"
DWD_DATA_BASE = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/recent/"
HEADERS = {"User-Agent": "dwd-hdd-app/1.0 (contact: demo@example.com)"}

HTML = '''
<!doctype html>
<title>DWD HDD App</title>
<h2>DWD HDD App (real DWD)</h2>
<form method="get">
<textarea name="plz_list" rows="4" cols="20">{{plz}}</textarea><br><br>
<input name="day" value="{{day}}" placeholder="YYYY-MM-DD"><br><br>
<input name="tbase" value="{{tbase}}"><br><br>
<button type="submit">Berechnen</button>
</form>

{% if rows %}
<table border=1>
<tr><th>PLZ</th><th>Station</th><th>Tmean (TMK)</th><th>HDD</th></tr>
{% for r in rows %}
<tr><td>{{r.plz}}</td><td>{{r.station}}</td><td>{{r.tmean}}</td><td>{{r.hdd}}</td></tr>
{% endfor %}
</table>
{% endif %}
'''

def parse_plz_list(text):
    raw = (text or "").replace("\r", "\n").replace("\t", "\n").replace(",", "\n")
    plz = []
    for line in raw.splitlines():
        s = "".join(c for c in line if c.isdigit())
        if s:
            plz.append(s)
    return list(dict.fromkeys(plz))

def load_stations():
    r = requests.get(DWD_STATIONS_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    stations = []
    for line in r.text.splitlines():
        if not line or not line[0].isdigit():
            continue
        sid = line[0:5].strip()
        name = line[61:102].strip()
        lat = float(line[102:110])
        lon = float(line[110:119])
        stations.append((sid, name, lat, lon))
    return stations

def dist(a,b,c,d):
    return math.sqrt((a-c)**2 + (b-d)**2)

def plz_to_coords(plz):
    url = "https://nominatim.openstreetmap.org/search"
    r = requests.get(url, params={
        "postalcode": plz,
        "country": "Germany",
        "format": "json"
    }, headers=HEADERS, timeout=30)
    r.raise_for_status()
    js = r.json()
    if not js:
        raise ValueError("PLZ not found")
    return float(js[0]["lat"]), float(js[0]["lon"])

def nearest_station(lat, lon, stations):
    return min(stations, key=lambda s: dist(lat, lon, s[2], s[3]))

def load_tmk(station_id, day):
    import zipfile, io
    fname = f"tageswerte_KL_{station_id}_akt.zip"
    url = DWD_DATA_BASE + fname
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        csv_name = z.namelist()[0]
        text = z.read(csv_name).decode("latin-1")
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        parts = line.split(";")
        if parts[1] == day.replace("-", ""):
            tmk = parts[13]
            if tmk == "-999":
                raise ValueError("No TMK for date")
            return float(tmk)
    raise ValueError("Date not found")

@app.route("/")
def index():
    plz_raw = request.args.get("plz_list","")
    day = request.args.get("day","")
    tbase = float(request.args.get("tbase", BASE_TEMP_DEFAULT))
    rows = []

    if plz_raw and day:
        stations = load_stations()
        for plz in parse_plz_list(plz_raw):
            try:
                lat, lon = plz_to_coords(plz)
                sid, sname, _, _ = nearest_station(lat, lon, stations)
                tmean = load_tmk(sid, day)
                hdd = max(0.0, tbase - tmean)
                rows.append({
                    "plz": plz,
                    "station": sname,
                    "tmean": round(tmean,1),
                    "hdd": round(hdd,1)
                })
            except Exception as e:
                rows.append({
                    "plz": plz,
                    "station": "ERROR",
                    "tmean": str(e),
                    "hdd": "-"
                })

    return render_template_string(HTML, plz=plz_raw, day=day, tbase=tbase, rows=rows)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
