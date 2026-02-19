
from flask import Flask, request, render_template_string, send_file
import requests, io, csv, os, math, datetime as dt

app = Flask(__name__)

HTML = '''
<!doctype html>
<title>DWD HDD App</title>
<h2>DWD HDD App</h2>
<form method="get">
<textarea name="plz_list" rows="4" cols="20">{{plz}}</textarea><br><br>
<input name="day" value="{{day}}"><br><br>
<input name="tbase" value="{{tbase}}"><br><br>
<button type="submit">Berechnen</button>
</form>
{% if rows %}
<table border=1>
<tr><th>PLZ</th><th>Tmean</th><th>HDD</th></tr>
{% for r in rows %}
<tr><td>{{r.plz}}</td><td>{{r.tmean}}</td><td>{{r.hdd}}</td></tr>
{% endfor %}
</table>
{% endif %}
'''

def parse_plz_list(text: str):
    raw_text = (text or "").replace("\r", "\n").replace("\t", "\n").replace(",", "\n")
    out = []
    for line in raw_text.splitlines():
        s = "".join(ch for ch in line if ch.isdigit())
        if s:
            out.append(s)
    return list(dict.fromkeys(out))

def fake_tmean(plz):
    return 5.0 + (int(plz[-1]) % 5)

@app.route("/")
def index():
    plz_raw = request.args.get("plz_list", "")
    day = request.args.get("day", "")
    tbase = float(request.args.get("tbase", "18") or 18)
    plzs = parse_plz_list(plz_raw)
    rows = []
    if plzs and day:
        for p in plzs:
            tmean = fake_tmean(p)
            hdd = max(0.0, tbase - tmean)
            rows.append({"plz": p, "tmean": round(tmean,1), "hdd": round(hdd,1)})
    return render_template_string(HTML, plz=plz_raw, day=day, tbase=tbase, rows=rows)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
