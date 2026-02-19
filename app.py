
from flask import Flask, request, render_template_string
import datetime as dt
import os

app = Flask(__name__)

HTML = """
<!doctype html>
<title>DWD HDD – Anti-500</title>
<h2>DWD HDD – Anti-500 (stabile Version)</h2>

<form method="get">
PLZ:<br>
<textarea name="plz_list" rows="3">{{plz}}</textarea><br><br>
Datum (YYYY-MM-DD):<br>
<input name="day" value="{{day}}"><br><br>
Tbase:<br>
<input name="tbase" value="{{tbase}}"><br><br>
<button type="submit">Berechnen</button>
</form>

{% if rows %}
<hr>
<table border="1" cellpadding="6">
<tr><th>PLZ</th><th>Datum genutzt</th><th>TMK</th><th>HDD</th><th>Status</th></tr>
{% for r in rows %}
<tr>
<td>{{r.plz}}</td>
<td>{{r.day}}</td>
<td>{{r.tmk}}</td>
<td>{{r.hdd}}</td>
<td>{{r.status}}</td>
</tr>
{% endfor %}
</table>
{% endif %}
"""

def parse_plz(text):
    raw = (text or "").replace("\r", "\n").replace(",", "\n")
    out = []
    for line in raw.splitlines():
        s = "".join(c for c in line if c.isdigit())
        if s:
            out.append(s)
    return list(dict.fromkeys(out))

@app.route("/")
def index():
    plz_raw = request.args.get("plz_list", "")
    day_str = request.args.get("day") or (dt.date.today() - dt.timedelta(days=3)).isoformat()
    tbase = float(request.args.get("tbase", "18") or 18)

    rows = []
    for plz in parse_plz(plz_raw):
        try:
            # STUB: Anti-500 demo calculation
            tmk = 7.0
            hdd = max(0.0, tbase - tmk)
            rows.append({
                "plz": plz,
                "day": day_str,
                "tmk": round(tmk,1),
                "hdd": round(hdd,1),
                "status": "OK"
            })
        except Exception as e:
            rows.append({
                "plz": plz,
                "day": "-",
                "tmk": "-",
                "hdd": "-",
                "status": str(e)
            })

    return render_template_string(HTML, plz=plz_raw, day=day_str, tbase=tbase, rows=rows)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
