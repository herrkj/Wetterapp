
from flask import Flask, request, render_template_string
import os

app = Flask(__name__)

HTML = '''
<!doctype html>
<title>DWD HDD Test</title>
<h2>DWD HDD App – Testversion</h2>
<form method="get">
<textarea name="plz_list" rows="4" cols="20">{{plz}}</textarea><br><br>
<input name="day" value="{{day}}"><br><br>
<input name="tbase" value="{{tbase}}"><br><br>
<button type="submit">Berechnen</button>
</form>
{% if clean %}
<p><b>Bereinigte PLZ:</b> {{clean}}</p>
{% endif %}
'''

@app.route("/")
def index():
    raw = request.args.get("plz_list", "")
    clean = ",".join(
        p.strip() for p in raw.replace("\r", "").split("\n") if p.strip().isdigit()
    )
    return render_template_string(
        HTML,
        plz=raw,
        clean=clean,
        day=request.args.get("day",""),
        tbase=request.args.get("tbase","")
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
