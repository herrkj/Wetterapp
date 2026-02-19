from flask import Flask, request, render_template_string
app = Flask(__name__)

HTML = '''
<h2>DWD HDD – Anti-500 (stable)</h2>
<form method="get">
PLZ:<br><input name="plz_list"><br>
Datum (YYYY-MM-DD):<br><input name="day"><br>
Tbase:<br><input name="tbase" value="18"><br><br>
<button>Berechnen</button>
</form>
'''

@app.route("/", methods=["GET"])
def index():
    return render_template_string(HTML)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
