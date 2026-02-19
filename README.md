# DWD Heizgradtage (HDD) – PLZ-scharf (Flask)

## Was ist das?
Ein kleines Web-Tool (Browser-App), das für deutsche PLZ tägliche Heizgradtage (HDD) berechnet:

PLZ → Koordinaten (Nominatim/OSM) → nächste DWD-Station (daily/kl) → TMK (Tagesmitteltemp.) → HDD

Formel: HDD = max(0, Tbase − TMK)

## Start (lokal)

### 1) Python installieren
Python 3.10+ empfohlen.

### 2) Abhängigkeiten installieren
Im Projektordner:

Windows:
```bat
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
python app.py
```

macOS/Linux:
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python app.py
```

Dann im Browser öffnen: http://127.0.0.1:5000

## Hinweise
- PLZ→Koordinaten: Nominatim (OpenStreetMap) ohne API-Key. Für Produktion besser lokale PLZ-Koordinaten-Tabelle.
- DWD Daten: CDC OpenData daily/kl (TMK). Es wird zuerst `recent` versucht, dann `historical`.
- Cache: `cache.sqlite3` im Projektordner (PLZ-Geocoding + Stationsliste + historical ZIP-Mapping).

