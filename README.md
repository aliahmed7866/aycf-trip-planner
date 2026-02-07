# AYCF Flask Trip Planner (MVP)

A small Flask app that uses historical AYCF availability runs (CSV snapshots) to suggest "stable" itineraries:
Base → Hub → Target, plus a recommended return via a hub.

## 1) Put the data in place

The GitHub repo you shared contains daily CSV run files under a `data/` folder.

Set an environment variable to point the app at that folder:

```bash
export AYCF_DATA_DIR="/path/to/wizzair-aycf-availability-main/data"
```

If you instead copy the repo's `data/` folder into this project as `./data/`, you don't need the env var.

## 2) Install and run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python app.py
```

Open http://127.0.0.1:5000

## Notes

- "Stability" is based on how often a route appears in the filtered history (start/end date).
- This is NOT a guarantee seats will exist in the live 3-day AYCF booking window.
- Add more options in planner.py (DEFAULT_* lists) as you discover more useful hubs/targets.


## Live viability checking

This MVP does not scrape Wizz. Instead it provides one-click links to:
- WIZZ Link (self-transfer combinations)
- Wizz Air homepage search

Use the displayed legs and your chosen dates to verify flight times and layovers.
