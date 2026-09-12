"""One search's memoized cache reads and unique missing route/date checks."""
from datetime import date


class SearchCache:
    def __init__(self, db, run_id):
        self.db, self.run_id = db, run_id
        self.missing = set()
        self._flights = {}
        run = db.get_pdf_run(run_id) if hasattr(db, 'get_pdf_run') else None
        self.start = self.end = None
        if run:
            try:
                self.start = date.fromisoformat(str(run['departure_start'])[:10])
                self.end = date.fromisoformat(str(run['departure_end'])[:10])
            except (ValueError, TypeError, KeyError):
                self.start = self.end = None

    def __getattr__(self, name):
        return getattr(self.db, name)

    def in_window(self, day):
        return (self.start is None or day >= self.start) and (self.end is None or day <= self.end)

    def get_flights(self, origin, destination, travel_day, pdf_run_id=None):
        if not self.in_window(travel_day):
            return []
        key = (pdf_run_id or self.run_id, origin, destination, travel_day)
        if key not in self._flights:
            self._flights[key] = self.db.get_flights(origin, destination, travel_day, key[0])
        result = self._flights[key]
        if result is None:
            self.missing.add((origin, destination, travel_day.isoformat()))
        return result


def scan_readiness(db, run_id, run):
    """Keep verified inventory usable without claiming full scan coverage."""
    ready = bool(run and run.get("scanned_at"))
    cache = db.stats(run_id) if run and not ready else {}
    usable = bool(ready or cache.get("cached_checks") or cache.get("cached_flights"))
    return {"ready": ready, "usable": usable, "partial": usable and not ready}
