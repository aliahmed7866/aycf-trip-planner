"""One search's memoized cache reads and unique missing route/date checks."""
from collections import defaultdict
from copy import deepcopy
from datetime import date

from scan_scope import route_allowed


class SearchCache:
    def __init__(self, db, run_id):
        self.db, self.run_id = db, run_id
        self.missing = set()
        self._flights = {}
        self._graph = self._scope = self._checked_routes = None
        self._adjacency = {}
        self._allowed = {}
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

    def adjacency(self, graph, day, scope):
        """Build the directed route index once per day in this search."""
        if graph is not self._graph or scope != self._scope:
            self._graph, self._scope = graph, deepcopy(scope)
            self._adjacency.clear()
            self._allowed.clear()
        if day not in self._adjacency:
            if self._checked_routes is None:
                if hasattr(self.db, 'searchable_routes'):
                    self._checked_routes = self.db.searchable_routes(self.run_id)
                elif hasattr(self.db, 'checked_routes'):
                    self._checked_routes = self.db.checked_routes(self.run_id)
                else:
                    self._checked_routes = set()
            adj = defaultdict(list)
            for origin, dest in set(graph.edges_for_day(day)).union(self._checked_routes):
                if scope is not None:
                    pair = (origin, dest)
                    if pair not in self._allowed:
                        self._allowed[pair] = route_allowed(origin, dest, scope)
                    if not self._allowed[pair]:
                        continue
                adj[origin].append(dest)
            self._adjacency[day] = {a: sorted(bs) for a, bs in adj.items()}
        return self._adjacency[day]

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
