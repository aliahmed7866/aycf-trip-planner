"""Navigation derived from registered routes, shared by every app entrypoint."""
from flask import current_app, request, url_for


def navigation_context():
    specs = [
        ('Search', 'index', 'search', True),
        ('Short trips', 'short_trips.page', 'trips', True),
        ('Flights', 'all_flights', 'flights', True),
        ('Watches', 'watches.watchlist', 'watches', True),
        ('My places', 'places.page', 'places', False),
        ('Stability', 'stability.page', 'stability', False),
        ('Scan settings', 'scan_settings.page', 'settings', False),
        ('System status', 'system_health.page', 'system', False),
    ]
    endpoint = request.endpoint or ''
    active = ('search' if endpoint in {'index', 'scan', 'multi_search.scan'} else
              'trips' if request.blueprint == 'short_trips' else
              'flights' if endpoint == 'all_flights' else
              {'watches': 'watches', 'places': 'places', 'stability': 'stability',
               'scan_settings': 'settings', 'system_health': 'system'}.get(request.blueprint))
    items = [dict(label=label, href=url_for(endpoint), active=key == active, primary=primary)
             for label, endpoint, key, primary in specs if endpoint in current_app.view_functions]
    return {'navigation_items': items}
