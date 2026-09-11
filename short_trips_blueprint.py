"""Read-only short-trip search using the app's current scan identity."""
from datetime import datetime
from flask import Blueprint, render_template, request

from airport_catalog import country_for
from inbound_coverage import inbound_coverage
from scan_scope import endpoint_excluded, origin_options, endpoint_matches
from short_trips import UK_ZONE, local_datetime, released_short_trips


def create_short_trips_blueprint(current_scope_run, db, csrf_ok):
    bp = Blueprint('short_trips', __name__)

    @bp.get('/route-directory')
    def directory():
        from route_directory import load_directory
        data = load_directory()
        captured = datetime.fromtimestamp(data['captured_at'], UK_ZONE).strftime('%d %b %Y, %H:%M UK time') if data else ''
        labels = {'LTN': 'London Luton (LTN)', 'LGW': 'London Gatwick (LGW)', 'STN': 'London Stansted (STN)', 'LPL': 'Liverpool (LPL)', 'BHX': 'Birmingham (BHX)', 'LBA': 'Leeds/Bradford (LBA)'}
        return render_template('route_directory.html', directory=data, captured=captured, airport_labels=labels)

    @bp.route('/short-trips', methods=['GET', 'POST'])
    def page():
        ctx = current_scope_run()
        scope = ctx['scope']
        # Partial runs can contain verified flights; global readiness stays unchanged.
        with db.connect() as conn:
            search_ready = bool(ctx.get('run_id') and conn.execute(
                'SELECT 1 FROM route_checks WHERE pdf_run_id=? LIMIT 1', (ctx['run_id'],)).fetchone())

        options = sorted({name for name in origin_options(ctx['origins'] + ctx.get('destinations', []) + scope['origins'])
                          if country_for(name) == 'United Kingdom' and not endpoint_excluded(name, scope)})
        defaults = [name for name in options if any(endpoint_matches(name, selected) for selected in scope['origins'])]
        submitted = request.method == 'POST'
        selected_origins = request.form.getlist('origins') if submitted else defaults
        selected_returns = request.form.getlist('returns') if submitted else defaults
        values = {key: request.form.get(key, default) for key, default in {
            'leave_after': '', 'return_by': '', 'destination': '', 'min_stay_hours': '24',
            'min_daytime_hours': '8', 'max_stops': '0', 'min_transfer_minutes': '150',
            'max_layover_minutes': '720', 'max_journey_minutes': '0',
        }.items()}
        destinations = sorted({name for pair in ctx['pairs'] for name in pair
                               if country_for(name) != 'United Kingdom' and not endpoint_excluded(name, scope)})
        errors, result = [], None
        if submitted:
            if not csrf_ok():
                errors.append('Your search form expired. Please submit it again.')
            if not selected_origins or not selected_returns:
                errors.append('Select at least one UK departure airport and one UK return airport.')
            if any(name not in options for name in selected_origins + selected_returns):
                errors.append('Choose UK airports from the list.')
            if values['destination'] and values['destination'] not in destinations:
                errors.append('Choose a destination from the list, or Anywhere.')
            if not search_ready:
                errors.append('Run the current scan before searching for complete trips.')
            try:
                earliest = local_datetime(values['leave_after'], UK_ZONE) if values['leave_after'] else None
                latest = local_datetime(values['return_by'], UK_ZONE) if values['return_by'] else None
                if latest and latest <= (earliest or datetime.now(UK_ZONE)):
                    raise ValueError('The latest UK arrival must be after your earliest departure and the current time.')
                settings = {}
                for key, minimum, maximum in [('min_stay_hours', 24, 168), ('min_daytime_hours', 0, 72),
                                              ('max_stops', 0, 2), ('min_transfer_minutes', 120, 600),
                                              ('max_layover_minutes', 0, 2880), ('max_journey_minutes', 0, 5760)]:
                    value = int(values[key])
                    if not minimum <= value <= maximum:
                        raise ValueError('Please choose valid stay and journey limits.')
                    settings[key] = value
                if settings['max_layover_minutes'] and settings['max_layover_minutes'] < settings['min_transfer_minutes']:
                    raise ValueError('Maximum layover must be at least the minimum connection time.')
            except (ValueError, TypeError) as exc:
                errors.append(str(exc) if str(exc).startswith(('The latest', 'Choose', 'Please', 'Maximum')) else 'Please enter valid dates, times and whole-number limits.')
            if not errors:
                result = released_short_trips(db, ctx['run_id'], scope, selected_origins, selected_returns,
                                             leave_after=earliest, return_by=latest,
                                             destinations=[values['destination']] if values['destination'] else [], **settings)
        coverage = inbound_coverage(db, ctx['run_id'], ctx['pairs'], scope, selected_returns, today=datetime.now(UK_ZONE).date()) if ctx.get('run_id') else None
        return render_template('short_trips.html', coverage=coverage, search_ready=search_ready, scope_ctx=ctx, uk_options=options,
                               selected_origins=selected_origins, selected_returns=selected_returns,
                               destinations=destinations, values=values, errors=errors, result=result), (400 if errors else 200)

    return bp
