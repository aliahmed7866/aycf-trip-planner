"""Manchester cash feeders, isolated from Wizz scans and their availability."""
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

from flask import Blueprint, flash, redirect, render_template, request, url_for

from feeder_store import FeederStore
from feeder_trips import DEFAULT_COUNTRIES, feeder_opportunities, match_feeders
from short_trips import UK_ZONE, airport_zone, local_datetime


def flight_search_url(hub, travel_date):
    return 'https://www.google.com/travel/flights?' + urlencode({
        'q': f'One way flights from MAN to {hub} on {travel_date} Ryanair easyJet nonstop',
        'curr': 'GBP', 'hl': 'en',
    })


def create_feeder_blueprint(current_scope_run, db, csrf_ok, store=None):
    bp = Blueprint('feeders', __name__)
    store = store or FeederStore(str(Path(db.path).with_name('feeder_quotes.sqlite3')))

    @bp.route('/feeder-trips', methods=['GET', 'POST'])
    def page():
        values = {name: request.args.get(name, default) for name, default in {
            'country': '', 'min_transfer': '180', 'max_layover': '1440', 'wizz_legs': '2',
        }.items()}
        errors = []
        if values['country'] not in ('', *DEFAULT_COUNTRIES):
            errors.append('Choose a destination country from the list.')
            values['country'] = ''
        for name, choices, default in [('min_transfer', {'180', '240', '360'}, '180'),
                                       ('max_layover', {'720', '1440', '2880'}, '1440'),
                                       ('wizz_legs', {'1', '2'}, '2')]:
            if values[name] not in choices:
                errors.append('Choose a connection limit from the list.')
                values[name] = default
        ctx = current_scope_run()
        report = feeder_opportunities(db, ctx.get('run_id'), ctx['scope'],
                                     countries=[values['country']] if values['country'] else DEFAULT_COUNTRIES,
                                     min_transfer_minutes=int(values['min_transfer']),
                                     max_layover_minutes=int(values['max_layover']),
                                     max_wizz_legs=int(values['wizz_legs']), limit=None)
        targets = {}
        for option in report['opportunities']:
            for day in option['feeder_dates']:
                item = targets.setdefault((option['hub'], day), {
                    'hub': option['hub'], 'hub_name': option['hub_name'], 'date': day, 'destinations': set(),
                    'search_url': flight_search_url(option['hub'], day),
                })
                item['destinations'].add(option['destination_name'])
        api_key = os.environ.get('AYCF_SERPAPI_KEY', '').strip()
        if request.method == 'POST':
            if not csrf_ok():
                errors.append('Your form expired. Refresh this page and try again.')
            if not errors:
                action = request.form.get('action')
                if action == 'refresh':
                    key = (request.form.get('hub', ''), request.form.get('date', ''))
                    if key not in targets:
                        errors.append('This hub and date no longer have a matching onward flight. Refresh your scan.')
                    elif not api_key:
                        errors.append('Connect your own free SerpApi key to check fares here, or use the manual option below.')
                    else:
                        result = store.refresh(*key, api_key)
                        flash(result['message'], 'info')
                        return redirect(url_for('feeders.page', **values))
                elif action == 'add':
                    try:
                        if request.form.get('checked_now') != 'yes':
                            raise ValueError('Confirm you have just checked the fare and flight times.')
                        hub = request.form.get('hub', '')
                        if hub not in {key[0] for key in targets}:
                            raise ValueError('Choose a hub with an onward flight in this scan.')
                        departure = local_datetime(request.form.get('departure', ''), UK_ZONE)
                        zone = airport_zone(hub)
                        if not zone:
                            raise ValueError('This hub has no reliable timezone.')
                        arrival = local_datetime(request.form.get('arrival', ''), zone)
                        if (hub, departure.date().isoformat()) not in targets:
                            raise ValueError('Choose one of the suggested Manchester departure dates.')
                        store.save_offer({
                            'origin': 'MAN', 'destination': hub, 'airline': request.form.get('airline', ''),
                            'flight_number': request.form.get('flight_number', ''),
                            'departure': departure.isoformat(), 'arrival': arrival.isoformat(),
                            'price_gbp': request.form.get('price_gbp', ''), 'currency': 'GBP',
                            'observed_at': datetime.now(timezone.utc).isoformat(), 'source': 'Manually checked',
                            'booking_url': flight_search_url(hub, departure.date().isoformat()),
                        })
                        flash('Fare saved. Matching connections are shown below; recheck before booking.', 'success')
                        return redirect(url_for('feeders.page', **values))
                    except (ValueError, TypeError) as exc:
                        errors.append(str(exc) or 'Enter valid flight details.')
                elif action == 'delete':
                    store.delete_offer(request.form.get('offer_id', ''))
                    flash('Saved fare removed.', 'info')
                    return redirect(url_for('feeders.page', **values))
                else:
                    errors.append('Choose a valid action.')
        offers = store.list_offers()
        for offer in offers:
            offer['departure_local'] = datetime.fromisoformat(offer['departure']).astimezone(UK_ZONE).isoformat()
        matches = match_feeders(report, offers, min_transfer_minutes=int(values['min_transfer']),
                               max_layover_minutes=int(values['max_layover']))
        for trip in matches:
            offer = trip['feeder']
            offer['departure_local'] = datetime.fromisoformat(offer['departure']).astimezone(UK_ZONE).isoformat()
            offer['arrival_local'] = datetime.fromisoformat(offer['arrival']).astimezone(airport_zone(offer['destination'])).isoformat()
        for target in targets.values():
            target['status'] = store.search_status(target['hub'], target['date'])
        hubs = sorted({(target['hub'], target['hub_name']) for target in targets.values()})
        target_items = sorted(targets.values(), key=lambda x: (x['date'], x['hub']))
        page_count = max(1, (len(target_items) + 49) // 50)
        try:
            page_number = min(page_count, max(1, int(request.args.get('page', '1'))))
        except ValueError:
            page_number = 1
        return render_template('feeder_trips.html', values=values, errors=errors, report=report,
                               matches=matches[:100], total_matches=len(matches),
                               targets=target_items[(page_number - 1) * 50:page_number * 50],
                               page_number=page_number, page_count=page_count,
                               hubs=hubs, countries=DEFAULT_COUNTRIES, connected=bool(api_key),
                               offers=offers, usage=store.usage()), (400 if errors else 200)

    return bp
