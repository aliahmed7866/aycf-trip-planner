"""Manchester cash feeders, isolated from Wizz scans and their availability."""
import os
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlencode

from flask import Blueprint, flash, redirect, render_template, request, url_for

from airport_catalog import COUNTRY_CODES
from feeder_account import check_serpapi_account
from feeder_provider import ProviderError
from feeder_store import FeederStore
from feeder_automation import automation_status, set_automation_enabled
from feeder_ranking import rank_feeder_targets
from feeder_trips import DEFAULT_COUNTRIES, GROUND_LINKS, feeder_opportunities, match_feeders, normalize_offer
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
            'country': '', 'min_transfer': '180', 'max_layover': '1440', 'wizz_legs': '2', 'sort': 'price', 'airports': 'nearby',
        }.items()}
        errors = []
        if values['country'] not in ('', 'all', *COUNTRY_CODES):
            errors.append('Choose a destination country from the list.')
            values['country'] = ''
        for name, choices, default in [('min_transfer', {'180', '240', '360'}, '180'),
                                       ('max_layover', {'720', '1440', '2880'}, '1440'),
                                       ('wizz_legs', {'1', '2'}, '2'),
                                       ('sort', {'price', 'time', 'return'}, 'price'),
                                       ('airports', {'nearby', 'man'}, 'nearby')]:
            if values[name] not in choices:
                errors.append('Choose a connection limit from the list.')
                values[name] = default
        ctx = current_scope_run()
        countries = DEFAULT_COUNTRIES
        if values['country']:
            countries = None if values['country'] == 'all' else [values['country']]
        report = feeder_opportunities(db, ctx.get('run_id'), ctx['scope'],
                                     countries=countries,
                                     min_transfer_minutes=int(values['min_transfer']),
                                     max_layover_minutes=int(values['max_layover']),
                                     max_wizz_legs=int(values['wizz_legs']), limit=None,
                                     nearby=values['airports'] == 'nearby')
        ranked = rank_feeder_targets(dict(report, min_transfer_minutes=int(values['min_transfer']),
                                          max_layover_minutes=int(values['max_layover'])),
                                     countries=countries, show_paths=True)
        targets = {(item['hub'], item['date']): item for item in ranked}
        selected = targets.get((request.args.get('hub'), request.args.get('day')))
        api_key = os.environ.get('AYCF_SERPAPI_KEY', '').strip()
        if request.method == 'POST':
            if not csrf_ok():
                errors.append('Your form expired. Refresh this page and try again.')
            if not errors:
                action = request.form.get('action')
                if action == 'test_connection':
                    try:
                        result = check_serpapi_account(api_key)
                        flash(result['message'], 'success' if result['state'] == 'ready' else 'warning')
                    except ProviderError as exc:
                        flash(exc.safe_message, 'warning')
                    except Exception:
                        flash('The account check could not complete. No flight search was requested.', 'warning')
                    return redirect(url_for('feeders.page', **values))
                elif action in {'auto_pause', 'auto_resume'}:
                    set_automation_enabled(store, action == 'auto_resume')
                    flash('Automatic fare checks resumed.' if action == 'auto_resume' else 'Automatic fare checks paused.', 'info')
                    return redirect(url_for('feeders.page', **values))
                elif action == 'refresh':
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
                        departure_value = request.form.get('departure', '')
                        if selected and request.form.get('departure_time'):
                            departure_value = selected['date'] + 'T' + request.form['departure_time']
                        departure = local_datetime(departure_value, UK_ZONE)
                        if departure <= datetime.now(timezone.utc):
                            raise ValueError('Choose a Manchester departure that is still ahead.')
                        zone = airport_zone(hub)
                        if not zone:
                            raise ValueError('This hub has no reliable timezone.')
                        arrival = local_datetime(request.form.get('arrival', ''), zone)
                        if (hub, departure.date().isoformat()) not in targets:
                            raise ValueError('Choose one of the suggested Manchester departure dates.')
                        offer = {
                            'origin': 'MAN', 'destination': hub, 'airline': request.form.get('airline', ''),
                            'flight_number': request.form.get('flight_number', ''),
                            'departure': departure.isoformat(), 'arrival': arrival.isoformat(),
                            'price_gbp': request.form.get('price_gbp', ''), 'currency': 'GBP',
                            'observed_at': datetime.now(timezone.utc).isoformat(), 'source': 'Manually checked',
                            'booking_url': flight_search_url(hub, departure.date().isoformat()),
                        }
                        checked = normalize_offer(offer)
                        if Decimal(checked['price_gbp']) >= 40:
                            raise ValueError('Choose a fare below £40 including mandatory fees.')
                        fits = match_feeders(report, [checked], min_transfer_minutes=int(values['min_transfer']),
                                             max_layover_minutes=int(values['max_layover']))
                        store.save_offer(checked)
                        if fits:
                            flash('Fare saved. Your matching connections are listed below.', 'success')
                        else:
                            flash('Fare saved. Its arrival misses the selected transfer windows. Check another flight or change the layover filters.', 'info')
                        return redirect(url_for('feeders.page', _anchor='matches-title', **values))
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
        if values['sort'] == 'time':
            matches.sort(key=lambda trip: trip['total_minutes'])
        elif values['sort'] == 'return':
            matches.sort(key=lambda trip: not bool(trip.get('direct_return')))
        hubs = sorted({(item['hub'], item['hub_name']) for item in ranked})
        page_count = max(1, (len(ranked) + 11) // 12)
        page_number = min(page_count, max(1, request.args.get('page', 1, type=int)))
        visible = ranked[(page_number - 1) * 12:page_number * 12]
        for target in visible:
            target['search_url'] = flight_search_url(target['hub'], target['date'])
            if api_key:
                target['status'] = store.search_status(target['hub'], target['date'])
        if selected:
            selected['search_url'] = flight_search_url(selected['hub'], selected['date'])
        pass_paths = report.get('pass_only', [])
        pass_pages = max(1, (len(pass_paths) + 11) // 12)
        pass_page = min(pass_pages, max(1, request.args.get('pass_page', 1, type=int)))
        paths = selected['paths'] if selected else []
        path_pages = max(1, (len(paths) + 11) // 12)
        path_page = min(path_pages, max(1, request.args.get('path_page', 1, type=int)))
        return render_template('feeder_trips.html', values=values, errors=errors, report=report,
                               matches=matches[:100], total_matches=len(matches),
                               targets=visible, selected=selected, path_page=path_page, path_pages=path_pages,
                               paths=paths[(path_page-1)*12:path_page*12],
                               pass_paths=pass_paths[(pass_page-1)*12:pass_page*12], pass_page=pass_page, pass_pages=pass_pages,
                               page_number=page_number, page_count=page_count,
                               hubs=hubs, ground_links=GROUND_LINKS, countries=sorted(COUNTRY_CODES), connected=bool(api_key),
                               offers=offers, usage=store.usage(), automation=automation_status(store)), (400 if errors else 200)

    return bp
