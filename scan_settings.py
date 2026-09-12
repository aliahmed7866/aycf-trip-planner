"""Reversible scan exclusions with a local, request-free scan preview."""
from collections import defaultdict
import json

from flask import Blueprint, abort, flash, jsonify, redirect, render_template, request, url_for

from airport_catalog import COUNTRY_CODES, airport_code, country_for, airport_labels, is_current_wizz_airport
from recommendation_preferences import scan_scope_with_preferences
from scan_scope import (AIRPORT_GROUPS, clean_exclusions, load_scope, normalize_name,
                        save_scope, scan_plan, endpoint_matches, endpoint_excluded, endpoint_key,
                        exclusions_fingerprint, scope_fingerprint, scan_window, route_allowed)


def exclusion_catalog(pairs, scope):
    names = {name for pair in pairs for name in pair}
    for key in ("origins", "connection_hubs", "connection_airports", "excluded_airports", "preferred_destinations", "destinations"):
        names.update(scope.get(key) or [])
    for pair in scope.get("excluded_routes") or []:
        names.update(pair)
    names.update(airport_labels().values())
    for group, members in AIRPORT_GROUPS.items():
        if any(normalize_name(name) == group for name in names):
            names.update(members)
    # One checkbox per airport identity, preferring its readable city label.
    labels = {}
    for name in sorted(names, key=lambda value: (len(value) == 3, normalize_name(value))):
        labels.setdefault(endpoint_key(name), name)
    label = lambda name: labels[endpoint_key(name)]
    excluded_keys = {endpoint_key(name) for name in scope.get("excluded_airports") or []}
    connection_keys = {endpoint_key(name) for name in scope.get('connection_airports') or []}
    grouped = defaultdict(list)
    for key, name in sorted(labels.items(), key=lambda item: normalize_name(item[1])):
        grouped[country_for(name)].append({"name": name, "code": airport_code(name), "current": is_current_wizz_airport(name),
            "key": key, "excluded": key in excluded_keys, "connection": key in connection_keys,
            "members": ",".join(endpoint_key(member) for member in AIRPORT_GROUPS.get(normalize_name(name), [])),
            "group": normalize_name(name) in AIRPORT_GROUPS})
    selected_countries = {normalize_name(country) for country in scope.get("excluded_countries") or []}
    for country in scope.get("excluded_countries") or []:
        if normalize_name(country) not in {normalize_name(item) for item in grouped}:
            grouped.setdefault(country, [])
    route_pairs = set()
    for a, b in pairs:
        for first in AIRPORT_GROUPS.get(normalize_name(a), [a]):
            for second in AIRPORT_GROUPS.get(normalize_name(b), [b]):
                if endpoint_key(first) != endpoint_key(second):
                    route_pairs.add(tuple(sorted((label(first), label(second)), key=normalize_name)))
    selected = {tuple(sorted((label(a), label(b)), key=normalize_name))
                for a, b in clean_exclusions(scope)["excluded_routes"]}
    route_pairs.update(selected)
    routes = [{"origin": a, "destination": b, "value": json.dumps([a, b]), "excluded": (a, b) in selected,
               "origin_key": endpoint_key(a), "destination_key": endpoint_key(b),
               "origin_country": country_for(a), "destination_country": country_for(b),
               "search": f"{a} {airport_code(a)} {country_for(a)} {b} {airport_code(b)} {country_for(b)}"}
              for a, b in sorted(route_pairs)]
    connection_options = sorted((item for country, items in grouped.items() for item in items
                                 if item['current'] and not item['group'] and country != 'United Kingdom'),
                                key=lambda item: normalize_name(item['name']))
    for item in connection_options:
        item['country'] = country_for(item['name'])
    return {"countries": dict(sorted(grouped.items())), "routes": routes, "connection_options": connection_options,
            "selected_countries": [country for country in grouped if normalize_name(country) in selected_countries],
            "uk_options": sorted(name for code, name in airport_labels().items() if country_for(code) == 'United Kingdom'),
            "coverage_options": sorted({name for pair in pairs for name in pair if is_current_wizz_airport(name) and country_for(name) != 'United Kingdom'} | set(scope.get('destinations') or []) | set(scope.get('connection_hubs') or []), key=normalize_name)}


def submitted_settings(form, catalog, scope):
    result = dict(scope)
    result.update(submitted_exclusions(form, catalog))
    if form.get('scope_settings') == '1':
        origins = form.getlist('scope_origins')
        destinations = form.getlist('scope_destinations')
        hubs = form.getlist('connection_hubs')
        mode = form.get('destination_mode', 'all')
        if not origins or not set(origins) <= set(catalog['uk_options']):
            raise ValueError('Choose at least one UK airport.')
        if not set(destinations + hubs) <= set(catalog['coverage_options']):
            raise ValueError('Choose destinations and hubs from the displayed list.')
        if mode not in ('all', 'only') or (mode == 'only' and not destinations):
            raise ValueError('Choose destinations when using Only selected destinations.')
        result.update(origins=origins, destinations=destinations, connection_hubs=hubs, destination_mode=mode)
    return result


def submitted_exclusions(form, catalog):
    airports = {item["name"] for items in catalog["countries"].values() for item in items}
    countries = set(catalog["countries"]) | set(COUNTRY_CODES)
    routes = {item["value"]: [item["origin"], item["destination"]] for item in catalog["routes"]}
    selected_airports, selected_countries, selected_routes = (form.getlist(name) for name in (
        "excluded_airports", "excluded_countries", "excluded_routes"))
    if (not set(selected_airports) <= airports or not set(selected_countries) <= countries
            or not set(selected_routes) <= routes.keys()):
        raise ValueError("The route list changed. Reload this page and try again.")
    result = {"excluded_airports": selected_airports, "excluded_countries": selected_countries,
              "excluded_routes": [routes[value] for value in selected_routes]}
    if 'connection_budget' in form:
        connections = form.getlist('connection_airports')
        if not set(connections) <= airports:
            raise ValueError('Choose connection airports from the displayed list.')
        try:
            budget = int(form['connection_budget'])
        except (ValueError, TypeError):
            raise ValueError('Connection budget must be a whole number from 0 to 100.')
        if not 0 <= budget <= 100:
            raise ValueError('Connection budget must be between 0 and 100.')
        result.update(connection_airports=connections, connection_budget=budget)
    return result


def exclusion_preview(pairs, scope, window=None):
    window = window or scan_window(None)
    plan = scan_plan(pairs, scope, days=window["days"])
    unrestricted = dict(scope, excluded_airports=[], excluded_countries=[], excluded_routes=[])
    if unrestricted.get("destination_mode") == "exclude":
        unrestricted.update(destination_mode="all", destinations=[])
    baseline = scan_plan(pairs, unrestricted, days=window["days"])
    return {key: plan[key] for key in ("route_count", "checks", "request_units", "estimated_minutes")} | {
        'connection_coverage': plan['connection_coverage'],
        "saved_requests": max(0, baseline["request_units"] - plan["request_units"]),
        "blocked_preferences": [name for name in scope.get("preferred_destinations") or [] if not any(not endpoint_excluded(airport, scope) for airport in AIRPORT_GROUPS.get(normalize_name(name), [name]))],
        "blocked_watches": sum(not route_allowed(a, b, scope) for a, b in scope.get("watch_routes") or []),
        "window": window,
    }


def editor_scope():
    scope = scan_scope_with_preferences(load_scope())
    # Expose old "All except selected" choices in the same reversible controls.
    if scope.get("destination_mode") == "exclude":
        scope["excluded_airports"] = sorted(set(scope["excluded_airports"] + scope["destinations"]))
        scope.update(destination_mode="all", destinations=[])
    return scope


def create_scan_settings_blueprint(route_catalog, csrf_ok):
    bp = Blueprint("scan_settings", __name__)

    @bp.route("/settings/scan-exclusions", methods=["GET", "POST"])
    def page():
        frame, pairs, _, _, _ = route_catalog()
        scope = editor_scope()
        catalog = exclusion_catalog(pairs, scope)
        if request.method == "POST":
            if not csrf_ok():
                abort(400, "Your form expired. Reload the page.")
            try:
                revision = request.form.get("exclusion_revision")
                if revision is not None and revision != scope_fingerprint(scope):
                    if request.accept_mimetypes.best == "application/json":
                        return jsonify({"error": "Exclusions were changed in another tab. Reload before saving."}), 409
                    abort(409, "Exclusions were changed in another tab. Reload before saving.")
                updated = submitted_settings(request.form, catalog, scope)
                save_scope(updated["origins"], updated["destination_mode"], updated["destinations"],
                           updated["connection_hubs"], updated["workers"],
                           **{key: updated[key] for key in ('excluded_airports', 'excluded_countries', 'excluded_routes', 'connection_airports', 'connection_budget')})
            except ValueError as exc:
                if request.accept_mimetypes.best == "application/json":
                    return jsonify({"error": str(exc)}), 400
                flash(str(exc), "warning")
                return redirect(url_for("scan_settings.page"))
            if request.accept_mimetypes.best == "application/json":
                return jsonify({"ok": True, "url": url_for("scan_settings.page")})
            flash("Exclusions saved for the next scan. An already-running scan keeps its starting settings.", "success")
            return redirect(url_for("scan_settings.page"))
        return render_template("scan_exclusions.html", catalog=catalog, scope=scope,
                               preview=exclusion_preview(pairs, scope, scan_window(frame)),
                               exclusion_revision=scope_fingerprint(scope))

    @bp.post("/settings/scan-exclusions/preview")
    def preview():
        if not csrf_ok():
            abort(400, "Your form expired. Reload the page.")
        frame, pairs, _, _, _ = route_catalog()
        scope = editor_scope()
        try:
            scope = submitted_settings(request.form, exclusion_catalog(pairs, scope), scope)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        return jsonify(exclusion_preview(pairs, scope, scan_window(frame)))

    return bp
