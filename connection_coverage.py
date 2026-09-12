"""Bounded PDF-backed two-leg UK connections through explicitly allowed airports."""


def extend_plan(pdf_pairs, scope, primary, hubs, days):
    from scan_scope import (connection_settings, transit_scope, route_requests,
                            endpoint_matches, endpoint_excluded, origin_variants)
    from airport_catalog import country_for
    settings = connection_settings(scope)
    report = {'extra_checks': 0, 'candidate_bundles': 0, 'deferred_bundles': 0,
              'budget': settings['connection_budget']}
    if not settings['connection_airports'] or not report['budget']:
        return report
    permissive = transit_scope(scope)
    preferred = scope.get('preferred_destinations', [])
    targets = preferred + scope.get('connection_hubs', [])
    pairs = sorted(set(map(tuple, pdf_pairs)))
    bundles = []
    for a, b in pairs:
        inbound = bool(origin_variants(b, scope))
        outbound = bool(origin_variants(a, scope))
        if inbound == outbound:
            continue
        via = a if inbound else b
        if not any(endpoint_matches(via, item) for item in settings['connection_airports']):
            continue
        if not route_requests(a, b, permissive):
            continue
        for c, d in pairs:
            if not endpoint_matches(d if inbound else c, via):
                continue
            target = c if inbound else d
            if endpoint_excluded(target, scope) or not any(endpoint_matches(target, t) for t in targets):
                continue
            if country_for(target) == 'United Kingdom' or origin_variants(target, scope) or not route_requests(c, d, permissive):
                continue
            rank = 0 if any(endpoint_matches(target, t) for t in preferred) else 1
            bundles.append((rank, 0 if inbound else 1, (a, b), (c, d)))
    existing = set(primary + hubs)
    requests = {}
    for _, _, anchor, connector in sorted(set(bundles)):
        report['candidate_bundles'] += 1
        additions = set((anchor, connector)) - existing
        cost = sum(len(route_requests(a, b, permissive)) * days for a, b in additions)
        if report['extra_checks'] + cost > report['budget']:
            report['deferred_bundles'] += 1
            continue
        for a, b in sorted(additions):
            requests[a + '\n' + b] = route_requests(a, b, permissive)
            primary.append((a, b))
        existing.update(additions)
        report['extra_checks'] += cost
    scope['_connection_requests'] = requests
    return report
