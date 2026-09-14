from scan_scope import default_scope, route_requests


def test_pdf_alexandria_uses_directed_hbe_evidence():
    scope = default_scope()
    scope['_route_directory'] = {'routes': {'HBE': ['FCO'], 'FCO': ['HBE'], 'CIA': ['OLB']}}
    assert route_requests('Alexandria', 'Rome', scope) == [('Alexandria (Borg El Arab)', 'Rome Fiumicino')]
    assert route_requests('Rome', 'Alexandria', scope) == [('Rome Fiumicino', 'Alexandria (Borg El Arab)')]
    assert route_requests('ALY', 'Rome', scope) == [('ALY', 'Rome')]
    scope['excluded_airports'] = ['HBE']
    assert route_requests('Alexandria', 'Rome', scope) == []
    assert route_requests('Rome', 'Alexandria', scope) == []


def test_without_directory_no_hbe_route_is_invented():
    assert route_requests('Alexandria', 'Rome', default_scope()) == [('Alexandria', 'Rome')]
