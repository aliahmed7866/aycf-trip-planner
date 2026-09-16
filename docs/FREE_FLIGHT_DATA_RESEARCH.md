# Free feeder-flight data research

Research checked 16 September 2026. The goal is one adult travelling from
Manchester to an exact Wizz departure airport on a separate Ryanair/easyJet
ticket costing **less than £40**, then joining an AYCF flight. These are data
source findings, not a claim that a particular dated itinerary is available.

## Practical choice

The optional implementation uses **Google Flights through SerpApi**, a third
party, with a user-supplied key and a locally enforced free quota. No payment or
subscription upgrade is needed by the integration, and it must stop when its
budget is exhausted. There is no automatic fallback to a paid provider or a
direct website scraper. Manual entry remains useful when coverage is missing.

Automatic collection now uses the existing Termux supervisor and a post-scan
hook: up to two prioritised hub/date checks per six-hour batch, six automatic
attempts per rolling day, all inside the shared eight/day and 220/31-day caps.
It chooses current exact-airport onward options using recent route recurrence;
historical evidence never creates an available flight. Empty results wait a day
before automatic rechecking. Page loads do not trigger collection, and the
Manchester connections page exposes a persistent pause/resume control.

SerpApi currently advertises a recurring **$0 plan with 250 searches/month and
50/hour**. This is about eight searches/day, not enough for a wide daily scan of
every route and future date. Its pricing says successful empty searches count,
while cached, failed and errored searches do not. Account usage may include
other tools: a local budget cannot guarantee an account has unused credits.
Source: [SerpApi pricing](https://serpapi.com/pricing).

The documented `google_flights` engine supports exact airport/date queries,
GBP, one adult, one-way, economy, nonstop and maximum price filters. One response
can contain multiple flights. The adapter checks both `best_flights` and
`other_flights`, rejects other airlines and multi-leg offers, checks the actual
airport codes, and independently enforces price < £40. SerpApi describes a
one-hour exact-query cache; its `search_metadata.created_at` is preserved so a
cached result is not relabelled as freshly observed. `deep_search=true` and
`show_hidden=true` improve returned coverage, but no result proves absence of a
flight. Source: [SerpApi Google Flights documentation](https://serpapi.com/google-flights-api).

**Validation limitation:** the adapter has fixture-based tests. No user key was
available during development, so a real MAN fare response and current airline
coverage have not been verified. Public advertising prices are not substituted
for dated, timed, bookable offers. Returned prices must be checked with the
airline before buying.

## Alternatives and their limits

| Source | Free availability | Suitability |
| --- | --- | --- |
| Google Flights official integration | Public consumer website; developer material describes invited airline/OTA partners | No self-service consumer fare-search API found |
| SerpApi | Advertised recurring 250 searches/month free tier, key required | Optional limited provider; no coverage guarantee |
| Ryanair website endpoints / `ryanair-py` | Unofficial no-key access described by open-source maintainer | Unsupported surface, incomplete cheapest-fare discovery, usage restrictions |
| easyJet website / approved API | Free consumer fare finder; reseller API agreements | No documented open free fare API found |
| Amadeus Self-Service | Monthly free quotas, production billing beyond quota | Official FAQ excludes low-cost carriers; unsuitable for this requirement |
| Skyscanner | Partnership application required | No open automatic-monitoring access; live requests must be user-generated |
| Duffel | No upfront fee, booking-based business model | Excess searches can be charged; not an unlimited free monitoring feed |
| aviationstack | 100 monthly free real-time requests advertised | Flight-status data; future schedules/routes are paid features, not a low-fare search source |

### Google Flights and scraping

Google's own developer landing page describes partner onboarding and select,
invite-only integration specifications. It is an integration for airlines and
OTAs supplying booking options, not documentation for a public consumer search
API: [Google Flights Search](https://developers.google.com/travel/flights).

Open-source [fast-flights](https://github.com/AWeirdDev/flights) demonstrates
that extracting Google Flights data is technically possible; it is an
independent scraper using Google's website formats, with optional browser and
third-party integrations. It does not establish official API access or stable
coverage. Google [terms](https://policies.google.com/terms) prohibit automated
access contrary to machine-readable instructions, and its
[robots.txt](https://www.google.com/robots.txt) disallows `/travel/flights/search`
and booking paths. Direct scraping is therefore not the application's default.
SerpApi is a third-party scraping service, not an official Google API; its
availability does not establish Google approval.

### Ryanair

The original [ryanair-py project](https://github.com/cohaolain/ryanair-py)
describes no-key queries against Ryanair's website API. Its cheapest-flight
method returns at most one flight per destination across the requested range:
it cannot serve as a complete schedule. Its README also warns that requested
currency may not be respected and availability/accuracy are not guaranteed.

Ryanair's [current website terms](https://www.ryanair.com/gb/en/lp/legal/terms-of-use)
permit private noncommercial use and explicitly prohibit automated extraction
for commercial purposes. This is not a blanket statement banning personal
searches. However, those terms do not provide a supported public API contract,
and [robots.txt](https://www.ryanair.com/robots.txt) disallows `/api`. The
implementation does not assume unrestricted automated access or bypass blocks.

### easyJet

The [official low-fare finder](https://www.easyjet.com/en/cheap-flights) is a
useful free consumer tool. Its advertised prices are per person based on two
people on the same booking, so headline fares cannot be treated as confirmed
single-traveller offers. The [distribution charter](https://www.easyjet.com/en/business/distribution-charter)
requires approved-channel/direct API agreements for resellers and prohibits
their scraping; it explicitly concerns commercial resellers. The newer
[acceptable-use page](https://www.easyjet.com/en/help-centre/policy-terms-and-conditions/acceptable-use-policy)
was JavaScript-only in the research reader, so personal automation permissions
could not be confirmed. No blanket legal conclusion about personal scraping is
drawn from the reseller charter.

### Other developer feeds

The official [Amadeus developer FAQ](https://amadeus4dev.github.io/developer-guides/faq/)
says Self-Service excludes low-cost carriers; the test environment uses limited
data and production can bill beyond free quotas. Its old portal URLs redirected
to a blank landing page during this research, adding onboarding uncertainty.

[Skyscanner authentication](https://developers.skyscanner.net/docs/getting-started/authentication)
requires a successful partnership application. Its
[usage guidelines](https://developers.skyscanner.net/docs/getting-started/usage-guidelines)
require exact user-generated live-price searches and prohibit background live
requests; they describe expectations for booking-link click-through.

[Duffel pricing](https://duffel.com/pricing) lists an excess-search charge of
$0.005 above a 1500:1 search-to-book ratio. “No upfront costs” is not equivalent
to free search-only monitoring.

[aviationstack pricing](https://aviationstack.com/pricing) lists 100 free
real-time requests/month; future schedules and airline routes are in paid
plans. It does not solve the requirement to identify a sub-£40 fare.

## Planner implications

1. Use released Wizz/AYCF candidates to select valuable hubs and feeder dates
   before spending free searches. Do not multiply every Manchester route by
   every future day.
2. Store exact airports, offset-aware flight times, GBP price, source and
   observation time. Never join city aliases such as Warsaw or Milan as if
   their separate airports were interchangeable.
3. Show the feeder quote and Wizz availability separately. A normal Google
   Flights Wizz fare cannot prove AYCF seat availability.
4. Count transfers, bags, overnight accommodation and the return journey when
   comparing the full trip. Separate tickets have no implied connection
   protection. Unavailable or exhausted providers must stay visible as missing
   coverage, not become “no flights”.
