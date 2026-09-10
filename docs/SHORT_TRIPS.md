# Released short trips

Open **Planner → Find short trips**, or `/short-trips`.

Choose UK departure airports and acceptable UK return airports independently.
The finder pairs a complete outward journey and return journey to one destination.
No weekdays are fixed. Optional departure and UK arrival deadlines use UK local
time; set the latter to Monday morning when appropriate. A deadline means landing
at the UK airport, so allow extra time for travelling home.

Every trip must provide strictly more than 24 hours between arrival at the
visiting airport and departure from that same airport. Longer minimum stays are
available. By default it also requires eight useful daytime hours, estimated
between 09:00 and 21:00 local time after allowing 90 minutes after arrival and
120 minutes before departure. These are estimates, not airport-transfer promises.

Direct flights are the default. Optional one- or two-stop journeys each way use
configured connection hubs, a minimum two-hour connection floor, exact physical
airport matches, and configurable layover and total journey limits. Domestic UK
stops and cycles within a journey are excluded. Different outward and return UK
airports are explicitly labelled. Existing exclusions apply to every flight.
Preferred destinations rank first, followed by useful daytime, fewer connections
and shorter travel time.

The finder makes no live requests and changes no scanner settings. It bulk-reads
successful route checks from the current completed scan identity and restricts
all departures to its released date range. It never predicts a future return,
uses historical stability as availability, or combines flights from different
scans. Unscanned routes cannot appear; scan the desired airports first. Results
include the oldest leg check time, since availability can change before booking.

Time arithmetic uses UTC instants after resolving airport-local times. Original
offset-bearing provider timestamps are recovered from retained raw fields.
Unknown airports/timezones, ambiguous daylight-saving clocks without offsets,
and invalid durations are omitted and counted on the page. Country timezone
mappings include airport overrides for the Canary Islands and Madeira; timezone
rules come from the installed dateutil/system timezone data.

Pairing and eligibility happen before the top-100 display limit. A 200,000-step
calculation budget protects the local device; hitting it produces an explicit
incomplete-search notice and asks for narrower choices. No schema migration or
additional runtime dependency is required.
