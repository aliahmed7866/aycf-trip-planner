# Mahrukh — Pakistani clothing storefront

A separate Flask + SQLite app inside the AYCF repository. Black and gold design, eight original SVG illustrations, responsive catalog/search/category filters, product details/sizes/gallery, persistent session cart, WhatsApp order enquiries, and password-protected product management.

## Termux setup matched to this repository

The checked-in hub configuration reserves 8079 for the hub, 8080 for AYCF, 8081 for Sunscape, 8082 for Pocketwise, 8083 for Media Hub and 8084 for Places. Mahrukh defaults to **8085**. These are repository defaults, not a live inspection of your phone. The installer checks registered port conflicts; the service checks the actual listening port. Other apps are never stopped.

To test this branch without switching the working tree used by AYCF's auto-deployer:

```bash
cd "$HOME/aycf-trip-planner"
git fetch origin feat/mahrukh-storefront
git worktree add "$HOME/mahrukh-preview" origin/feat/mahrukh-storefront
bash "$HOME/mahrukh-preview/termux/install-mahrukh.sh"
```

If Python is missing, install `pkg install python python-pip git`. Do not upgrade system packages or other apps' virtual environments just to install Mahrukh. The installer creates `mahrukh/.venv` with only Flask and Waitress, a pure-Python WSGI server. MarkupSafe (a Flask dependency) has an optional C speedup and falls back to Python if compilation is unavailable. No Node.js, Docker, npm or CSS build step is used.

On first run, enter a new admin password (at least 12 characters), your seller WhatsApp number such as `923001234567`, and the port. A blank WhatsApp number leaves checkout disabled. The example phone number is a format illustration; enter your own number. Credentials are stored only in ignored `mahrukh/instance/config.json`, with a password hash and private file permissions. Sample catalog entries are seeded only on the first setup.

If `termux-services` is already installed, this adds and starts only the `mahrukh` runit service. Otherwise it prints a foreground start command. For a background service:

```bash
pkg install termux-services
```

Reopen Termux, then rerun the installer. Open **http://127.0.0.1:8085** and use **/admin** for the seller studio. The AYCF hub reads its registry on each request; refresh it to see Mahrukh. If the hub Install button is used before first-time configuration, the installer explains how to perform the interactive setup in a terminal.

Foreground start (do not run this while the Mahrukh background service is running):

```bash
cd "$HOME/mahrukh-preview/mahrukh"
.venv/bin/python run.py
```

Service controls:

```bash
sv status mahrukh
sv down mahrukh
sv up mahrukh
sv restart mahrukh
```

The app binds to loopback only and uses a unique `mahrukh_session` cookie to avoid signing users out of the other localhost apps. It intentionally ignores generic `PORT` and Flask secret variables from other apps. Local HTTP cookies are suitable for this phone-only workflow; public hosting would require HTTPS and secure-cookie configuration.

## Changing settings or an occupied port

```bash
cd "$HOME/mahrukh-preview/mahrukh"
.venv/bin/python setup.py
bash ../termux/install-mahrukh.sh
```

Blank password keeps the existing password. Blank WhatsApp number keeps the existing number; `-` disables checkout. Setup also asks for the port. Rerun the installer after a port change so the hub URLs stay in sync. Reserved but stopped apps are checked in the hub registry; active unregistered apps are caught by socket binding. No process is killed to free a port.

`MAHRUKH_INSTANCE` optionally selects a separate persistent data directory. `AYCF_CONFIG_DIR` / `AYCF_ADMIN_REGISTRY` select a custom existing hub registry if you use one. Use the same `MAHRUKH_INSTANCE` whenever running setup, the installer or the foreground server.

## Catalog and artwork

All eight seeded products are **illustrated samples**, with sample PKR prices and fabric descriptions. Replace these with your actual inventory before using the shop commercially. In the seller studio, add/edit/hide/delete products, edit whole-PKR prices, optional original prices, sizes, descriptions and up to six images. Use HTTPS image URLs for actual product photos or one of the bundled SVG paths shown in the editor. Selecting bundled art automatically retains the illustration label. The gallery displays thumbnails when multiple images exist.

The original editable SVGs depict emerald and indigo shalwar kameez, rose unstitched fabrics, a midnight abaya, ruby lehenga, saffron peshwas, and ivory/plum formals. They are used across the hero, category cards, catalog, detail pages, editorial panel and empty state. Regenerate them with `python make_art.py`.

CSS, JavaScript, fonts and SVGs are served locally; no Tailwind CDN or external font is required. The storefront works offline after installation. External product photographs need connectivity, and WhatsApp checkout needs WhatsApp/internet access. Core browsing and cart forms also work without JavaScript.

## Checkout behaviour

The server validates product IDs, available sizes and quantities, then recalculates prices from SQLite. No client-supplied price is trusted. The bag has limits of 20 selections and 10 units per selection. Items hidden, deleted or no longer offering the selected size are excluded. Delivery details are sent through an encoded WhatsApp message, which the customer reviews and sends manually. Delivery fees, availability, payment and confirmation are handled by the seller. No payment is captured, no stock is reserved and no order is saved by this app. The bag persists in a signed cookie; it is not an inventory reservation.

Admin sessions expire after four hours; login attempts are throttled, and every mutation requires a CSRF token. Delete prompts provide a browser confirmation. Back up `instance/` while the service is stopped; keep it private and out of Git. Starting the app does not repopulate products removed by the seller.

## Checks

```bash
cd "$HOME/mahrukh-preview/mahrukh"
.venv/bin/python -m unittest discover -s tests -v
```

Tests cover filtering, bundled art, cart validation, recalculated totals, encoded WhatsApp messages, disabled checkout, CSRF, admin authorization/CRUD, throttling and preservation of other hub apps. Browser checks can be run separately; Playwright is not a runtime dependency. This implementation is tested in Linux, but should still be smoke-tested on your Android device.

The review worktree remains independent of AYCF's auto-deployer. Keep it in place while the Mahrukh service points to it. After merging, you may install from the main checkout, but first stop Mahrukh and move its private instance directory or point `MAHRUKH_INSTANCE` to the existing one. Mahrukh is not automatically restarted by AYCF's deployment watcher.
