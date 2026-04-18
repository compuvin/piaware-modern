# PiAware Modern

This project is a drop-in replacement web UI for an existing PiAware / SkyAware
installation.

It keeps the original SkyAware live-view runtime pieces that PiAware already
ships, but replaces the top-level pages, styling, history tooling, and aircraft
thumbnail flow with the modernized version in this repository.

## What it includes

- Modernized live view at `index.html`
- Flight history page at `history.html`
- History stats page at `history-stats.html`
- Local history logger and aircraft image cache services in `services/`

## Historical view

The history side is what turns this from a prettier live map into something
much more useful.

It keeps a local record of aircraft seen by your receiver, stores flight-path
points over time, and lets you go back and inspect what actually passed
through your airspace. You can browse aircraft you have seen before, review
saved flights, draw historic paths on the map, and look at recent traffic
windows like the last 6 hours, 24 hours, 7 days, or 30 days.

That means you are not limited to "what is overhead right now." You can use it
to answer questions like:

- What flew over last night?
- Which aircraft have I seen before?
- What path did that flight actually take as received by my station?
- How busy was my receiver today, this week, or this month?

## Install

These instructions assume you already have a working PiAware installation and
want to replace the contents of its SkyAware HTML directory.

1. Copy this project into the PiAware SkyAware HTML directory.

   Typical target:

   ```bash
   /usr/share/skyaware/html
   ```

   Example:

   ```bash
   sudo rsync -a --delete /path/to/piaware-modern/ /usr/share/skyaware/html/
   ```

2. Install and start the supporting services.

   ```bash
   sudo bash /usr/share/skyaware/html/services/install-services.sh
   ```

## What the install script does

The service installer:

- writes systemd unit files for the history logger and aircraft image cache
- points those units at the actual install path you copied into
- reloads systemd
- enables and starts both services

## Runtime notes

- The history database is created automatically under `data/` if it does not
  already exist.
- Aircraft images are cached automatically under `assets/aircraft/types/` as
  aircraft types are resolved.
- Those runtime cache files are intentionally not tracked in git.

## URLs

After installation, the main pages are:

- `http://<piaware-host>/skyaware/`
- `http://<piaware-host>/skyaware/history.html`
- `http://<piaware-host>/skyaware/history-stats.html`

If you are fronting PiAware with an HTTPS reverse proxy, make sure the proxy
also exposes:

- `/history-api/`
- `/image-cache/`

## Upstream base

This project is derived in part from FlightAware's `dump1090` SkyAware web UI:

- `https://github.com/flightaware/dump1090`

See `UPSTREAM.md` and `LICENSE` for the current upstream and licensing notes.
