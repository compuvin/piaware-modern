#!/usr/bin/env python3
"""Fetch and cache exact aircraft type photos from Wikimedia Commons."""

from __future__ import annotations

import argparse
import html
import json
import logging
import os
import re
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlencode, urlparse
from urllib.request import Request, urlopen


SERVICE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = SERVICE_ROOT.parent
TYPE_DIR = PROJECT_ROOT / "assets" / "aircraft" / "types"
INDEX_PATH = TYPE_DIR / "index.json"
DEFAULT_LOG_PATH = PROJECT_ROOT / "logs" / "aircraft-image-cache.log"
COMMONS_API = "https://commons.wikimedia.org/w/api.php"
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
USER_AGENT = "piaware-modern-aircraft-cache/1.0"
LOCK = threading.Lock()
LOGGER = logging.getLogger("piaware-modern-aircraft-cache")


TYPE_SEARCH = {}

BAD_TITLE_WORDS = {
    "AIRPORT",
    "PARK",
    "PAGE",
    "PAGES",
    "LOGO",
    "DIAGRAM",
    "DRAWING",
    "MAP",
    "MUSEUM",
    "POSTER",
    "BROCHURE",
    "BADGE",
    "EMBLEM",
    "WIKIPEDIA",
    "SKYLINE",
    "CITY",
    "STATION",
    "BUILDING",
    "MONUMENT",
    "SCULPTURE",
}

AIRCRAFT_HINT_WORDS = {
    "AIRCRAFT",
    "AIRPLANE",
    "AEROPLANE",
    "AVIATION",
    "BOEING",
    "AIRBUS",
    "CESSNA",
    "PIPER",
    "BEECHCRAFT",
    "BOMBARDIER",
    "EMBRAER",
    "GULFSTREAM",
    "DIAMOND",
    "CIRRUS",
    "PILATUS",
    "SOCATA",
    "LEONARDO",
    "AGUSTAWESTLAND",
    "DASSAULT",
    "GRUMMAN",
    "NORTHAMERICAN",
    "LOCKHEED",
    "MCDONNELL",
    "DOUGLAS",
}


def normalize_type(type_code: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", type_code.upper())


def log(message: str) -> None:
    LOGGER.info(message)


def setup_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    LOGGER.setLevel(logging.INFO)
    LOGGER.handlers.clear()

    formatter = logging.Formatter("%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    file_handler = TimedRotatingFileHandler(log_path, when="midnight", backupCount=7)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    LOGGER.addHandler(file_handler)
    LOGGER.addHandler(console_handler)
    LOGGER.propagate = False


def load_index() -> dict[str, Any]:
    if not INDEX_PATH.exists():
        return {}
    with INDEX_PATH.open() as fh:
        return json.load(fh)


def save_index(index: dict[str, Any]) -> None:
    TYPE_DIR.mkdir(parents=True, exist_ok=True)
    with INDEX_PATH.open("w") as fh:
        json.dump(index, fh, indent=2, sort_keys=True)


def strip_html(value: str) -> str:
    return re.sub(r"<[^>]+>", "", html.unescape(value or "")).strip()


def fetch_json(url: str) -> dict[str, Any]:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=20) as response:
        return json.load(response)


def download_binary(url: str) -> tuple[bytes, str]:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=30) as response:
        content_type = response.headers.get_content_type()
        return response.read(), content_type


def guess_search_term(type_code: str) -> str:
    if type_code in TYPE_SEARCH:
        return TYPE_SEARCH[type_code]
    if re.fullmatch(r"B77[23W8X9]", type_code):
        mapping = {"B772": "Boeing 777-200", "B773": "Boeing 777-300", "B77W": "Boeing 777-300ER"}
        return mapping.get(type_code, f"Boeing {type_code}")
    if re.fullmatch(r"B7[34][0-9A-Z]{1,2}", type_code):
        return f"Boeing {type_code}"
    if type_code.startswith("A3"):
        return f"Airbus {type_code}"
    if type_code.startswith("C") and len(type_code) == 4:
        return f"Cessna {type_code[1:]}"
    return type_code


def score_aircraft_name_result(type_code: str, title: str, snippet: str) -> int:
    normalized_type = re.sub(r"[^A-Z0-9]", "", type_code.upper())
    normalized_title = re.sub(r"[^A-Z0-9]", "", title.upper())
    text = f"{title} {snippet}".upper()
    words = set(re.findall(r"[A-Z0-9]+", text))
    score = 0

    if normalized_type and normalized_type in normalized_title:
        score += 35
    elif normalized_type and normalized_type in re.sub(r"[^A-Z0-9]", "", text):
        score += 20

    if any(word in words for word in AIRCRAFT_HINT_WORDS):
        score += 20
    if re.search(r"\b(AIRCRAFT|AIRPLANE|AEROPLANE|AVIATION|TWIN-ENGINE|SINGLE-ENGINE)\b", text):
        score += 15
    if "(DISAMBIGUATION)" in title.upper():
        score -= 35
    if any(word in words for word in BAD_TITLE_WORDS):
        score -= 35
    if re.search(r"\b(ROUTE|ROAD|HIGHWAY|COUNTY|TOWNSHIP|ELECTION|DISTRICT)\b", text):
        score -= 35

    return score


def resolve_aircraft_name_from_wikipedia(type_code: str) -> str | None:
    queries = [
        f'"{type_code}" aircraft',
        f'"{type_code}" ICAO aircraft',
        f'"{type_code}" aircraft type',
    ]
    best_title: str | None = None
    best_score = -10_000

    for query in queries:
        params = {
            "action": "query",
            "format": "json",
            "list": "search",
            "srlimit": "5",
            "srsearch": query,
        }
        data = fetch_json(f"{WIKIPEDIA_API}?{urlencode(params)}")
        results = data.get("query", {}).get("search", [])

        for result in results:
            title = result.get("title", "")
            snippet = strip_html(result.get("snippet", ""))
            if not title:
                continue
            score = score_aircraft_name_result(type_code, title, snippet)
            if score > best_score:
                best_title = title
                best_score = score

    return best_title if best_title and best_score >= 40 else None


def resolve_search_term(type_code: str) -> str:
    guessed = guess_search_term(type_code)
    if guessed != type_code:
        return guessed

    try:
        wikipedia_name = resolve_aircraft_name_from_wikipedia(type_code)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        log(f"[cache] Wikipedia aircraft name lookup failed for {type_code}: {exc}")
        wikipedia_name = None

    if wikipedia_name:
        log(f"[cache] Wikipedia aircraft name for {type_code}: {wikipedia_name}")
        return wikipedia_name

    return type_code


def search_queries(type_code: str, term: str) -> list[str]:
    queries = [f'"{term}" aircraft', f'"{term}" airplane', f'"{term}" aviation', f'"{term}"']
    if term == type_code:
        queries.extend([f'"{type_code}" aircraft', f'"{type_code}" airplane'])

    deduped: list[str] = []
    seen: set[str] = set()
    for query in queries:
        if query not in seen:
            deduped.append(query)
            seen.add(query)
    return deduped


def score_result(term: str, title: str) -> int:
    normalized_term = re.sub(r"[^A-Z0-9]", "", term.upper())
    normalized_title = re.sub(r"[^A-Z0-9]", "", title.upper())
    title_words = set(re.findall(r"[A-Z0-9]+", title.upper()))
    score = 0

    if normalized_term and normalized_term in normalized_title:
        score += 30
    if any(word in title_words for word in AIRCRAFT_HINT_WORDS):
        score += 15
    if "FILE:" in title.upper():
        score += 2
    if any(word in title_words for word in BAD_TITLE_WORDS):
        score -= 40
    if re.search(r"\b(ICAO|IATA|LOGO|MAP|PAGE|PARK)\b", title.upper()):
        score -= 20
    return score


def is_plausible_aircraft_result(title: str) -> bool:
    title_words = set(re.findall(r"[A-Z0-9]+", title.upper()))
    if any(word in title_words for word in BAD_TITLE_WORDS):
        return False
    return True


def search_commons_file(type_code: str, term: str) -> str | None:
    best_title: str | None = None
    best_score = -10_000

    for query in search_queries(type_code, term):
        params = {
            "action": "query",
            "format": "json",
            "list": "search",
            "srnamespace": "6",
            "srlimit": "8",
            "srsearch": query,
        }
        data = fetch_json(f"{COMMONS_API}?{urlencode(params)}")
        results = data.get("query", {}).get("search", [])

        for result in results:
            title = result.get("title", "")
            if not title or not is_plausible_aircraft_result(title):
                continue
            score = score_result(term, title)
            if score > best_score:
                best_title = title
                best_score = score

        if best_title and best_score >= 30:
            return best_title

    return best_title


def get_commons_image_info(file_title: str) -> dict[str, Any] | None:
    params = {
        "action": "query",
        "format": "json",
        "prop": "imageinfo",
        "titles": file_title,
        "iiprop": "url|extmetadata",
        "iiurlwidth": "1280",
    }
    data = fetch_json(f"{COMMONS_API}?{urlencode(params)}")
    pages = data.get("query", {}).get("pages", {})
    for page in pages.values():
        imageinfo = page.get("imageinfo", [])
        if imageinfo:
            return imageinfo[0]
    return None


def choose_extension(content_type: str, url: str) -> str:
    if content_type == "image/png":
        return ".png"
    if content_type == "image/webp":
        return ".webp"
    suffix = Path(urlparse(url).path).suffix.lower()
    return suffix if suffix in {".jpg", ".jpeg", ".png", ".webp"} else ".jpg"


def resolve_type(type_code: str) -> dict[str, Any]:
    type_code = normalize_type(type_code)
    log(f"[cache] request for type {type_code}")

    with LOCK:
        index = load_index()
        existing = index.get(type_code)
        if existing:
            asset_path = PROJECT_ROOT / existing["asset"]
            if asset_path.exists():
                log(f"[cache] hit for {type_code}: {existing['asset']}")
                return {"status": "ready", **existing}

    search_term = resolve_search_term(type_code)
    file_title = search_commons_file(type_code, search_term)
    if not file_title:
        log(f"[cache] no Commons search result for {type_code}")
        return {"status": "missing", "reason": "no_search_result"}
    log(f"[cache] Commons match for {type_code}: {file_title}")

    info = get_commons_image_info(file_title)
    if not info:
        log(f"[cache] no image info for {type_code} ({file_title})")
        return {"status": "missing", "reason": "no_image_info"}

    image_url = info.get("thumburl") or info.get("url")
    if not image_url:
        log(f"[cache] no image URL for {type_code} ({file_title})")
        return {"status": "missing", "reason": "no_image_url"}

    log(f"[cache] downloading {type_code} from {image_url}")
    binary, content_type = download_binary(image_url)
    ext = choose_extension(content_type, image_url)
    TYPE_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{type_code}{ext}"
    target = TYPE_DIR / filename
    target.write_bytes(binary)
    log(f"[cache] saved {type_code} to {target} ({len(binary)} bytes)")

    meta = info.get("extmetadata", {})
    artist = strip_html(meta.get("Artist", {}).get("value", "")) or "unknown author"
    license_name = strip_html(meta.get("LicenseShortName", {}).get("value", "")) or strip_html(meta.get("UsageTerms", {}).get("value", "license unknown"))
    title_text = search_term + " reference"
    caption = f"{search_term}. Auto-cached from Wikimedia Commons. Photo by {artist}, {license_name}."
    source_url = f"https://commons.wikimedia.org/wiki/{quote(file_title.replace(' ', '_'), safe=':/_()')}"
    entry = {
        "asset": f"assets/aircraft/types/{filename}",
        "title": title_text,
        "caption": caption,
        "source_url": source_url,
        "file_title": file_title,
    }

    with LOCK:
        index = load_index()
        index[type_code] = entry
        save_index(index)
    log(f"[cache] indexed {type_code} -> {entry['asset']}")

    return {"status": "ready", **entry}


class Handler(BaseHTTPRequestHandler):
    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        super().end_headers()

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/resolve":
            self.respond({"status": "error", "reason": "not_found"}, HTTPStatus.NOT_FOUND)
            return

        params = parse_qs(parsed.query)
        type_code = params.get("type", [""])[0]
        if not type_code:
            self.respond({"status": "error", "reason": "missing_type"}, HTTPStatus.BAD_REQUEST)
            return

        try:
            payload = resolve_type(type_code)
            self.respond(payload)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            self.respond({"status": "error", "reason": str(exc)}, HTTPStatus.BAD_GATEWAY)
        except Exception as exc:  # pragma: no cover
            self.respond({"status": "error", "reason": str(exc)}, HTTPStatus.INTERNAL_SERVER_ERROR)

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def respond(self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--log-file", type=Path, default=DEFAULT_LOG_PATH)
    args = parser.parse_args()
    setup_logging(args.log_file)

    TYPE_DIR.mkdir(parents=True, exist_ok=True)
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    log(f"[cache] listening on http://{args.host}:{args.port}")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
