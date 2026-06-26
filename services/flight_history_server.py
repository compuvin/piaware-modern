#!/usr/bin/env python3
"""Log aircraft and flight paths to SQLite and expose a small history API."""

from __future__ import annotations

import argparse
import json
import logging
import math
import sqlite3
import threading
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen


SERVICE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = SERVICE_ROOT.parent
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "flight_history.sqlite3"
DEFAULT_LOG_PATH = PROJECT_ROOT / "logs" / "flight-history.log"
DEFAULT_SOURCE_URL = "http://127.0.0.1/skyaware/data/aircraft.json"
DEFAULT_SOURCE_URL_978 = ""
AIRCRAFT_DB_DIR = PROJECT_ROOT / "db"
AIRCRAFT_TYPE_DB_PATH = AIRCRAFT_DB_DIR / "aircraft_types" / "icao_aircraft_types.json"
AIRCRAFT_IMAGE_CACHE_DIR = PROJECT_ROOT / "assets" / "aircraft" / "types"
USER_AGENT = "piaware-modern-flight-history/1.0"
STATS_CACHE_KEY = "history_stats"
STATS_CACHE_TTL_SECONDS = 900
LOGGER = logging.getLogger("piaware-modern-history")


def log(message: str) -> None:
    LOGGER.info(message)


def configure_sqlite_connection(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA mmap_size=0")


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


def utc_now() -> int:
    return int(time.time())


def normalize_callsign(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def normalize_type(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def normalize_registration(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_m = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    return 2 * radius_m * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def fetch_json(source: str) -> dict[str, Any]:
    if source.startswith("/") or source.startswith("file://"):
        path = Path(source.removeprefix("file://"))
        with path.open() as handle:
            return json.load(handle)

    request = Request(source, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=10) as response:
        return json.load(response)


@dataclass
class LoggerConfig:
    db_path: Path
    source_url: str
    source_url_978: str | None
    poll_interval: float
    min_position_seconds: int
    min_position_distance_m: float
    flight_gap_seconds: int


class FlightHistoryStore:
    def __init__(self, db_path: Path, min_position_seconds: int, min_position_distance_m: float, flight_gap_seconds: int) -> None:
        self.db_path = db_path
        self.min_position_seconds = min_position_seconds
        self.min_position_distance_m = min_position_distance_m
        self.flight_gap_seconds = flight_gap_seconds
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        configure_sqlite_connection(self.conn)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.aircraft_metadata_cache: dict[str, dict[str, Any] | None] = {}
        self.aircraft_type_cache: dict[str, dict[str, Any]] | None = None
        self.stats_refresh_lock = threading.Lock()
        self.stats_refreshing = False
        self._init_schema()

    def _init_schema(self) -> None:
        with self.conn:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS aircraft (
                    icao TEXT PRIMARY KEY,
                    registration TEXT,
                    aircraft_type TEXT,
                    description TEXT,
                    category TEXT,
                    first_seen_ts INTEGER NOT NULL,
                    last_seen_ts INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS flights (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    icao TEXT NOT NULL,
                    callsign TEXT,
                    registration TEXT,
                    aircraft_type TEXT,
                    source TEXT,
                    start_ts INTEGER NOT NULL,
                    end_ts INTEGER,
                    first_seen_ts INTEGER NOT NULL,
                    last_seen_ts INTEGER NOT NULL,
                    first_lat REAL,
                    first_lon REAL,
                    last_lat REAL,
                    last_lon REAL,
                    min_altitude_ft REAL,
                    max_altitude_ft REAL,
                    position_count INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY (icao) REFERENCES aircraft (icao)
                );

                CREATE TABLE IF NOT EXISTS positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    flight_id INTEGER NOT NULL,
                    ts INTEGER NOT NULL,
                    lat REAL NOT NULL,
                    lon REAL NOT NULL,
                    altitude_ft REAL,
                    speed_kt REAL,
                    track_deg REAL,
                    vertical_rate_fpm REAL,
                    squawk TEXT,
                    source TEXT,
                    FOREIGN KEY (flight_id) REFERENCES flights (id)
                );

                CREATE INDEX IF NOT EXISTS idx_flights_icao_start ON flights (icao, start_ts DESC);
                CREATE INDEX IF NOT EXISTS idx_flights_last_seen ON flights (last_seen_ts DESC);
                CREATE INDEX IF NOT EXISTS idx_positions_flight_ts ON positions (flight_id, ts);
                CREATE INDEX IF NOT EXISTS idx_positions_ts ON positions (ts);
                CREATE INDEX IF NOT EXISTS idx_aircraft_last_seen ON aircraft (last_seen_ts DESC);

                CREATE TABLE IF NOT EXISTS history_stats_cache (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_ts INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS history_daily_stats (
                    day TEXT PRIMARY KEY,
                    positions INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS history_aircraft_stats (
                    icao TEXT PRIMARY KEY,
                    flights INTEGER NOT NULL DEFAULT 0,
                    positions INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS history_stats_meta (
                    key TEXT PRIMARY KEY,
                    value INTEGER NOT NULL
                );
                """
            )

    def ingest_snapshot(self, payload: dict[str, Any], source_name: str) -> None:
        snapshot_now = payload.get("now")
        if isinstance(snapshot_now, (int, float)):
            snapshot_ts = int(snapshot_now)
        else:
            snapshot_ts = utc_now()

        aircraft_list = payload.get("aircraft", [])
        if not isinstance(aircraft_list, list):
            return

        with self.lock:
            for entry in aircraft_list:
                if not isinstance(entry, dict):
                    continue
                self._ingest_aircraft(snapshot_ts, entry, source_name)
            self._close_stale_flights(snapshot_ts)
            self.conn.commit()

    def _ingest_aircraft(self, snapshot_ts: int, entry: dict[str, Any], source_name: str) -> None:
        icao = str(entry.get("hex", "")).strip().upper()
        if not icao:
            return

        seen = entry.get("seen")
        seen_age = int(seen) if isinstance(seen, (int, float)) else 0
        seen_ts = max(snapshot_ts - seen_age, 0)
        callsign = normalize_callsign(entry.get("flight"))
        registration = normalize_registration(entry.get("r"))
        aircraft_type = normalize_type(entry.get("t"))
        category = entry.get("category")
        description = entry.get("desc")
        squawk = entry.get("squawk")

        metadata = self._lookup_aircraft_metadata(icao)
        if metadata:
            registration = registration or normalize_registration(metadata.get("r"))
            aircraft_type = aircraft_type or normalize_type(metadata.get("t"))
            description = description or metadata.get("desc")

        if aircraft_type and not description:
            description = self._lookup_aircraft_type_description(aircraft_type)

        lat = entry.get("lat")
        lon = entry.get("lon")
        has_position = isinstance(lat, (int, float)) and isinstance(lon, (int, float))

        altitude_ft = self._pick_numeric(entry, "alt_baro", "alt_geom", "altitude")
        speed_kt = self._pick_numeric(entry, "gs", "tas", "ias", "speed")
        track_deg = self._pick_numeric(entry, "track", "nav_heading", "true_heading", "mag_heading")
        vertical_rate_fpm = self._pick_numeric(entry, "baro_rate", "geom_rate")

        self.conn.execute(
            """
            INSERT INTO aircraft (icao, registration, aircraft_type, description, category, first_seen_ts, last_seen_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(icao) DO UPDATE SET
                registration = COALESCE(excluded.registration, aircraft.registration),
                aircraft_type = COALESCE(excluded.aircraft_type, aircraft.aircraft_type),
                description = COALESCE(excluded.description, aircraft.description),
                category = COALESCE(excluded.category, aircraft.category),
                last_seen_ts = excluded.last_seen_ts
            """,
            (icao, registration, aircraft_type, description, category, seen_ts, seen_ts),
        )

        flight = self._get_open_flight(icao)
        if flight and (
            (callsign and flight["callsign"] and callsign != flight["callsign"])
            or (seen_ts - int(flight["last_seen_ts"])) > self.flight_gap_seconds
        ):
            self._close_flight(int(flight["id"]), int(flight["last_seen_ts"]))
            flight = None

        if not flight:
            flight_id = self._create_flight(
                icao=icao,
                callsign=callsign,
                registration=registration,
                aircraft_type=aircraft_type,
                source=source_name,
                start_ts=seen_ts,
                lat=float(lat) if has_position else None,
                lon=float(lon) if has_position else None,
                altitude_ft=altitude_ft,
            )
            log(f"[history] new flight {flight_id} for {icao} ({callsign or 'no callsign'})")
            flight = self._get_flight(flight_id)

        self.conn.execute(
            """
            UPDATE flights
            SET callsign = COALESCE(?, callsign),
                registration = COALESCE(?, registration),
                aircraft_type = COALESCE(?, aircraft_type),
                source = COALESCE(?, source),
                last_seen_ts = ?,
                end_ts = NULL
            WHERE id = ?
            """,
            (callsign, registration, aircraft_type, source_name, seen_ts, int(flight["id"])),
        )

        if has_position:
            self._maybe_insert_position(
                flight_id=int(flight["id"]),
                ts=seen_ts,
                lat=float(lat),
                lon=float(lon),
                altitude_ft=altitude_ft,
                speed_kt=speed_kt,
                track_deg=track_deg,
                vertical_rate_fpm=vertical_rate_fpm,
                squawk=str(squawk).strip() if squawk is not None else None,
                source=source_name,
            )

    def _pick_numeric(self, entry: dict[str, Any], *keys: str) -> float | None:
        for key in keys:
            value = entry.get(key)
            if isinstance(value, (int, float)):
                return float(value)
        return None

    def _load_aircraft_type_cache(self) -> dict[str, dict[str, Any]]:
        if self.aircraft_type_cache is not None:
            return self.aircraft_type_cache

        if not AIRCRAFT_TYPE_DB_PATH.exists():
            self.aircraft_type_cache = {}
            return self.aircraft_type_cache

        with AIRCRAFT_TYPE_DB_PATH.open() as handle:
            payload = json.load(handle)

        self.aircraft_type_cache = payload if isinstance(payload, dict) else {}
        return self.aircraft_type_cache

    def _lookup_aircraft_type_description(self, type_code: str) -> str | None:
        lookup = self._load_aircraft_type_cache()
        entry = lookup.get(type_code.upper())
        if not isinstance(entry, dict):
            return None
        description = entry.get("desc")
        return str(description).strip() if description else None

    def _lookup_aircraft_metadata(self, icao: str) -> dict[str, Any] | None:
        icao = icao.upper()
        if icao in self.aircraft_metadata_cache:
            return self.aircraft_metadata_cache[icao]

        result = self._lookup_aircraft_metadata_from_prefix_files(icao)
        self.aircraft_metadata_cache[icao] = result
        return result

    def _lookup_aircraft_metadata_from_prefix_files(self, icao: str) -> dict[str, Any] | None:
        max_prefix = min(3, len(icao) - 1)
        for prefix_len in range(max_prefix, 0, -1):
            prefix = icao[:prefix_len]
            suffix = icao[prefix_len:]
            db_path = AIRCRAFT_DB_DIR / f"{prefix}.json"
            if not db_path.exists():
                continue
            try:
                with db_path.open() as handle:
                    payload = json.load(handle)
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            value = payload.get(suffix)
            if isinstance(value, dict):
                return value
        return None

    def _metadata_for_icao(self, icao: str) -> dict[str, Any]:
        metadata = self._lookup_aircraft_metadata(icao) or {}
        registration = normalize_registration(metadata.get("r"))
        aircraft_type = normalize_type(metadata.get("t"))
        description = metadata.get("desc")
        if aircraft_type and not description:
            description = self._lookup_aircraft_type_description(aircraft_type)
        return {
            "registration": registration,
            "aircraft_type": aircraft_type,
            "description": str(description).strip() if description else None,
        }

    def _enrich_row_metadata(self, row: dict[str, Any]) -> dict[str, Any]:
        icao = row.get("icao")
        if not icao:
            return row
        metadata = self._metadata_for_icao(str(icao))
        changed = False
        for key in ("registration", "aircraft_type", "description"):
            if not row.get(key) and metadata.get(key):
                row[key] = metadata[key]
                changed = True

        if changed:
            with self.lock:
                if self.conn.in_transaction:
                    return row
                if "id" in row and row.get("id") is not None and "start_ts" in row:
                    self.conn.execute(
                        "UPDATE flights SET registration = COALESCE(registration, ?), aircraft_type = COALESCE(aircraft_type, ?) WHERE id = ?",
                        (row.get("registration"), row.get("aircraft_type"), row["id"]),
                    )
                self.conn.execute(
                    "UPDATE aircraft SET registration = COALESCE(registration, ?), aircraft_type = COALESCE(aircraft_type, ?), description = COALESCE(description, ?) WHERE icao = ?",
                    (row.get("registration"), row.get("aircraft_type"), row.get("description"), row["icao"]),
                )
                self.conn.commit()
        return row

    def _get_open_flight(self, icao: str) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT *
            FROM flights
            WHERE icao = ? AND end_ts IS NULL
            ORDER BY start_ts DESC
            LIMIT 1
            """,
            (icao,),
        ).fetchone()

    def _get_flight(self, flight_id: int) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM flights WHERE id = ?", (flight_id,)).fetchone()

    def _create_flight(
        self,
        *,
        icao: str,
        callsign: str | None,
        registration: str | None,
        aircraft_type: str | None,
        source: str,
        start_ts: int,
        lat: float | None,
        lon: float | None,
        altitude_ft: float | None,
    ) -> int:
        cursor = self.conn.execute(
            """
            INSERT INTO flights (
                icao, callsign, registration, aircraft_type, source, start_ts, first_seen_ts, last_seen_ts,
                first_lat, first_lon, last_lat, last_lon, min_altitude_ft, max_altitude_ft
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                icao,
                callsign,
                registration,
                aircraft_type,
                source,
                start_ts,
                start_ts,
                start_ts,
                lat,
                lon,
                lat,
                lon,
                altitude_ft,
                altitude_ft,
            ),
        )
        return int(cursor.lastrowid)

    def _close_flight(self, flight_id: int, end_ts: int) -> None:
        self.conn.execute("UPDATE flights SET end_ts = COALESCE(end_ts, ?), last_seen_ts = ? WHERE id = ?", (end_ts, end_ts, flight_id))

    def _last_position(self, flight_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT ts, lat, lon, altitude_ft, track_deg
            FROM positions
            WHERE flight_id = ?
            ORDER BY ts DESC, id DESC
            LIMIT 1
            """,
            (flight_id,),
        ).fetchone()

    def _maybe_insert_position(
        self,
        *,
        flight_id: int,
        ts: int,
        lat: float,
        lon: float,
        altitude_ft: float | None,
        speed_kt: float | None,
        track_deg: float | None,
        vertical_rate_fpm: float | None,
        squawk: str | None,
        source: str,
    ) -> None:
        previous = self._last_position(flight_id)
        if previous:
            seconds_delta = ts - int(previous["ts"])
            distance_delta = haversine_meters(lat, lon, float(previous["lat"]), float(previous["lon"]))
            altitude_delta = abs((altitude_ft or 0.0) - (float(previous["altitude_ft"]) if previous["altitude_ft"] is not None else 0.0))
            track_delta = abs((track_deg or 0.0) - (float(previous["track_deg"]) if previous["track_deg"] is not None else 0.0))
            if (
                seconds_delta < self.min_position_seconds
                and distance_delta < self.min_position_distance_m
                and altitude_delta < 500
                and track_delta < 15
            ):
                self.conn.execute(
                    """
                    UPDATE flights
                    SET last_lat = ?, last_lon = ?, last_seen_ts = ?,
                        min_altitude_ft = COALESCE(MIN(min_altitude_ft, ?), ?),
                        max_altitude_ft = COALESCE(MAX(max_altitude_ft, ?), ?)
                    WHERE id = ?
                    """,
                    (lat, lon, ts, altitude_ft, altitude_ft, altitude_ft, altitude_ft, flight_id),
                )
                return

        self.conn.execute(
            """
            INSERT INTO positions (flight_id, ts, lat, lon, altitude_ft, speed_kt, track_deg, vertical_rate_fpm, squawk, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (flight_id, ts, lat, lon, altitude_ft, speed_kt, track_deg, vertical_rate_fpm, squawk, source),
        )
        self.conn.execute(
            """
            UPDATE flights
            SET last_lat = ?, last_lon = ?, last_seen_ts = ?,
                min_altitude_ft = CASE
                    WHEN min_altitude_ft IS NULL THEN ?
                    WHEN ? IS NULL THEN min_altitude_ft
                    ELSE MIN(min_altitude_ft, ?)
                END,
                max_altitude_ft = CASE
                    WHEN max_altitude_ft IS NULL THEN ?
                    WHEN ? IS NULL THEN max_altitude_ft
                    ELSE MAX(max_altitude_ft, ?)
                END,
                position_count = position_count + 1
            WHERE id = ?
            """,
            (
                lat,
                lon,
                ts,
                altitude_ft,
                altitude_ft,
                altitude_ft,
                altitude_ft,
                altitude_ft,
                altitude_ft,
                flight_id,
            ),
        )

    def _close_stale_flights(self, now_ts: int) -> None:
        self.conn.execute(
            """
            UPDATE flights
            SET end_ts = last_seen_ts
            WHERE end_ts IS NULL AND (? - last_seen_ts) > ?
            """,
            (now_ts, self.flight_gap_seconds),
        )

    def summary(self) -> dict[str, int]:
        with self.lock:
            aircraft_count = int(self.conn.execute("SELECT COUNT(*) FROM aircraft").fetchone()[0])
            flight_count = int(self.conn.execute("SELECT COUNT(*) FROM flights").fetchone()[0])
            position_count = int(self.conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0])
        return {
            "aircraft_count": aircraft_count,
            "flight_count": flight_count,
            "position_count": position_count,
        }

    def stats(self) -> dict[str, Any]:
        cached = self._read_stats_cache()
        now = utc_now()
        if cached and (now - int(cached.get("stats_cache_updated_ts") or 0)) > STATS_CACHE_TTL_SECONDS:
            self.refresh_stats_cache_async()

        if not cached:
            self.refresh_stats_cache_async()
            cached = self._empty_stats_payload("building")

        payload = dict(cached)
        payload.update(self._stats_file_sizes())
        payload["stats_cache_refreshing"] = self.stats_refreshing
        return payload

    def refresh_stats_cache_async(self, *, force: bool = False) -> None:
        if not force:
            cached = self._read_stats_cache()
            if cached and (utc_now() - int(cached.get("stats_cache_updated_ts") or 0)) <= STATS_CACHE_TTL_SECONDS:
                return

        with self.stats_refresh_lock:
            if self.stats_refreshing:
                return
            self.stats_refreshing = True

        thread = threading.Thread(target=self._refresh_stats_cache, name="history-stats-cache-refresh", daemon=True)
        thread.start()

    def _refresh_stats_cache(self) -> None:
        started = time.perf_counter()
        try:
            payload = self._build_stats_payload()
            encoded = json.dumps(payload, separators=(",", ":"))
            with self.lock:
                self.conn.execute(
                    """
                    INSERT INTO history_stats_cache (key, value_json, updated_ts)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value_json = excluded.value_json,
                        updated_ts = excluded.updated_ts
                    """,
                    (STATS_CACHE_KEY, encoded, int(payload["stats_cache_updated_ts"])),
                )
                self.conn.commit()
            log(f"[history] refreshed stats cache in {time.perf_counter() - started:.1f}s")
        except Exception as exc:
            LOGGER.exception("[history] failed to refresh stats cache: %s", exc)
        finally:
            with self.stats_refresh_lock:
                self.stats_refreshing = False

    def _read_stats_cache(self) -> dict[str, Any] | None:
        with self.lock:
            row = self.conn.execute(
                "SELECT value_json, updated_ts FROM history_stats_cache WHERE key = ?",
                (STATS_CACHE_KEY,),
            ).fetchone()
        if not row:
            return None
        try:
            payload = json.loads(row["value_json"])
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict):
            return None
        payload["stats_cache_updated_ts"] = int(row["updated_ts"])
        payload["stats_cache_status"] = "ready"
        return payload

    def _empty_stats_payload(self, status: str) -> dict[str, Any]:
        return {
            "aircraft_count": 0,
            "flight_count": 0,
            "position_count": 0,
            "db_internal_bytes": 0,
            "oldest_aircraft_ts": None,
            "oldest_flight_ts": None,
            "oldest_position_ts": None,
            "newest_aircraft_ts": None,
            "newest_flight_ts": None,
            "newest_position_ts": None,
            "positions_per_day": [],
            "busy_aircraft": [],
            "unique_week_aircraft": [],
            "stats_cache_updated_ts": None,
            "stats_cache_status": status,
        }

    def _stats_file_sizes(self) -> dict[str, int]:
        wal_path = Path(str(self.db_path) + "-wal")
        shm_path = Path(str(self.db_path) + "-shm")
        db_size = self.db_path.stat().st_size if self.db_path.exists() else 0
        wal_size = wal_path.stat().st_size if wal_path.exists() else 0
        shm_size = shm_path.stat().st_size if shm_path.exists() else 0
        image_cache_size = 0
        if AIRCRAFT_IMAGE_CACHE_DIR.exists():
            image_cache_size = sum(
                path.stat().st_size for path in AIRCRAFT_IMAGE_CACHE_DIR.rglob("*") if path.is_file()
            )
        return {
            "db_size_bytes": db_size,
            "wal_size_bytes": wal_size,
            "shm_size_bytes": shm_size,
            "image_cache_size_bytes": image_cache_size,
        }

    def _meta_int(self, conn: sqlite3.Connection, key: str, default: int = 0) -> int:
        row = conn.execute("SELECT value FROM history_stats_meta WHERE key = ?", (key,)).fetchone()
        return int(row["value"]) if row else default

    def _set_meta_int(self, conn: sqlite3.Connection, key: str, value: int) -> None:
        conn.execute(
            """
            INSERT INTO history_stats_meta (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, int(value)),
        )

    def _bootstrap_stats_rollups(self, conn: sqlite3.Connection) -> None:
        log("[history] bootstrapping stats rollups from existing history")
        started = time.perf_counter()
        with conn:
            conn.execute("DELETE FROM history_daily_stats")
            conn.execute("DELETE FROM history_aircraft_stats")
            conn.execute(
                """
                INSERT INTO history_daily_stats (day, positions)
                SELECT
                    strftime('%Y-%m-%d', datetime(ts, 'unixepoch', 'localtime')) AS day,
                    COUNT(*) AS positions
                FROM positions
                GROUP BY day
                """
            )
            conn.execute(
                """
                INSERT INTO history_aircraft_stats (icao, flights, positions)
                SELECT
                    a.icao,
                    COUNT(f.id) AS flights,
                    COALESCE(SUM(f.position_count), 0) AS positions
                FROM aircraft a
                LEFT JOIN flights f ON f.icao = a.icao
                GROUP BY a.icao
                """
            )

            aircraft_count = int(conn.execute("SELECT COUNT(*) FROM history_aircraft_stats").fetchone()[0])
            flight_count = int(conn.execute("SELECT COUNT(*) FROM flights").fetchone()[0])
            position_count = int(conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0])
            last_flight_id = int(conn.execute("SELECT COALESCE(MAX(id), 0) FROM flights").fetchone()[0])
            last_position_id = int(conn.execute("SELECT COALESCE(MAX(id), 0) FROM positions").fetchone()[0])
            oldest = conn.execute(
                """
                SELECT
                    (SELECT MIN(first_seen_ts) FROM aircraft) AS oldest_aircraft_ts,
                    (SELECT MIN(start_ts) FROM flights) AS oldest_flight_ts,
                    (SELECT MIN(ts) FROM positions) AS oldest_position_ts
                """
            ).fetchone()
            newest = conn.execute(
                """
                SELECT
                    (SELECT MAX(last_seen_ts) FROM aircraft) AS newest_aircraft_ts,
                    (SELECT MAX(last_seen_ts) FROM flights) AS newest_flight_ts,
                    (SELECT MAX(ts) FROM positions) AS newest_position_ts
                """
            ).fetchone()

            self._set_meta_int(conn, "aircraft_count", aircraft_count)
            self._set_meta_int(conn, "flight_count", flight_count)
            self._set_meta_int(conn, "position_count", position_count)
            self._set_meta_int(conn, "last_flight_id", last_flight_id)
            self._set_meta_int(conn, "last_position_id", last_position_id)
            self._set_meta_int(conn, "oldest_aircraft_ts", oldest["oldest_aircraft_ts"] or 0)
            self._set_meta_int(conn, "oldest_flight_ts", oldest["oldest_flight_ts"] or 0)
            self._set_meta_int(conn, "oldest_position_ts", oldest["oldest_position_ts"] or 0)
            self._set_meta_int(conn, "newest_aircraft_ts", newest["newest_aircraft_ts"] or 0)
            self._set_meta_int(conn, "newest_flight_ts", newest["newest_flight_ts"] or 0)
            self._set_meta_int(conn, "newest_position_ts", newest["newest_position_ts"] or 0)
            self._set_meta_int(conn, "rollups_bootstrapped", 1)
        log(f"[history] bootstrapped stats rollups in {time.perf_counter() - started:.1f}s")

    def _update_stats_rollups(self, conn: sqlite3.Connection) -> None:
        if not self._meta_int(conn, "rollups_bootstrapped"):
            self._bootstrap_stats_rollups(conn)
            return

        last_flight_id = self._meta_int(conn, "last_flight_id")
        last_position_id = self._meta_int(conn, "last_position_id")
        current_flight_id = int(conn.execute("SELECT COALESCE(MAX(id), 0) FROM flights").fetchone()[0])
        current_position_id = int(conn.execute("SELECT COALESCE(MAX(id), 0) FROM positions").fetchone()[0])

        with conn:
            new_aircraft = conn.execute(
                """
                SELECT COUNT(*) AS count,
                       MIN(first_seen_ts) AS oldest_aircraft_ts,
                       MAX(last_seen_ts) AS newest_aircraft_ts
                FROM aircraft
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM history_aircraft_stats s
                    WHERE s.icao = aircraft.icao
                )
                """
            ).fetchone()
            new_aircraft_count = int(new_aircraft["count"] or 0)
            if new_aircraft_count:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO history_aircraft_stats (icao, flights, positions)
                    SELECT icao, 0, 0
                    FROM aircraft
                    """
                )
                self._set_meta_int(conn, "aircraft_count", self._meta_int(conn, "aircraft_count") + new_aircraft_count)
                oldest_aircraft_ts = int(new_aircraft["oldest_aircraft_ts"] or 0)
                if oldest_aircraft_ts:
                    previous = self._meta_int(conn, "oldest_aircraft_ts")
                    self._set_meta_int(conn, "oldest_aircraft_ts", min(previous, oldest_aircraft_ts) if previous else oldest_aircraft_ts)

            previous_newest_aircraft_ts = self._meta_int(conn, "newest_aircraft_ts")
            newest_aircraft = conn.execute(
                "SELECT MAX(last_seen_ts) FROM aircraft WHERE last_seen_ts > ?",
                (previous_newest_aircraft_ts,),
            ).fetchone()[0]
            if newest_aircraft:
                self._set_meta_int(conn, "newest_aircraft_ts", int(newest_aircraft))

            if current_flight_id > last_flight_id:
                flight_summary = conn.execute(
                    """
                    SELECT COUNT(*) AS count,
                           MIN(start_ts) AS oldest_flight_ts,
                           MAX(last_seen_ts) AS newest_flight_ts
                    FROM flights
                    WHERE id > ?
                    """,
                    (last_flight_id,),
                ).fetchone()
                new_flight_count = int(flight_summary["count"] or 0)
                self._set_meta_int(conn, "flight_count", self._meta_int(conn, "flight_count") + new_flight_count)
                if flight_summary["oldest_flight_ts"]:
                    previous = self._meta_int(conn, "oldest_flight_ts")
                    oldest_flight_ts = int(flight_summary["oldest_flight_ts"])
                    self._set_meta_int(conn, "oldest_flight_ts", min(previous, oldest_flight_ts) if previous else oldest_flight_ts)
                if flight_summary["newest_flight_ts"]:
                    self._set_meta_int(
                        conn,
                        "newest_flight_ts",
                        max(self._meta_int(conn, "newest_flight_ts"), int(flight_summary["newest_flight_ts"])),
                    )

                for row in conn.execute(
                    """
                    SELECT icao, COUNT(*) AS flights
                    FROM flights
                    WHERE id > ?
                    GROUP BY icao
                    """,
                    (last_flight_id,),
                ):
                    conn.execute(
                        """
                        INSERT INTO history_aircraft_stats (icao, flights, positions)
                        VALUES (?, ?, 0)
                        ON CONFLICT(icao) DO UPDATE SET
                            flights = flights + excluded.flights
                        """,
                        (row["icao"], int(row["flights"])),
                    )
                self._set_meta_int(conn, "last_flight_id", current_flight_id)

            if current_position_id > last_position_id:
                position_summary = conn.execute(
                    """
                    SELECT COUNT(*) AS count,
                           MIN(ts) AS oldest_position_ts,
                           MAX(ts) AS newest_position_ts
                    FROM positions
                    WHERE id > ?
                    """,
                    (last_position_id,),
                ).fetchone()
                new_position_count = int(position_summary["count"] or 0)
                self._set_meta_int(conn, "position_count", self._meta_int(conn, "position_count") + new_position_count)
                if position_summary["oldest_position_ts"]:
                    previous = self._meta_int(conn, "oldest_position_ts")
                    oldest_position_ts = int(position_summary["oldest_position_ts"])
                    self._set_meta_int(conn, "oldest_position_ts", min(previous, oldest_position_ts) if previous else oldest_position_ts)
                if position_summary["newest_position_ts"]:
                    self._set_meta_int(
                        conn,
                        "newest_position_ts",
                        max(self._meta_int(conn, "newest_position_ts"), int(position_summary["newest_position_ts"])),
                    )

                for row in conn.execute(
                    """
                    SELECT
                        strftime('%Y-%m-%d', datetime(ts, 'unixepoch', 'localtime')) AS day,
                        COUNT(*) AS positions
                    FROM positions
                    WHERE id > ?
                    GROUP BY day
                    """,
                    (last_position_id,),
                ):
                    conn.execute(
                        """
                        INSERT INTO history_daily_stats (day, positions)
                        VALUES (?, ?)
                        ON CONFLICT(day) DO UPDATE SET
                            positions = positions + excluded.positions
                        """,
                        (row["day"], int(row["positions"])),
                    )

                for row in conn.execute(
                    """
                    SELECT f.icao, COUNT(*) AS positions
                    FROM positions p
                    JOIN flights f ON f.id = p.flight_id
                    WHERE p.id > ?
                    GROUP BY f.icao
                    """,
                    (last_position_id,),
                ):
                    conn.execute(
                        """
                        INSERT INTO history_aircraft_stats (icao, flights, positions)
                        VALUES (?, 0, ?)
                        ON CONFLICT(icao) DO UPDATE SET
                            positions = positions + excluded.positions
                        """,
                        (row["icao"], int(row["positions"])),
                    )
                self._set_meta_int(conn, "last_position_id", current_position_id)

    def _build_stats_payload(self) -> dict[str, Any]:
        week_cutoff = int(time.time()) - (7 * 24 * 60 * 60)
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        configure_sqlite_connection(conn)
        try:
            self._update_stats_rollups(conn)
            aircraft_count = self._meta_int(conn, "aircraft_count")
            flight_count = self._meta_int(conn, "flight_count")
            position_count = self._meta_int(conn, "position_count")
            pages = conn.execute("PRAGMA page_count").fetchone()[0]
            page_size = conn.execute("PRAGMA page_size").fetchone()[0]

            recent_days = conn.execute(
                """
                SELECT day, positions
                FROM history_daily_stats
                ORDER BY day DESC
                LIMIT 14
                """
            ).fetchall()

            busy_aircraft = conn.execute(
                """
                SELECT
                    a.icao,
                    a.registration,
                    a.aircraft_type,
                    a.description,
                    s.flights,
                    s.positions
                FROM history_aircraft_stats s
                LEFT JOIN aircraft a ON a.icao = s.icao
                ORDER BY s.positions DESC, s.flights DESC
                LIMIT 10
                """
            ).fetchall()

            unique_week_aircraft = conn.execute(
                """
                WITH recent_aircraft AS (
                    SELECT
                        a.icao,
                        a.registration,
                        a.aircraft_type,
                        a.description,
                        a.last_seen_ts
                    FROM aircraft a
                    WHERE a.last_seen_ts >= ?
                ),
                type_totals AS (
                    SELECT
                        COALESCE(NULLIF(aircraft_type, ''), 'UNKNOWN') AS type_key,
                        COUNT(*) AS aircraft_seen
                    FROM recent_aircraft
                    GROUP BY COALESCE(NULLIF(aircraft_type, ''), 'UNKNOWN')
                )
                SELECT
                    ra.icao,
                    ra.registration,
                    ra.aircraft_type,
                    ra.description,
                    ra.last_seen_ts,
                    tt.aircraft_seen AS type_seen_count
                FROM recent_aircraft ra
                JOIN type_totals tt
                  ON tt.type_key = COALESCE(NULLIF(ra.aircraft_type, ''), 'UNKNOWN')
                ORDER BY tt.aircraft_seen ASC, ra.last_seen_ts DESC, ra.icao ASC
                LIMIT 15
                """,
                (week_cutoff,),
            ).fetchall()
            oldest_aircraft_ts = self._meta_int(conn, "oldest_aircraft_ts") or None
            oldest_flight_ts = self._meta_int(conn, "oldest_flight_ts") or None
            oldest_position_ts = self._meta_int(conn, "oldest_position_ts") or None
            newest_aircraft_ts = self._meta_int(conn, "newest_aircraft_ts") or None
            newest_flight_ts = self._meta_int(conn, "newest_flight_ts") or None
            newest_position_ts = self._meta_int(conn, "newest_position_ts") or None
        finally:
            conn.close()

        return {
            "aircraft_count": aircraft_count,
            "flight_count": flight_count,
            "position_count": position_count,
            "db_internal_bytes": pages * page_size,
            "oldest_aircraft_ts": oldest_aircraft_ts,
            "oldest_flight_ts": oldest_flight_ts,
            "oldest_position_ts": oldest_position_ts,
            "newest_aircraft_ts": newest_aircraft_ts,
            "newest_flight_ts": newest_flight_ts,
            "newest_position_ts": newest_position_ts,
            "positions_per_day": [dict(row) for row in recent_days],
            "busy_aircraft": [self._enrich_row_metadata_from_cache(dict(row)) for row in busy_aircraft],
            "unique_week_aircraft": [self._enrich_row_metadata_from_cache(dict(row)) for row in unique_week_aircraft],
            "stats_cache_updated_ts": utc_now(),
            "stats_cache_status": "ready",
        }

    def _enrich_row_metadata_from_cache(self, row: dict[str, Any]) -> dict[str, Any]:
        icao = row.get("icao")
        if not icao:
            return row
        metadata = self._metadata_for_icao(str(icao))
        for key in ("registration", "aircraft_type", "description"):
            if not row.get(key) and metadata.get(key):
                row[key] = metadata[key]
        return row

    def list_aircraft(self, limit: int, search: str | None) -> list[dict[str, Any]]:
        query = """
            SELECT
                a.icao,
                a.registration,
                a.aircraft_type,
                a.description,
                a.category,
                a.first_seen_ts,
                a.last_seen_ts,
                (
                    SELECT COUNT(*)
                    FROM flights f
                    WHERE f.icao = a.icao
                ) AS flight_count
            FROM aircraft a
        """
        params: list[Any] = []
        if search:
            query += """
                WHERE a.icao LIKE ?
                   OR a.registration LIKE ?
                   OR a.aircraft_type LIKE ?
                   OR a.description LIKE ?
                   OR EXISTS (
                        SELECT 1
                        FROM flights f
                        WHERE f.icao = a.icao
                          AND f.callsign LIKE ?
                   )
            """
            wildcard = f"%{search.upper()}%"
            params.extend([wildcard, wildcard, wildcard, wildcard, wildcard])
        query += " ORDER BY a.last_seen_ts DESC LIMIT ?"
        params.append(limit)
        with self.lock:
            rows = self.conn.execute(query, params).fetchall()
        return [self._enrich_row_metadata(dict(row)) for row in rows]

    def list_flights(self, icao: str, limit: int) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT *
                FROM flights
                WHERE icao = ?
                ORDER BY start_ts DESC
                LIMIT ?
                """,
                (icao.upper(), limit),
            ).fetchall()
        return [self._enrich_row_metadata(dict(row)) for row in rows]

    def get_flight(self, flight_id: int) -> dict[str, Any] | None:
        with self.lock:
            row = self.conn.execute("SELECT * FROM flights WHERE id = ?", (flight_id,)).fetchone()
        return self._enrich_row_metadata(dict(row)) if row else None

    def get_flight_path(self, flight_id: int) -> dict[str, Any]:
        with self.lock:
            flight = self.conn.execute("SELECT * FROM flights WHERE id = ?", (flight_id,)).fetchone()
            rows = self.conn.execute(
                """
                SELECT ts, lat, lon, altitude_ft, speed_kt, track_deg, vertical_rate_fpm, squawk, source
                FROM positions
                WHERE flight_id = ?
                ORDER BY ts ASC, id ASC
                """,
                (flight_id,),
            ).fetchall()
        if not flight:
            return {"flight": None, "positions": [], "geojson": None}

        flight_dict = self._enrich_row_metadata(dict(flight))
        positions = [dict(row) for row in rows]
        geojson = {
            "type": "Feature",
            "properties": {
                "flight_id": flight_id,
                "icao": flight_dict["icao"],
                "callsign": flight_dict["callsign"],
            },
            "geometry": {
                "type": "LineString",
                "coordinates": [[row["lon"], row["lat"]] for row in rows],
            },
        }
        return {"flight": flight_dict, "positions": positions, "geojson": geojson}

    def get_recent_paths(self, since_ts: int, limit: int) -> dict[str, Any]:
        with self.lock:
            flights = self.conn.execute(
                """
                SELECT id, icao, callsign, aircraft_type, registration, start_ts, end_ts, last_seen_ts, position_count
                FROM flights
                WHERE last_seen_ts >= ? AND position_count > 1
                ORDER BY last_seen_ts DESC
                LIMIT ?
                """,
                (since_ts, limit),
            ).fetchall()

            flight_ids = [int(row["id"]) for row in flights]
            if not flight_ids:
                return {"since_ts": since_ts, "flights": [], "geojson": {"type": "FeatureCollection", "features": []}}

            placeholders = ",".join("?" for _ in flight_ids)
            positions = self.conn.execute(
                f"""
                SELECT flight_id, ts, lat, lon
                FROM positions
                WHERE flight_id IN ({placeholders})
                ORDER BY flight_id ASC, ts ASC, id ASC
                """,
                flight_ids,
            ).fetchall()

        grouped: dict[int, list[list[float]]] = {}
        for row in positions:
            grouped.setdefault(int(row["flight_id"]), []).append([float(row["lon"]), float(row["lat"])])

        features = []
        for flight in flights:
            flight_id = int(flight["id"])
            coordinates = grouped.get(flight_id, [])
            if len(coordinates) < 2:
                continue
            features.append(
                {
                    "type": "Feature",
                    "properties": {
                        "flight_id": flight_id,
                        "icao": flight["icao"],
                        "callsign": flight["callsign"],
                        "aircraft_type": flight["aircraft_type"],
                        "registration": flight["registration"],
                        "start_ts": flight["start_ts"],
                        "last_seen_ts": flight["last_seen_ts"],
                        "position_count": flight["position_count"],
                    },
                    "geometry": {
                        "type": "LineString",
                        "coordinates": coordinates,
                    },
                }
            )

        return {
            "since_ts": since_ts,
            "flights": [dict(row) for row in flights],
            "geojson": {
                "type": "FeatureCollection",
                "features": features,
            },
        }


class Poller(threading.Thread):
    def __init__(self, store: FlightHistoryStore, config: LoggerConfig) -> None:
        super().__init__(daemon=True)
        self.store = store
        self.config = config
        self.stop_event = threading.Event()
        self.sources: list[dict[str, Any]] = [{"name": "dump1090-fa", "url": self.config.source_url, "enabled": True}]
        if self.config.source_url_978:
            self.sources.append({"name": "skyaware978", "url": self.config.source_url_978, "enabled": True})

    def run(self) -> None:
        while not self.stop_event.is_set():
            for source in self.sources:
                if not source["enabled"]:
                    continue
                try:
                    payload = fetch_json(source["url"])
                    self.store.ingest_snapshot(payload, source["name"])
                except HTTPError as exc:
                    if exc.code == 404:
                        source["enabled"] = False
                        log(f"[history] disabling {source['name']} polling after HTTP 404 at {source['url']}")
                    else:
                        log(f"[history] fetch failed for {source['name']}: {exc}")
                except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
                    log(f"[history] fetch failed for {source['name']}: {exc}")
                except Exception as exc:  # pragma: no cover
                    log(f"[history] unexpected error for {source['name']}: {exc}")
            self.stop_event.wait(self.config.poll_interval)


class StatsCacheRefresher(threading.Thread):
    def __init__(self, store: FlightHistoryStore, interval_seconds: float) -> None:
        super().__init__(daemon=True)
        self.store = store
        self.interval_seconds = max(60.0, interval_seconds)
        self.stop_event = threading.Event()

    def run(self) -> None:
        while not self.stop_event.wait(self.interval_seconds):
            self.store._refresh_stats_cache()


class Handler(BaseHTTPRequestHandler):
    store: FlightHistoryStore | None = None

    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        super().end_headers()

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self.end_headers()

    def do_GET(self) -> None:
        if not self.store:
            self._respond({"status": "error", "reason": "store_unavailable"}, HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if parsed.path == "/health":
            self._respond({"status": "ok", **self.store.summary()})
            return

        if parsed.path == "/api/history/summary":
            self._respond(self.store.summary())
            return

        if parsed.path == "/api/history/stats":
            self._respond(self.store.stats())
            return

        if parsed.path == "/api/history/aircraft":
            limit = max(1, min(int(params.get("limit", ["250"])[0]), 1000))
            search = params.get("search", [""])[0].strip() or None
            self._respond({"aircraft": self.store.list_aircraft(limit, search)})
            return

        if parsed.path == "/api/history/flights":
            icao = params.get("icao", [""])[0].strip().upper()
            if not icao:
                self._respond({"status": "error", "reason": "missing_icao"}, HTTPStatus.BAD_REQUEST)
                return
            limit = max(1, min(int(params.get("limit", ["100"])[0]), 1000))
            self._respond({"flights": self.store.list_flights(icao, limit)})
            return

        if parsed.path == "/api/history/recent-paths":
            hours = params.get("hours", [""])[0].strip()
            days = params.get("days", [""])[0].strip()
            limit = max(1, min(int(params.get("limit", ["2000"])[0]), 2000))
            now_ts = utc_now()
            if days:
                since_ts = now_ts - int(float(days) * 86400)
            else:
                since_ts = now_ts - int(float(hours or "24") * 3600)
            self._respond(self.store.get_recent_paths(since_ts, limit))
            return

        if parsed.path.startswith("/api/history/flight/"):
            suffix = parsed.path.removeprefix("/api/history/flight/")
            if suffix.endswith("/path"):
                flight_id_text = suffix.removesuffix("/path")
                if not flight_id_text.isdigit():
                    self._respond({"status": "error", "reason": "invalid_flight_id"}, HTTPStatus.BAD_REQUEST)
                    return
                self._respond(self.store.get_flight_path(int(flight_id_text)))
                return

            if suffix.isdigit():
                flight = self.store.get_flight(int(suffix))
                if not flight:
                    self._respond({"status": "error", "reason": "not_found"}, HTTPStatus.NOT_FOUND)
                    return
                self._respond({"flight": flight})
                return

        self._respond({"status": "error", "reason": "not_found"}, HTTPStatus.NOT_FOUND)

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def _respond(self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8766)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--log-file", type=Path, default=DEFAULT_LOG_PATH)
    parser.add_argument("--source-url", default=DEFAULT_SOURCE_URL)
    parser.add_argument("--source-url-978", default=DEFAULT_SOURCE_URL_978)
    parser.add_argument("--poll-interval", type=float, default=2.0)
    parser.add_argument("--min-position-seconds", type=int, default=20)
    parser.add_argument("--min-position-distance-m", type=float, default=750.0)
    parser.add_argument("--flight-gap-seconds", type=int, default=1800)
    args = parser.parse_args()
    setup_logging(args.log_file)

    config = LoggerConfig(
        db_path=args.db_path,
        source_url=args.source_url,
        source_url_978=args.source_url_978 or None,
        poll_interval=args.poll_interval,
        min_position_seconds=args.min_position_seconds,
        min_position_distance_m=args.min_position_distance_m,
        flight_gap_seconds=args.flight_gap_seconds,
    )

    config.db_path.parent.mkdir(parents=True, exist_ok=True)
    store = FlightHistoryStore(
        db_path=config.db_path,
        min_position_seconds=config.min_position_seconds,
        min_position_distance_m=config.min_position_distance_m,
        flight_gap_seconds=config.flight_gap_seconds,
    )
    store._refresh_stats_cache()
    stats_refresher = StatsCacheRefresher(store, STATS_CACHE_TTL_SECONDS)
    stats_refresher.start()
    poller = Poller(store, config)
    poller.start()

    Handler.store = store
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    log(f"[history] listening on http://{args.host}:{args.port}")
    log(f"[history] source 1090: {config.source_url}")
    if config.source_url_978:
        log(f"[history] source 978: {config.source_url_978}")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
