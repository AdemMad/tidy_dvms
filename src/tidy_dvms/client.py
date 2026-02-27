from __future__ import annotations
import io
import json
import time
import typing as t
import warnings
import xml.etree.ElementTree as ET

import duckdb
import polars as pl
import requests

# from .transform import transform_fixtures, physical_splits, physical_summary
from tidy_dvms.transform import transform_fixtures, physical_splits, physical_summary

warnings.filterwarnings("ignore")


class DVMS:
    BASE_URL = "https://dvms.premierleague.com"
    AUTH_URL = f"{BASE_URL}/api/v2/authenticate"

    # Tracking: 38, Metadata: 40, Physical Splits: 42, Physical Total/Summary: 43
    SUBTYPE_TRACKING = 38
    SUBTYPE_EVENTS = 20
    SUBTYPE_METADATA = 40
    SUBTYPE_SPLITS = 42
    SUBTYPE_SUMMARY = 43

    EVENT_TYPES = {
        1: "Pass",
        2: "Offside Pass",
        3: "Take On",
        4: "Foul",
        5: "Out",
        6: "Corner Awarded",
        7: "Tackle",
        8: "Interception",
        10: "Save",
        11: "Claim",
        12: "Clearance",
        13: "Miss",
        14: "Post",
        15: "Attempt Saved",
        16: "Goal",
        17: "Card",
        18: "Player Off",
        19: "Player on",
        20: "Player retired",
        21: "Player returns",
        22: "Player becomes goalkeeper",
        23: "Goalkeeper becomes player",
        24: "Condition change",
        25: "Official change",
        27: "Start delay",
        28: "End delay",
        30: "End",
        32: "Start",
        34: "Team set up",
        36: "Player changed Jersey number",
        37: "Collection End",
        38: "Temp_Goal",
        39: "Temp_Attempt",
        40: "Formation change",
        41: "Punch",
        42: "Good skill",
        43: "Deleted event",
        44: "Aerial",
        45: "Challenge",
        49: "Ball recovery",
        50: "Dispossessed",
        51: "Error",
        52: "Keeper pick-up",
        53: "Cross not claimed",
        54: "Smother",
        55: "Offside provoked",
        56: "Shield ball opp",
        57: "Foul throw-in",
        58: "Penalty faced",
        59: "Keeper Sweeper",
        60: "Chance missed",
        61: "Ball touch",
        63: "Temp_Save",
        64: "Resume",
        65: "Contentious referee decision",
        67: "50/50",
        68: "Referee Drop Ball",
        70: "Injury Time Announcement",
        71: "Coach Setup",
        74: "Blocked Pass",
        75: "Delayed Start",
        76: "Early end",
        79: "Coverage interruption",
        80: "Drop of Ball",
        81: "Obstacle",
        82: "Control",
        83: "Attempted Tackle",
        84: "Deleted After Review",
    }

    COMP_MAP = {
        "English Premier League": "8",
        "EFL Championship": "10",
        "EFL Cup": "2",
        "FA Cup": "1",
    }

    def __init__(
        self,
        season: int,
        competition_name: str,
        username: str,
        password: str,
        *,
        request_timeout: int = 30,
        request_retries: int = 3,
        sleep_between_retries: float = 1.0,
    ) -> None:
        self.season_id = season
        self.competition_name = competition_name

        self._timeout = request_timeout
        self._retries = request_retries
        self._sleep = sleep_between_retries

        token = self._get_api_key(username, password)
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Hudl-AuthToken": token,
        }

        # Caches populated by fixtures()
        self._competition_id: str | None = None
        self._opta_competition_id: str | None = None
        self._fixtures_df: pl.DataFrame | None = None
        self._fixtures_list: list[dict] | None = None
        self._fixture_assets: list[dict] | None = None

    # ============================
    # Public API (your requested UX)
    # ============================

    def fixtures(self, *, format: str = "dataframe"):
        """
        Fetch fixtures for the season/competition set in __init__ and cache assets.

        Args:
            format: "dataframe" (default) -> returns Polars DataFrame
                    "json"               -> returns Python list[dict] (raw JSON payload)

        Side effects:
            - self._fixtures_df      (Polars DataFrame)
            - self._fixtures_list    (list[dict])
            - self._fixture_assets   (list[dict])
            - self._competition_id / self._opta_competition_id
            - self.fixtures_json_text (str) pretty JSON string for convenience
        """
        comp = self._resolve_competition(self.competition_name)
        self._competition_id = comp["competitionId"]
        self._opta_competition_id = comp["optaCompetitionId"]

        fixtures = self._get_fixtures(self._competition_id, self.season_id)
        self._fixtures_list = fixtures
        self.fixtures_json_text = json.dumps(fixtures, ensure_ascii=False)  # handy if you need text

        # Build DF (normalized opta_match_id)
        df = (
            pl.DataFrame(fixtures)
            .with_columns(
                pl.col("optaMatchId").cast(pl.Utf8).str.replace_all("g", "").alias("opta_match_id")
            )
        )
        self._fixtures_df = transform_fixtures(df)

        self._fixtures_df = self._fixtures_df.to_pandas()

        # Cache assets for all fixtures
        self._fixture_assets = self._collect_fixture_assets(fixtures)

        fmt = format.lower()
        if fmt == "dataframe":
            return self._fixtures_df
        if fmt == "json":
            return self._fixtures_list
        raise ValueError("format must be 'dataframe' or 'json'")

    def _ensure_fixtures_loaded(self) -> None:
        if self._fixtures_df is None or self._fixture_assets is None or self._opta_competition_id is None:
            raise RuntimeError("Call fixtures() first to load and cache fixtures/assets.")

    def splits(self, *, opta_match_id: str, type: str = "players", model_form: str = "denormalized") -> pl.DataFrame:
        """
        Get physical splits for a match.
        type='players' | 'teams'
        """
        self._ensure_fixtures_loaded()

        metadata_raw = self._download_metadata(opta_match_id)
        splits_csv = self._download_physical(opta_match_id, self.SUBTYPE_SPLITS)

        metadata_df = pl.from_dicts([metadata_raw])
        players_df, players_df_normalized, teams_df, teams_df_normalized = physical_splits(
            self.season_id,
            self._opta_competition_id,  # type: ignore[arg-type]
            metadata_df,
            splits_csv,
            opta_match_id,
            self._fixtures_df
        )

        if type.lower() == "players" and model_form.lower() == "denormalized":
            return players_df
        elif type.lower() == "players" and model_form.lower() == "normalized":
            return players_df_normalized
        if type.lower() == "teams" and model_form.lower() == "denormalized":
            return teams_df
        elif type.lower() == "teams" and model_form.lower() == "normalized":
            return teams_df_normalized
        raise ValueError("type must be 'players' or 'teams'")
    


    def summary(self, *, opta_match_id: str) -> pl.DataFrame:
        """Get physical summary for a match."""
        self._ensure_fixtures_loaded()

        metadata_raw = self._download_metadata(opta_match_id)
        summary_csv = self._download_physical(opta_match_id, self.SUBTYPE_SUMMARY)

        metadata_df = pl.from_dicts([metadata_raw])
        return physical_summary(
            self._fixtures_df,          # type: ignore[arg-type]
            metadata_df,
            summary_csv,
            opta_match_id,
        )

    def events(self, *, opta_match_id: str | int, format: str = "dataframe"):
        """
        Get match events for a match and enrich with event type names.

        Args:
            opta_match_id: Match id (with or without 'g' prefix)
            format: "dataframe" (default) or "json"
        """
        self._ensure_fixtures_loaded()

        events_xml = self._download_physical(str(opta_match_id), self.SUBTYPE_EVENTS)
        match_events = self._parse_events_xml(events_xml)
        events_df = self._join_events_with_type_labels(match_events)

        fmt = format.lower()
        if fmt == "dataframe":
            return events_df
        if fmt == "json":
            return events_df.to_dict(orient="records")
        raise ValueError("format must be 'dataframe' or 'json'")

    # ============================
    # Internals
    # ============================

    def _get_api_key(self, username: str, password: str) -> str:
        r = requests.post(
            self.AUTH_URL,
            headers={"Content-Type": "application/json"},
            json={"username": username, "password": password},
            timeout=self._timeout,
        )
        r.raise_for_status()
        token = r.json().get("token")
        if not token:
            raise RuntimeError("Authentication succeeded but token missing.")
        return token

    def _resolve_competition(self, competition_name: str) -> dict:
        r = self._get(f"{self.BASE_URL}/dvms/competitions")
        comps: list[dict] = r.json()
        selected = [c for c in comps if c["name"] == competition_name]
        if not selected:
            raise ValueError(f"Competition not found: {competition_name}")
        comp = selected[0]
        comp["optaCompetitionId"] = self.COMP_MAP.get(comp["name"])
        if not comp["optaCompetitionId"]:
            raise ValueError(f"No optaCompetitionId mapping for competition: {comp['name']}")
        return comp

    def _get_fixtures(self, competition_id: str, season_id: int) -> list[dict]:
        fixtures: list[dict] = []
        for page in range(0, 6):
            payload = {"pageNumber": page, "limit": 100}
            r = self._post(f"{self.BASE_URL}/dvms/{competition_id}/fixtures/{season_id}", payload)
            data = r.json()
            fixtures.extend(data.get("fixtures", []))
        return fixtures

    def _collect_fixture_assets(self, fixtures: list[dict]) -> list[dict]:
        out: list[dict] = []
        for fx in fixtures:
            fx_match_id_clean = (fx.get("optaMatchId") or "").replace("g", "")
            for asset in fx.get("assets", []):
                sub_type = asset.get("subType")
                if sub_type in (
                    self.SUBTYPE_EVENTS,
                    self.SUBTYPE_TRACKING,
                    self.SUBTYPE_METADATA,
                    self.SUBTYPE_SPLITS,
                    self.SUBTYPE_SUMMARY,
                ):
                    out.append(
                        {
                            "fixture_id": fx["fixtureId"],
                            "opta_match_id": fx_match_id_clean,
                            "competition_id": fx["competition"],
                            "opta_competition_id": fx["optaCompetition"],
                            "opta_season_id": fx["optaSeason"],
                            "asset_id": asset["assetId"],
                            "sub_type": sub_type,
                            "key": asset.get("key"),
                            "ready": asset.get("ready"),
                        }
                    )
        return out

    def _find_asset(self, *, opta_match_id: str, sub_type: int) -> dict:
        if not self._fixture_assets:
            raise RuntimeError("No cached assets. Call fixtures(...) first.")
        match_assets = [a for a in self._fixture_assets if a["opta_match_id"] == str(opta_match_id)]
        if not match_assets:
            raise ValueError(f"No cached assets for match {opta_match_id}.")

        typed_assets = [a for a in match_assets if a["sub_type"] == sub_type]
        if typed_assets:
            ready_assets = [a for a in typed_assets if a.get("ready") is True]
            return ready_assets[0] if ready_assets else typed_assets[0]

        available_subtypes = sorted({a["sub_type"] for a in match_assets})
        raise ValueError(
            f"No asset for match {opta_match_id} with sub_type {sub_type}. "
            f"Available sub_types: {available_subtypes}"
        )

    # -------- Downloads --------
    def _download_metadata(self, opta_match_id: str) -> dict:
        a = self._find_asset(opta_match_id=opta_match_id, sub_type=self.SUBTYPE_METADATA)
        return self._download_asset_json(a["opta_competition_id"], a["fixture_id"], a["asset_id"])

    def _download_physical(self, opta_match_id: str, sub_type: int) -> str:
        a = self._find_asset(opta_match_id=opta_match_id, sub_type=sub_type)
        return self._download_asset_text(a["opta_competition_id"], a["fixture_id"], a["asset_id"])

    # -------- HTTP helpers with simple retry --------
    def _post(self, url: str, json_payload: dict | None = None) -> requests.Response:
        last_exc = None
        for _ in range(self._retries):
            try:
                r = requests.post(url, headers=self.headers, json=json_payload, timeout=self._timeout)
                r.raise_for_status()
                return r
            except requests.RequestException as e:
                last_exc = e
                time.sleep(self._sleep)
        raise RuntimeError(f"POST failed: {url}") from last_exc

    def _get(self, url: str, *, stream: bool = False) -> requests.Response:
        last_exc = None
        for _ in range(self._retries):
            try:
                r = requests.get(url, headers=self.headers, timeout=self._timeout, stream=stream)
                r.raise_for_status()
                return r
            except requests.RequestException as e:
                last_exc = e
                time.sleep(self._sleep)
        raise RuntimeError(f"GET failed: {url}") from last_exc

    def _download_asset_text(self, competition_id: str, fixture_id: str, asset_id: str) -> str:
        url = f"{self.BASE_URL}/dvms/{competition_id}/fixtures/{fixture_id}/download/{asset_id}"
        return self._get(url).text

    def _download_asset_json(self, competition_id: str, fixture_id: str, asset_id: str) -> dict:
        url = f"{self.BASE_URL}/dvms/{competition_id}/fixtures/{fixture_id}/download/{asset_id}"
        return self._get(url).json()

    def _parse_events_xml(self, xml_text: str) -> list[dict]:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            raise RuntimeError("Failed to parse events XML payload.") from e

        match_events: list[dict] = []
        for game in root.findall(".//Game"):
            for event in game.findall("Event"):
                match_events.append(
                    {
                        "opta_matchid": game.get("id"),
                        "id": event.get("id"),
                        "event_id": event.get("event_id"),
                        "type_id": event.get("type_id"),
                        "period_id": event.get("period_id"),
                        "min": event.get("min"),
                        "sec": event.get("sec"),
                        "player_id": event.get("player_id"),
                        "team_id": event.get("team_id"),
                        "outcome": event.get("outcome"),
                        "x": event.get("x"),
                        "y": event.get("y"),
                        "timestamp": event.get("timestamp"),
                        "timestamp_utc": event.get("timestamp_utc"),
                        "last_modified": event.get("last_modified"),
                    }
                )
        return match_events

    def _join_events_with_type_labels(self, match_events: list[dict]):
        if not match_events:
            return pl.DataFrame(
                schema={
                    "opta_matchid": pl.Utf8,
                    "id": pl.Utf8,
                    "event_id": pl.Utf8,
                    "type_id": pl.Utf8,
                    "period_id": pl.Utf8,
                    "min": pl.Utf8,
                    "sec": pl.Utf8,
                    "player_id": pl.Utf8,
                    "team_id": pl.Utf8,
                    "outcome": pl.Utf8,
                    "x": pl.Utf8,
                    "y": pl.Utf8,
                    "timestamp": pl.Utf8,
                    "timestamp_utc": pl.Utf8,
                    "last_modified": pl.Utf8,
                    "event_type": pl.Utf8,
                }
            ).to_pandas()

        events_df = pl.DataFrame(match_events)
        event_types_df = pl.DataFrame(
            {
                "type_id": list(self.EVENT_TYPES.keys()),
                "event_type": list(self.EVENT_TYPES.values()),
            }
        )

        con = duckdb.connect()
        try:
            con.register("events_raw", events_df.to_arrow())
            con.register("event_types", event_types_df.to_arrow())
            return con.execute(
                """
                SELECT
                    e.*,
                    t.event_type
                FROM events_raw e
                LEFT JOIN event_types t
                    ON TRY_CAST(e.type_id AS INTEGER) = t.type_id
                ORDER BY
                    TRY_CAST(e.min AS INTEGER) NULLS LAST,
                    TRY_CAST(e.sec AS INTEGER) NULLS LAST,
                    TRY_CAST(e.id AS BIGINT) NULLS LAST
                """
            ).fetchdf()
        finally:
            con.close()

    # (Optional) Events / Tracking helpers you can add later if needed:
    # def tracking(self, *, opta_match_id: str) -> pl.DataFrame: ...
    # def events(self, *, opta_match_id: str) -> list[dict]: ...

