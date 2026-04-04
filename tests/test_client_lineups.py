from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tidy_dvms.client import DVMS


LINEUPS_XML = """
<SoccerDocument>
  <Team uID="t100">
    <Name>Home FC</Name>
    <Player uID="p11" Position="FW" ShirtNumber="9" Status="Start">
      <PersonName>
        <First>Alex</First>
        <Last>Jones</Last>
      </PersonName>
    </Player>
    <Player uID="p12" Position="MF" ShirtNumber="10" Status="Bench">
      <PersonName>
        <First>Sam</First>
        <Last>Lee</Last>
      </PersonName>
    </Player>
  </Team>
  <Team uID="t200">
    <Name>Away FC</Name>
    <Player uID="p21" Position="GK" ShirtNumber="1" Status="Start">
      <PersonName>
        <First>Pat</First>
        <Last>Kim</Last>
      </PersonName>
    </Player>
  </Team>
</SoccerDocument>
""".strip()


def make_client() -> DVMS:
    client = DVMS()
    client._fixtures_list = [
        {
            "optaMatchId": "g12345",
            "homeTeamName": "Home FC",
            "awayTeamName": "Away FC",
            "date": "2026-03-29T15:00:00Z",
        }
    ]
    client._fixture_assets = [
        {
            "fixture_id": "fixture-1",
            "opta_match_id": "12345",
            "competition_id": "comp-1",
            "opta_competition_id": "8",
            "opta_season_id": "2025",
            "asset_id": "asset-1",
            "sub_type": DVMS.SUBTYPE_LINEUPS,
            "ready": True,
        }
    ]
    return client


def test_fixtures_accepts_method_level_context_without_initializer_state():
    client = DVMS()
    load_calls = []

    def fake_auth(creds):
        client._default_creds = dict(creds)
        client._auth_context = (creds["username"], creds["password"])

    def fake_load(competition, season):
        load_calls.append((competition, season))
        client._fixtures_df = "fixtures-df"
        client._fixtures_list = [{"fixtureId": "fixture-1"}]

    client._authenticate = fake_auth
    client._load_fixtures_context = fake_load

    fixtures = client.fixtures(
        competition="English Premier League",
        season=2025,
        creds={"username": "user@example.com", "password": "secret"},
        format="json",
    )

    assert fixtures == [{"fixtureId": "fixture-1"}]
    assert load_calls == [("English Premier League", 2025)]
    assert client._fixtures_context == (
        "English Premier League",
        2025,
        "user@example.com",
        "secret",
    )


def test_find_asset_accepts_g_prefixed_match_id():
    client = make_client()

    asset = client._find_asset(opta_match_id="g12345", sub_type=DVMS.SUBTYPE_LINEUPS)

    assert asset["asset_id"] == "asset-1"


def test_lineups_returns_match_lineups_with_fixture_context():
    client = make_client()
    captured = {}
    client._ensure_fixtures_loaded = lambda **kwargs: captured.update(kwargs)
    client._download_physical = lambda opta_match_id, sub_type: LINEUPS_XML

    records = client.lineups(
        opta_match_id="g12345",
        competition="English Premier League",
        season=2025,
        creds={"username": "user@example.com", "password": "secret"},
        format="json",
    )

    assert len(records) == 3
    assert {record["player_id"] for record in records} == {"11", "12", "21"}
    assert all(record["opta_match_id"] == "12345" for record in records)
    assert all(record["fixture"] == "Home FC - Away FC" for record in records)
    assert all(record["game_date"] == "2026-03-29" for record in records)
    assert captured == {
        "competition": "English Premier League",
        "season": 2025,
        "creds": {"username": "user@example.com", "password": "secret"},
    }

    alex = next(record for record in records if record["player_id"] == "11")
    assert alex["player_name"] == "Alex Jones"
    assert alex["position"] == "FW"
    assert alex["team_name"] == "Home FC"
    assert alex["shirt_number"] == "9"
    assert alex["status"] == "Start"


def test_ensure_fixtures_loaded_reuses_cached_context():
    client = DVMS()
    load_calls = []

    def fake_auth(creds):
        client._default_creds = dict(creds)
        client._auth_context = (creds["username"], creds["password"])

    def fake_load(competition, season):
        load_calls.append((competition, season))
        client._fixtures_df = "fixtures-df"
        client._fixture_assets = []
        client._opta_competition_id = "8"

    client._authenticate = fake_auth
    client._load_fixtures_context = fake_load

    request = {
        "competition": "English Premier League",
        "season": 2025,
        "creds": {"username": "user@example.com", "password": "secret"},
    }

    client._ensure_fixtures_loaded(**request)
    client._ensure_fixtures_loaded(**request)
    client._ensure_fixtures_loaded(
        competition="FA Cup",
        season=2025,
        creds={"username": "user@example.com", "password": "secret"},
    )

    assert load_calls == [
        ("English Premier League", 2025),
        ("FA Cup", 2025),
    ]
