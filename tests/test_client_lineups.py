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
    client = DVMS.__new__(DVMS)
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


def test_find_asset_accepts_g_prefixed_match_id():
    client = make_client()

    asset = client._find_asset(opta_match_id="g12345", sub_type=DVMS.SUBTYPE_LINEUPS)

    assert asset["asset_id"] == "asset-1"


def test_lineups_returns_match_lineups_with_fixture_context():
    client = make_client()
    client._ensure_fixtures_loaded = lambda: None
    client._download_physical = lambda opta_match_id, sub_type: LINEUPS_XML

    records = client.lineups(opta_match_id="g12345", format="json")

    assert len(records) == 3
    assert {record["player_id"] for record in records} == {"11", "12", "21"}
    assert all(record["opta_match_id"] == "12345" for record in records)
    assert all(record["fixture"] == "Home FC - Away FC" for record in records)
    assert all(record["game_date"] == "2026-03-29" for record in records)

    alex = next(record for record in records if record["player_id"] == "11")
    assert alex["player_name"] == "Alex Jones"
    assert alex["position"] == "FW"
    assert alex["team_name"] == "Home FC"
    assert alex["shirt_number"] == "9"
    assert alex["status"] == "Start"
