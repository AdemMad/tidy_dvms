import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from tidy_dvms import DVMS


client = DVMS(
    season=2025,                                # e.g., 2020–2025
    competition_name="English Premier League",  # "English Premier League", "EFL Championship", "EFL Cup", "FA Cup"
    username="rwilton@saintsfc.co.uk",
    password="Saints2023!",
)

# 1) Fixtures (choose representation)
fixtures_df   = client.fixtures(format="dataframe")  # Pandas DataFrame
fixtures_json = client.fixtures(format="json")       # list[dict]

# 2) Choose a match and compute outputs
opta_match_id = 2561923
players = client.splits(opta_match_id=opta_match_id, type="players", model_form="normalized")
teams   = client.splits(opta_match_id=opta_match_id, type="teams")
summary = client.summary(opta_match_id=opta_match_id)
