import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from tidy_dvms import DVMS


def main() -> None:
    username = os.getenv("DVMS_USERNAME")
    password = os.getenv("DVMS_PASSWORD")
    if not username or not password:
        raise SystemExit(
            "Set DVMS_USERNAME and DVMS_PASSWORD first. "
            "PowerShell: $env:DVMS_USERNAME='you@club.com'; $env:DVMS_PASSWORD='***'"
        )

    client = DVMS(
        season=2025,
        competition_name="EFL Championship",
        username=username,
        password=password,
    )

    # Required to cache fixture assets before calling events().
    client.fixtures(format="json")

    opta_match_id = int(os.getenv("DVMS_MATCH_ID", "2566773"))
    events = client.events(opta_match_id=opta_match_id, format="dataframe")
    print(events.head())


if __name__ == "__main__":
    main()
