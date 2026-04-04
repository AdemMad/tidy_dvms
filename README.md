# tidy_dvms

Unofficial Python client for working with Premier League DVMS / Second Spectrum data
(fixtures, physical splits, summary, events, and lineups).
Not affiliated with, endorsed by, or sponsored by the Premier League, Second Spectrum, or Hudl.

> ⚠️ You must have valid DVMS credentials issued by your organisation. Do not commit credentials to source control.

---

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [API](#api)
  - [DVMS](#dvms)
  - [fixtures](#fixtures)
  - [splits](#splits)
  - [summary](#summary)
  - [lineups](#lineups)
  - [events](#events)
- [Examples](#examples)
  - [Loop over all fixtures](#loop-over-all-fixtures)
  - [Work from JSON](#work-from-json)
  - [Persist to SQL Server (optional)](#persist-to-sql-server-optional)
- [Configuration & Secrets](#configuration--secrets)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [Versioning](#versioning)
- [License](#license)
- [Author](#author)
- [Disclaimer](#disclaimer)

---

## Features

- Minimal API with method-level context:
  ```python
  client.fixtures(competition=..., season=..., creds=..., format="dataframe" | "json")
  client.splits(opta_match_id=..., competition=..., season=..., creds=..., type="players" | "teams")
  client.summary(opta_match_id=..., competition=..., season=..., creds=...)
  client.lineups(opta_match_id=..., competition=..., season=..., creds=...)
  client.events(opta_match_id=..., competition=..., season=..., creds=...)
  ```
- `splits()`, `summary()`, `lineups()`, and `events()` automatically load fixtures for the active context when needed.
- Choose DataFrame or raw JSON for fixtures.
- Built on Polars / DuckDB internally and returns analysis-friendly DataFrames.
- Simple, configurable retries for HTTP calls.

---

## Installation

```bash
py -m pip install "git+https://github.com/AdemMad/tidy_dvms.git@main"
```

### Windows (optional: venv example)

```bash
cd C:\Users\your-user\Downloads\dvms
py -3.13 -m venv .venv
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
.\.venv\Scripts\Activate.ps1
py -m pip install --upgrade pip
py -m pip install "git+https://github.com/AdemMad/tidy_dvms.git@main"
```

---

## Quickstart

```python
import os
from tidy_dvms import DVMS

client = DVMS()
creds = {
    "username": os.environ["DVMS_USERNAME"],
    "password": os.environ["DVMS_PASSWORD"],
}

fixtures_df = client.fixtures(
    competition="English Premier League",
    season=2025,
    creds=creds,
    format="dataframe",
)

opta_match_id = 2561923
players = client.splits(
    opta_match_id=opta_match_id,
    competition="English Premier League",
    season=2025,
    creds=creds,
    type="players",
)
teams = client.splits(
    opta_match_id=opta_match_id,
    competition="English Premier League",
    season=2025,
    creds=creds,
    type="teams",
)
summary = client.summary(
    opta_match_id=opta_match_id,
    competition="English Premier League",
    season=2025,
    creds=creds,
)
lineups = client.lineups(
    opta_match_id=opta_match_id,
    competition="English Premier League",
    season=2025,
    creds=creds,
)
events = client.events(
    opta_match_id=opta_match_id,
    competition="English Premier League",
    season=2025,
    creds=creds,
)
```

---

## API

### DVMS

```python
DVMS(
    season: int | None = None,
    competition_name: str | None = None,
    username: str | None = None,
    password: str | None = None,
    *,
    request_timeout: int = 30,
    request_retries: int = 3,
    sleep_between_retries: float = 1.0,
)
```

Creates a client instance.

Recommended style:
- Initialize with `DVMS()` and pass `competition`, `season`, and `creds` into each public method.

Also supported:
- You can still provide `season`, `competition_name`, `username`, and `password` at initialization time if you want defaults.

---

### fixtures

```python
fixtures(
    *,
    competition: str | None = None,
    season: int | None = None,
    creds: dict[str, str] | None = None,
    format: str = "dataframe",
) -> DataFrame | list[dict]
```

Fetches fixtures for the provided competition/season and caches fixture assets for later match-level calls.

- `competition`: for example `"English Premier League"`, `"EFL Championship"`, `"EFL Cup"`, `"FA Cup"`
- `season`: for example `2025`
- `creds`: `{"username": "...", "password": "..."}`
- `format="dataframe"`: returns a DataFrame with normalized `opta_match_id`
- `format="json"`: returns the raw `list[dict]` payload

If an argument is omitted, the client falls back to constructor defaults or the most recently used context.

---

### splits

```python
splits(
    *,
    opta_match_id: str,
    competition: str | None = None,
    season: int | None = None,
    creds: dict[str, str] | None = None,
    type: str = "players",
    model_form: str = "denormalized",
) -> DataFrame
```

Returns physical splits for a match.

- `type="players"` returns per-player splits
- `type="teams"` returns per-team splits
- `model_form="denormalized"` or `"normalized"`

The client automatically loads fixtures for the active context if needed.

---

### summary

```python
summary(
    *,
    opta_match_id: str,
    competition: str | None = None,
    season: int | None = None,
    creds: dict[str, str] | None = None,
) -> DataFrame
```

Returns physical summary for a match.

The client automatically loads fixtures for the active context if needed.

---

### lineups

```python
lineups(
    *,
    opta_match_id: str,
    competition: str | None = None,
    season: int | None = None,
    creds: dict[str, str] | None = None,
    format: str = "dataframe",
) -> DataFrame | list[dict]
```

Returns match lineups for a match.

- `format="dataframe"` returns a DataFrame
- `format="json"`: `list[dict]`

The client automatically loads fixtures for the active context if needed.

---

### events

```python
events(
    *,
    opta_match_id: str,
    competition: str | None = None,
    season: int | None = None,
    creds: dict[str, str] | None = None,
    format: str = "dataframe",
) -> DataFrame | list[dict]
```

Returns match events for a match. Event names are joined by `type_id`, and player names are backfilled from lineup or metadata payloads when available.

- `format="dataframe"` returns a DataFrame
- `format="json"`: `list[dict]`

The client automatically loads fixtures for the active context if needed.

---

## Examples

### Loop over all fixtures

```python
import os
from tidy_dvms import DVMS

client = DVMS()
competition = "English Premier League"
season = 2025
creds = {
    "username": os.environ["DVMS_USERNAME"],
    "password": os.environ["DVMS_PASSWORD"],
}

fx = client.fixtures(
    competition=competition,
    season=season,
    creds=creds,
    format="dataframe",
)

for mid in fx["opta_match_id"].dropna().unique():
    players = client.splits(
        opta_match_id=mid,
        competition=competition,
        season=season,
        creds=creds,
        type="players",
    )
    teams = client.splits(
        opta_match_id=mid,
        competition=competition,
        season=season,
        creds=creds,
        type="teams",
    )
    summary = client.summary(
        opta_match_id=mid,
        competition=competition,
        season=season,
        creds=creds,
    )
    lineups = client.lineups(
        opta_match_id=mid,
        competition=competition,
        season=season,
        creds=creds,
    )
    events = client.events(
        opta_match_id=mid,
        competition=competition,
        season=season,
        creds=creds,
    )

    players.write_csv(f"players_{mid}.csv")
    teams.write_csv(f"teams_{mid}.csv")
    summary.write_csv(f"summary_{mid}.csv")
    lineups.to_csv(f"lineups_{mid}.csv", index=False)
    events.to_csv(f"events_{mid}.csv", index=False)
```

### Work from JSON

```python
fixtures = client.fixtures(
    competition=competition,
    season=season,
    creds=creds,
    format="json",
)

for fx in fixtures:
    mid = (fx.get("optaMatchId") or "").replace("g", "")
    if not mid:
        continue
    teams = client.splits(
        opta_match_id=mid,
        competition=competition,
        season=season,
        creds=creds,
        type="teams",
    )
    players = client.splits(
        opta_match_id=mid,
        competition=competition,
        season=season,
        creds=creds,
        type="players",
    )
    summary = client.summary(
        opta_match_id=mid,
        competition=competition,
        season=season,
        creds=creds,
    )
    lineups = client.lineups(
        opta_match_id=mid,
        competition=competition,
        season=season,
        creds=creds,
    )
    events = client.events(
        opta_match_id=mid,
        competition=competition,
        season=season,
        creds=creds,
    )
```

### Persist to SQL Server (optional)

```python
import sqlalchemy as sa

engine = sa.create_engine(
    "mssql+pyodbc:///?odbc_connect="
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=your-server;"
    "DATABASE=your-db;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

players.to_sql("PlayersSplits", engine, if_exists="append", index=False)
```

---

## Configuration & Secrets

- Never hardcode credentials. Use environment variables or a secret manager.
- Example (PowerShell):
  ```powershell
  $env:DVMS_USERNAME="you@club.com"
  $env:DVMS_PASSWORD="********"
  ```
- Then:
  ```python
  import os
  from tidy_dvms import DVMS

  client = DVMS()
  creds = {
      "username": os.environ["DVMS_USERNAME"],
      "password": os.environ["DVMS_PASSWORD"],
  }

  fixtures = client.fixtures(
      competition="English Premier League",
      season=2025,
      creds=creds,
      format="dataframe",
  )
  ```

---

## Troubleshooting

- `ValueError: Provide creds...`
  Pass `creds={"username": "...", "password": "..."}` or initialize `DVMS` with username/password defaults.

- `ValueError: Provide a competition value...` or `ValueError: Provide a season value...`
  Pass `competition=` and `season=` to the method, or initialize `DVMS` with defaults.

- `ValueError: Competition not found`
  Check the competition spelling and confirm that your account has access.

- `401/403 auth issues`
  Verify credentials and account permissions.

- `429 / transient HTTP failures`
  Increase `request_retries` and `sleep_between_retries`. Avoid tight loops.

- `Windows / ODBC`
  Install an appropriate ODBC driver (for example, ODBC Driver 18 for SQL Server) if you use `pyodbc`.

---

## Development

```bash
git clone https://github.com/AdemMad/tidy_dvms
cd tidy_dvms

py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
py -m pip install -U pip
py -m pip install -e .[dev]

py -m pytest -q
```

---

## Versioning

Follows **Semantic Versioning**:
- **MAJOR**: breaking changes
- **MINOR**: features (backwards-compatible)
- **PATCH**: fixes (backwards-compatible)

---

## License

MIT - see [LICENSE](./LICENSE).

---

## Author

Adem Madoun
(c) 2026 - tidy_dvms
Issues and feature requests: please open an issue in the repository.

---

## Disclaimer

This is an **unofficial** client for working with DVMS / Second Spectrum tracking data.
It is **not** affiliated with, endorsed by, or sponsored by the Premier League, Second Spectrum, or Hudl.
Use of this software requires valid access and credentials; respect all applicable terms, licenses, and policies.

