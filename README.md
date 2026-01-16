# tidy_dvms

**Unofficial** Python client for working with Premier League **DVMS / Second Spectrum** tracking data (fixtures, physical splits, and summary), with a clean API and **Polars**-first processing.  
_Not affiliated with, endorsed by, or sponsored by the Premier League, Second Spectrum, or Hudl._

> ⚠️ You must have valid DVMS credentials issued by your organisation. Do **not** commit credentials to source control.

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

- Minimal, explicit API:
  ```python
  client.fixtures(format="dataframe" | "json")
  client.splits(opta_match_id=..., type="players" | "teams")
  client.summary(opta_match_id=...)
  ```
- Choose **Pandas DataFrame** or **raw JSON (list[dict])** for fixtures:
  - DataFrame for quick exploration / saving to CSV.
  - JSON for flexible iteration (e.g., loop over `opta_match_id`).
- Only returns assets for **matches with available data** (played/processed).
- Built on **Polars**, compatible with **pandas**, **PyArrow**, and **DuckDB**.
- Simple, configurable retries for HTTP calls.

---

## Installation

```bash

# navigate to venv directory
cd C:\Users\ademmadoun\Downloads\dvms

# create venv
py -3.13 -m venv .venv

# grant venv activation permission
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

# activate venv
.\.venv\Scripts\Activate.ps1

# upgrade pip
python -m pip install --upgrade pip

# install package:
python -m pip install "git+https://github.com/AdemMad/tidy_dvms.git@main"
```

---

## Quickstart

```python
from tidy_dvms import DVMS

client = DVMS(
    season=2025,                                # e.g., 2020–2025
    competition_name="English Premier League",  # "English Premier League", "EFL Championship", "EFL Cup", "FA Cup"
    username="YOUR_EMAIL",
    password="YOUR_PASSWORD",
)

# 1) Fixtures (choose representation)
fixtures_df   = client.fixtures(format="dataframe")  # Pandas DataFrame
fixtures_json = client.fixtures(format="json")       # list[dict]

# 2) Choose a match and compute outputs
opta_match_id = 2561923      # integer value
players = client.splits(opta_match_id=opta_match_id, type="players")
teams   = client.splits(opta_match_id=opta_match_id, type="teams")
summary = client.summary(opta_match_id=opta_match_id)
```

---

## API

### DVMS

```python
DVMS(
    season: int,
    competition_name: str,
    username: str,
    password: str,
    *,
    request_timeout: int = 30,
    request_retries: int = 3,
    sleep_between_retries: float = 1.0,
)
```

Creates an authenticated client and sets defaults used by all calls.

- `season` — e.g., `2025`
- `competition_name` — `"English Premier League"`, `"EFL Championship"`, `"EFL Cup"`, `"FA Cup"`
- `username`, `password` — your DVMS credentials
- Retry/timeout knobs for transient HTTP issues

---

### fixtures

```python
fixtures(format: str = "dataframe") -> pl.DataFrame | list[dict]
```

Fetches fixtures for the configured season/competition and **caches assets** for later calls.

- `format="dataframe"` (default): returns a **Pandas DataFrame** with normalized `opta_match_id` (no leading `'g'`)
- `format="json"`: returns the raw **list[dict]** response

Side effects:
- Caches competition IDs and fixture assets (enables `splits()`/`summary()`).

---

### splits

```python
splits(*, opta_match_id: str, type: str = "players") -> pl.DataFrame
```

Returns physical **splits** for the specified match.

- `type="players"` → per-player splits DataFrame  
- `type="teams"` → per-team splits DataFrame

> Requires `fixtures()` to have been called (to cache assets).

---

### summary

```python
summary(*, opta_match_id: str) -> pl.DataFrame
```

Returns physical **summary** for the specified match.

> Requires `fixtures()` first.

---

## Examples

### Loop over all fixtures

```python
from tidy_dvms import DVMS

client = DVMS(
    season=2025,
    competition_name="English Premier League",
    username="YOUR_EMAIL",
    password="YOUR_PASSWORD",
)

fx = client.fixtures(format="dataframe")
for mid in fx.get_column("opta_match_id").unique():
    players = client.splits(opta_match_id=mid, type="players")
    teams   = client.splits(opta_match_id=mid, type="teams")
    summary = client.summary(opta_match_id=mid)

    # Save (Pandas → CSV)
    players.write_csv(f"players_{mid}.csv")
    teams.write_csv(f"teams_{mid}.csv")
    summary.write_csv(f"summary_{mid}.csv")
```

### Work from JSON

```python
fixtures = client.fixtures(format="json")  # list[dict]

for fx in fixtures:
    mid = (fx.get("optaMatchId") or "").replace("g", "")
    if not mid:
        continue
    teams   = client.splits(opta_match_id=mid, type="teams")
    players = client.splits(opta_match_id=mid, type="players")
    summary = client.summary(opta_match_id=mid)
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

# pandas → to_sql
players.to_sql("PlayersSplits", engine, if_exists="append", index=False)
```

---

## Configuration & Secrets

- **Never** hardcode credentials. Use environment variables or a secret manager.
- Example (PowerShell):
  ```powershell
  $env:DVMS_USERNAME="you@club.com"
  $env:DVMS_PASSWORD="********"
  ```
- Then:
  ```python
  import os
  client = DVMS(
      season=2025,
      competition_name="English Premier League",
      username=os.environ["DVMS_USERNAME"],
      password=os.environ["DVMS_PASSWORD"],
  )
  ```

---

## Troubleshooting

- **`RuntimeError: Call fixtures() first`**  
  Load fixtures at least once to cache IDs and assets.

- **`ValueError: Competition not found`**  
  Check `competition_name` spelling. Supported: English Premier League, EFL Championship, EFL Cup, FA Cup.

- **401/403 token issues**  
  Verify credentials and account access.

- **429 / transient HTTP failures**  
  Increase `request_retries` and `sleep_between_retries`. Avoid tight loops.

- **Windows / ODBC**  
  Install an appropriate ODBC driver (e.g., *ODBC Driver 18 for SQL Server*) if you use `pyodbc`.

---

## Development

```bash
# clone
git clone https://github.com/<your-org>/tidy_dvms
cd tidy_dvms

# venv (Windows example)
py -3.9 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install -e .[dev]

# tests
python -m pytest -q
```

---

## Versioning

Follows **Semantic Versioning**:
- **MAJOR**: breaking changes
- **MINOR**: features (backwards-compatible)
- **PATCH**: fixes (backwards-compatible)

---

## License

**MIT** — see [LICENSE](./LICENSE).

---

## Author

**Adem Madoun**  
© 2026 — tidy_dvms 
Issues & feature requests: open a ticket on the repository Issues page.

---

## Disclaimer

This is an **unofficial** client for working with DVMS / Second Spectrum tracking data.  
It is **not** affiliated with, endorsed by, or sponsored by the Premier League, Second Spectrum, or Hudl.  
Use of this software requires valid access and credentials; respect all applicable terms, licenses, and policies.
#




