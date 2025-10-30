# TDT4225 — Movies (MongoDB)

This repository provides utilities for **data cleaning (CSV → clean CSV)** and **loading into MongoDB**, as well as EDA notebooks.

> Dataset: *The Movies Dataset* (Kaggle). The pipeline is: `raw CSV → filter_movies.py → data_clean/*.csv → insertion.py → MongoDB`.

---

## Project Structure

```
.
├── DbConnector.py            # MongoDB connection (client + db wrapper)
├── filter_movies.py          # Data cleaning / normalization (chunk-safe)
├── insertion.py              # Robust insertion + indexing for MongoDB
├── requirements.txt          # Python dependencies
├── Dockerfile               
├── eda_movies.ipynb          # EDA (not included here)
└── task2.ipynb               # Query tasks / results (not included here)
```

---

## Requirements

* Python 3.10+
* MongoDB 7.x (local, Docker, or course VM)
* Install Python packages:

```bash
pip install -r requirements.txt
```

> Includes `pymongo`, `pandas`, `numpy`, `matplotlib`, `seaborn`, `tqdm`, `python-dateutil`, etc.

---

## MongoDB Connection Setup

Edit `DbConnector.py` to match your host, user, and database. Example:

```python
DbConnector(
  DATABASE="movies_db",
  HOST="mongo",          # or tdt4225-xx.idi.ntnu.no
  USER="<user>",
  PASSWORD="<password>"
)
```

* The connector builds a URI `mongodb://USER:PASSWORD@HOST/DATABASE` and exposes `self.client` and `self.db`. Upon closing, it calls `client.close()` and prints status messages.
* **Security tip**: avoid hardcoding credentials. Use environment variables (`os.getenv`) or a `.env` file.

---

## Data Cleaning

Script: `filter_movies.py`

**Expected input files** (`--in-dir`):

* `movies_metadata.csv`, `credits.csv`, `links.csv`, `ratings.csv`, and optional `keywords.csv`.

**Output** (`--out-dir`):

* `*_clean.csv` versions and optionally `keywords_exploded.csv`.

### Functionality Overview

* **Movies**:

  * Enforces numeric IDs, valid titles, deduplication, numeric casting, filters on votes/date/year, serializes JSON arrays, and fixes extreme runtimes (>873 min). Also extracts `year`.
* **Credits**: dedup by `id`, keep row with max (`len(cast)+len(crew)`), re-serializes JSON arrays.
* **Links**: coerces numeric types, handles null `tmdbId`, dedups by `movieId`.
* **Ratings**: chunked read, `userId/movieId` → int, `rating∈[0,10]`, timestamp → datetime, configurable dedup (`first|last|all`), inter-chunk dedup via in-memory index.
* **Keywords**: normalized (lowercased) and exploded into `movie-keyword` rows.

### Usage Example

```bash
python filter_movies.py \
  --in-dir data \
  --out-dir data_clean \
  --min-votes 0 \
  --year-min 1888 --year-max 2100 \
  --keep-null-tmdb \
  --ratings-keep last \
  --explode-keywords
```

> The script prints stats on filtered rows and kept ratios.

---

## ⬆Insertion into MongoDB

Script: `insertion.py`

### Key Features

* Batch insertions with duplicate tolerance (`BulkWriteError` handling). Tracks `ok/dup/errors` counters. Progress output every million rows.
* **Collections**:

  * `movies`: `_id=id (TMDB)`; parses `release_date` as datetime (tz-aware), normalizes JSON arrays.
  * `credits`: `_id=movie_id`, lists for `cast` and `crew`.
  * `links`: `_id=movieId (MovieLens)` with optional null `tmdbId`.
  * `ratings`: `userId`, `movieId`, `rating` [0–10], timestamp as datetime.
  * `keywords` + `keywords_exploded`: aggregated and exploded keyword forms.

### Created Indexes

* `movies`: `id_uq`, `title_idx`, `release_date_idx`, `text_idx` (on `overview` + `tagline`).
* `keywords`: `movie_id_uq`; `keywords_exploded`: `movie_idx`, `kw_idx`, `text_idx`.
* `credits`: `movie_id_uq`.
* `links`: `movieId_uq`, `tmdbId_idx`.
* `ratings`: `movie_idx`, `user_idx`, `movie_user_idx`, `timestamp_idx`.

### Usage Example

```bash
python insertion.py \
  --data-dir data_clean \
  --reset \
  --batch-movies 5000 \
  --batch-credits 5000 \
  --batch-links 10000 \
  --batch-ratings 100000 \
  --allow-null-tmdb \
  --max-runtime 873
```

> `--reset` drops existing collections. At the end, a summary of inserted docs per collection is printed.

---

## Docker

A `Dockerfile` is provided for reproducibility.

Typical usage:

```bash
# Build image
docker build -t tdt4225-movies .

# Run (assuming Mongo is reachable as "mongo" on Docker network)
docker run --rm -it \
  --name tdt4225-movies \
  --network=<your_network> \
  -v "$PWD":/app \
  tdt4225-movies bash
```

> Adjust environment variables and Mongo credentials accordingly.

---

## Notebooks

* `eda_movies.ipynb`: exploratory data analysis and visualization.
* `task2.ipynb`: MongoDB aggregation queries and analysis.

> These notebooks rely on the cleaned data and collections created by the scripts.

---

## Design Notes

* **Indexing Strategy**: optimized for join/filter keys (`movie_id`, `movieId`, `tmdbId`) and frequent filters (`title`, `release_date`, `timestamp`), plus text search for *overview*/*tagline*.
* **Keywords**: not directly used in current queries, but included (both aggregated and exploded) for potential future semantic or relational exploration.
* **Parsing Robustness**: tolerant JSON/date parsing, accepts single quotes, epoch/ISO formats, safely defaults to empty or `None`.

