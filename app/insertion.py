import csv
import json
import argparse
from datetime import datetime, timezone
from pathlib import Path

from pymongo.errors import BulkWriteError
from DbConnector import DbConnector


# ==================== Parse helpers ====================

def parse_json(value):
    """Safely parse JSON string. Returns [] on failure."""
    if value is None:
        return []
    s = str(value).strip()
    if s.lower() in {"", "null", "nan"}:
        return []
    try:
        return json.loads(s)
    except Exception:
        try:
            return json.loads(s.replace("'", '"'))
        except Exception:
            return []


def parse_float(value):
    try:
        return float(value)
    except Exception:
        return None


def parse_int(value):
    try:
        return int(float(value))
    except Exception:
        return None


def parse_date(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    # common formats
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    # ISO
    try:
        s = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def parse_timestamp(value):
    """Return timezone-aware UTC datetime or None."""
    if not value:
        return None
    s = str(value).strip()
    if s.isdigit():
        try:
            return datetime.fromtimestamp(int(s), tz=timezone.utc)
        except Exception:
            return None
    # try ISO
    try:
        s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


# ==================== IO helpers ====================

def assert_csv_exists(path, required_fields=None):
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"CSV not found: {path}")
    if required_fields:
        with p.open(encoding="utf-8") as f:
            header = next(csv.reader(f))
            missing = [c for c in required_fields if c not in header]
            if missing:
                raise ValueError(f"{path} missing fields: {missing}")


def safe_insert_many(collection, batch, stats: dict, *, bypass_validation=True):
    """Insert batch tolerating dup keys and partial failures."""
    if not batch:
        return
    try:
        collection.insert_many(batch, ordered=False)  # ← without bypass_document_validation
        stats["ok"] = stats.get("ok", 0) + len(batch)
    except BulkWriteError as bwe:
        stats["errors"] = stats.get("errors", 0) + 1
        n_ok = bwe.details.get("nInserted", 0)
        stats["ok"] = stats.get("ok", 0) + n_ok
        n_dup = sum(1 for w in bwe.details.get("writeErrors", []) if w.get("code") == 11000)
        if n_dup:
            stats["dupkey"] = stats.get("dupkey", 0) + n_dup
    finally:
        batch.clear()


def progress_print(n, step=1_000_000, label="progress"):
    if n and n % step == 0:
        print(f"[{label}] processed: {n:,}")


# ==================== Inserters ====================

def insert_movies(collection, csv_path, *, batch_size=5000, max_runtime=873):
    """Insert cleaned movies with defensive checks."""
    print(f"[movies] inserting from {csv_path}")
    batch, stats, total_rows = [], {}, 0

    def flush():
        safe_insert_many(collection, batch, stats)

    try:
        with open(csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_rows += 1
                movie_id = parse_int(row.get("id"))
                if movie_id is None or movie_id < 0:
                    continue

                # runtime defensive filter (max 873, allow None)
                runtime = parse_float(row.get("runtime"))
                if runtime is not None and runtime > max_runtime:
                    continue
                if runtime is not None and runtime < 0:
                    runtime = None

                # belongs_to_collection (dict or None)
                belongs_raw = row.get("belongs_to_collection")
                belongs = None
                if belongs_raw and str(belongs_raw).strip().lower() not in {"", "null", "nan"}:
                    try:
                        tmp = json.loads(belongs_raw)
                    except Exception:
                        try:
                            tmp = json.loads(str(belongs_raw).replace("'", '"'))
                        except Exception:
                            tmp = None
                    belongs = tmp if isinstance(tmp, dict) else None

                document = {
                    "_id": movie_id,
                    "id": movie_id,
                    "title": row.get("title"),
                    "original_title": row.get("original_title"),
                    "overview": row.get("overview"),
                    "tagline": row.get("tagline"),
                    "release_date": parse_date(row.get("release_date")),
                    "runtime": runtime,
                    "budget": parse_float(row.get("budget")),
                    "revenue": parse_float(row.get("revenue")),
                    "vote_average": parse_float(row.get("vote_average")),
                    "vote_count": parse_int(row.get("vote_count")),
                    "genres": parse_json(row.get("genres")),
                    "production_companies": parse_json(row.get("production_companies")),
                    "production_countries": parse_json(row.get("production_countries")),
                    "spoken_languages": parse_json(row.get("spoken_languages")),
                    "belongs_to_collection": belongs,
                    "original_language": row.get("original_language"),
                }
                batch.append(document)

                if len(batch) >= batch_size:
                    flush()
                progress_print(total_rows, label="movies")
    finally:
        flush()

    total_coll = collection.count_documents({})
    print(f"[movies] ok={stats.get('ok',0):,} dup={stats.get('dupkey',0):,} errors={stats.get('errors',0)} | total in collection: {total_coll:,}")


def insert_keywords(collection, csv_path):
    """Insert keywords_clean.csv (una fila por película con lista de keywords)."""
    print(f"[keywords] inserting from {csv_path}")
    batch, stats, total = [], {}, 0

    def flush():
        safe_insert_many(collection, batch, stats)

    try:
        with open(csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                total += 1
                movie_id = parse_int(row.get("id"))
                if movie_id is None:
                    continue
                kw_list = parse_json(row.get("keywords"))
                document = {"_id": movie_id, "movie_id": movie_id, "keywords": kw_list}
                batch.append(document)
                if len(batch) >= 5000:
                    flush()
                progress_print(total, label="keywords")
    finally:
        flush()

    total_coll = collection.count_documents({})
    print(f"[keywords] ok={stats.get('ok',0):,} | total in collection: {total_coll:,}")



def insert_keywords_exploded(collection, csv_path):
    """Insert keywords_exploded.csv (una fila por keyword–película)."""
    print(f"[keywords_exploded] inserting from {csv_path}")
    batch, stats, total = [], {}, 0

    def flush():
        safe_insert_many(collection, batch, stats)

    try:
        with open(csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                total += 1
                movie_id = parse_int(row.get("movie_id"))
                keyword = row.get("keyword")
                if not movie_id or not keyword:
                    continue
                doc = {"movie_id": movie_id, "keyword": keyword.strip().lower()}
                batch.append(doc)
                if len(batch) >= 5000:
                    flush()
                progress_print(total, label="keywords_exploded")
    finally:
        flush()

    total_coll = collection.count_documents({})
    print(f"[keywords_exploded] ok={stats.get('ok',0):,} | total in collection: {total_coll:,}")



def insert_credits(collection, csv_path, *, batch_size=5000):
    print(f"[credits] inserting from {csv_path}")
    batch, stats, total_rows = [], {}, 0

    def flush():
        safe_insert_many(collection, batch, stats)

    try:
        with open(csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_rows += 1
                movie_id = parse_int(row.get("id") or row.get("movie_id"))
                if movie_id is None:
                    continue
                document = {
                    "_id": movie_id,
                    "movie_id": movie_id,
                    "cast": parse_json(row.get("cast")),
                    "crew": parse_json(row.get("crew")),
                }
                batch.append(document)
                if len(batch) >= batch_size:
                    flush()
                progress_print(total_rows, label="credits")
    finally:
        flush()

    total_coll = collection.count_documents({})
    print(f"[credits] ok={stats.get('ok',0):,} dup={stats.get('dupkey',0):,} errors={stats.get('errors',0)} | total in collection: {total_coll:,}")


def insert_links(collection, csv_path, *, batch_size=10000, require_tmdb=True):
    """Insert MovieLens links: movieId -> imdbId, tmdbId. If require_tmdb=False, allow null tmdbId rows to be skipped gracefully."""
    print(f"[links] inserting from {csv_path}")
    batch, stats, total_rows = [], {}, 0

    def flush():
        safe_insert_many(collection, batch, stats)

    try:
        with open(csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_rows += 1
                movie_id = parse_int(row.get("movieId"))
                imdb_id = parse_int(row.get("imdbId"))
                tmdb_id = parse_int(row.get("tmdbId"))
                if movie_id is None or imdb_id is None:
                    continue
                if require_tmdb and tmdb_id is None:
                    continue

                document = {
                    "_id": movie_id,  # primary key = MovieLens movieId
                    "movieId": movie_id,
                    "imdbId": imdb_id,
                    "tmdbId": tmdb_id,
                }
                batch.append(document)
                if len(batch) >= batch_size:
                    flush()
                progress_print(total_rows, label="links")
    finally:
        flush()

    total_coll = collection.count_documents({})
    print(f"[links] ok={stats.get('ok',0):,} dup={stats.get('dupkey',0):,} errors={stats.get('errors',0)} | total in collection: {total_coll:,}")


def insert_ratings(collection, csv_path, *, batch_size=100_000):
    """Insert ratings (FULL, chunk-friendly through CSV streaming)."""
    print(f"[ratings] inserting from {csv_path}")
    batch, stats, total_rows = [], {}, 0

    def flush():
        safe_insert_many(collection, batch, stats)

    try:
        with open(csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_rows += 1
                user_id = parse_int(row.get("userId"))
                movie_id = parse_int(row.get("movieId"))
                rating = parse_float(row.get("rating"))
                if user_id is None or movie_id is None or rating is None:
                    continue
                if not (0 <= rating <= 10):
                    continue

                timestamp = parse_timestamp(row.get("timestamp"))
                document = {
                    "userId": user_id,
                    "movieId": movie_id,
                    "rating": rating,
                    "timestamp": timestamp,
                }
                batch.append(document)

                if len(batch) >= batch_size:
                    flush()
                progress_print(total_rows, label="ratings")
    finally:
        flush()

    total_coll = collection.count_documents({})
    print(f"[ratings] ok={stats.get('ok',0):,} dup={stats.get('dupkey',0):,} errors={stats.get('errors',0)} | total in collection: {total_coll:,}")


# ==================== Indexes ====================

def ensure_indexes(db):
    print("[indexes] creating indexes...")

    # Movies
    db.movies.create_index([("id", 1)], unique=True, name="id_uq")
    db.movies.create_index([("title", 1)], name="title_idx")
    db.movies.create_index([("release_date", 1)], name="release_date_idx")
    db.movies.create_index([("overview", "text"), ("tagline", "text")], name="text_idx")

    # Keywords
    db.keywords.create_index([("movie_id", 1)], unique=True, name="movie_id_uq")
    db.keywords_exploded.create_index([("movie_id", 1)], name="kwexp_movie_idx")
    db.keywords_exploded.create_index([("keyword", 1)], name="kwexp_kw_idx")
    db.keywords_exploded.create_index([("keyword", "text")], name="kwexp_text_idx")

    # Credits
    db.credits.create_index([("movie_id", 1)], unique=True, name="movie_id_uq")

    # Links
    db.links.create_index([("movieId", 1)], unique=True, name="movieId_uq")
    db.links.create_index([("tmdbId", 1)], name="tmdbId_idx")

    # Ratings
    db.ratings.create_index([("movieId", 1)], name="ratings_movie_idx")
    db.ratings.create_index([("userId", 1)], name="ratings_user_idx")
    db.ratings.create_index([("movieId", 1), ("userId", 1)], name="ratings_movie_user_idx")
    db.ratings.create_index([("timestamp", 1)], name="ratings_time_idx")

    print("[indexes] done.")


# ==================== Main ====================

def main():
    parser = argparse.ArgumentParser(description="Insert cleaned Movies CSVs into MongoDB (robust & indexed).")
    parser.add_argument("--data-dir", default="data_clean", help="Directory inside container with *_clean.csv")
    parser.add_argument("--reset", action="store_true", help="Drop collections before inserting")
    parser.add_argument("--batch-movies", type=int, default=5000)
    parser.add_argument("--batch-credits", type=int, default=5000)
    parser.add_argument("--batch-links", type=int, default=10000)
    parser.add_argument("--batch-ratings", type=int, default=100000)
    parser.add_argument("--allow-null-tmdb", action="store_true", help="Allow links rows without tmdbId (they will be inserted with tmdbId=None)")
    parser.add_argument("--max-runtime", type=int, default=873, help="Max allowed runtime in minutes (default: 873)")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    movies_path = data_dir / "movies_metadata_clean.csv"
    credits_path = data_dir / "credits_clean.csv"
    links_path = data_dir / "links_clean.csv"        # FULL
    ratings_path = data_dir / "ratings_clean.csv"    # FULL
    keywords_clean_path = data_dir / "keywords_clean.csv"
    keywords_exp_path   = data_dir / "keywords_exploded.csv"

    # Basic validations
    assert_csv_exists(movies_path, ["id", "title", "release_date"])
    assert_csv_exists(credits_path, ["cast", "crew"])
    assert_csv_exists(links_path, ["movieId", "imdbId", "tmdbId"])
    assert_csv_exists(ratings_path, ["userId", "movieId", "rating", "timestamp"])

    connector = DbConnector()
    db = connector.db

    if args.reset:
        print("[reset] dropping collections if they exist...")
        for name in ("movies", "credits", "links", "ratings"):
            if name in db.list_collection_names():
                db[name].drop()

    # Insert
    insert_movies(db.movies, str(movies_path), batch_size=args.batch_movies, max_runtime=args.max_runtime)
    insert_credits(db.credits, str(credits_path), batch_size=args.batch_credits)
    insert_links(db.links, str(links_path), batch_size=args.batch_links, require_tmdb=not args.allow_null_tmdb)
    insert_ratings(db.ratings, str(ratings_path), batch_size=args.batch_ratings)
    insert_keywords(db.keywords, str(keywords_clean_path))
    insert_keywords_exploded(db.keywords_exploded, str(keywords_exp_path))

    # Indexes
    ensure_indexes(db)

    # Final counts
    print("[check] movies:", db.movies.estimated_document_count())
    print("[check] keywords:", db.keywords.estimated_document_count())
    print("[check] keywords exploded:", db.keywords_exploded.estimated_document_count())
    print("[check] credits:", db.credits.estimated_document_count())
    print("[check] links:", db.links.estimated_document_count())
    print("[check] ratings:", db.ratings.estimated_document_count())

    print("[done] All data inserted + indexes created.")
    connector.close_connection()


if __name__ == "__main__":
    main()
