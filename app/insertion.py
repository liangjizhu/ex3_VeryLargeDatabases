import csv
import json
import argparse
from datetime import datetime
from pathlib import Path
from DbConnector import DbConnector

# -------------------- Parse helpers --------------------

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
        return value
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


# -------------------- Loaders --------------------

def insert_movies(collection, csv_path):
    print(f"[movies] inserting from {csv_path}")
    batch, total = [], 0
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            movie_id = parse_int(row.get("id"))
            if movie_id is None:
                continue

            belongs_raw = row.get("belongs_to_collection")
            belongs = None
            if belongs_raw and str(belongs_raw).strip().lower() not in {"", "null", "nan"}:
                try:
                    belongs = json.loads(belongs_raw)
                except Exception:
                    try:
                        belongs = json.loads(str(belongs_raw).replace("'", '"'))
                    except Exception:
                        belongs = None

            document = {
                "_id": movie_id,
                "id": movie_id,
                "title": row.get("title"),
                "original_title": row.get("original_title"),
                "overview": row.get("overview"),
                "tagline": row.get("tagline"),
                "release_date": parse_date(row.get("release_date")),
                "runtime": parse_float(row.get("runtime")),
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
            if len(batch) >= 2000:
                collection.insert_many(batch, ordered=False)
                total += len(batch)
                batch.clear()
    if batch:
        collection.insert_many(batch, ordered=False)
        total += len(batch)
    print(f"[movies] inserted now: {total} | total in collection: {collection.count_documents({})}")


def insert_credits(collection, csv_path):
    print(f"[credits] inserting from {csv_path}")
    batch, total = [], 0
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
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
            if len(batch) >= 2000:
                collection.insert_many(batch, ordered=False)
                total += len(batch)
                batch.clear()
    if batch:
        collection.insert_many(batch, ordered=False)
        total += len(batch)
    print(f"[credits] inserted now: {total} | total in collection: {collection.count_documents({})}")


def insert_links(collection, csv_path):
    """Insert MovieLens links: movieId -> imdbId, tmdbId (FULL)."""
    print(f"[links] inserting from {csv_path}")
    batch, total = [], 0
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            movie_id = parse_int(row.get("movieId"))
            imdb_id = parse_int(row.get("imdbId"))
            tmdb_id = parse_int(row.get("tmdbId"))
            if movie_id is None or imdb_id is None or tmdb_id is None:
                continue
            document = {
                "_id": movie_id,  # primary key = MovieLens movieId
                "movieId": movie_id,
                "imdbId": imdb_id,
                "tmdbId": tmdb_id
            }
            batch.append(document)
            if len(batch) >= 5000:
                collection.insert_many(batch, ordered=False)
                total += len(batch)
                batch.clear()
    if batch:
        collection.insert_many(batch, ordered=False)
        total += len(batch)
    print(f"[links] inserted now: {total} | total in collection: {collection.count_documents({})}")


def insert_ratings(collection, csv_path):
    """Insert ratings (FULL)."""
    print(f"[ratings] inserting from {csv_path}")
    batch, total = [], 0
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            user_id = parse_int(row.get("userId"))
            movie_id = parse_int(row.get("movieId"))
            rating = parse_float(row.get("rating"))
            if user_id is None or movie_id is None or rating is None:
                continue

            timestamp_str = row.get("timestamp")
            timestamp = None
            if timestamp_str and str(timestamp_str).isdigit():
                try:
                    timestamp = datetime.fromtimestamp(int(timestamp_str))
                except Exception:
                    timestamp = None
            else:
                try:
                    timestamp = datetime.fromisoformat(timestamp_str)
                except Exception:
                    timestamp = None

            document = {
                "userId": user_id,
                "movieId": movie_id,
                "rating": rating,
                "timestamp": timestamp,
            }
            batch.append(document)
            if len(batch) >= 50_000:
                collection.insert_many(batch, ordered=False)
                total += len(batch)
                batch.clear()
    if batch:
        collection.insert_many(batch, ordered=False)
        total += len(batch)
    print(f"[ratings] inserted now: {total} | total in collection: {collection.count_documents({})}")


# -------------------- Main --------------------

def main():
    parser = argparse.ArgumentParser(description="Insert cleaned Movies CSVs into MongoDB (full links and ratings).")
    parser.add_argument("--data-dir", default="data_clean", help="Directory inside container with *_clean.csv")
    parser.add_argument("--reset", action="store_true", help="Drop collections before inserting")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    movies_path = data_dir / "movies_metadata_clean.csv"
    credits_path = data_dir / "credits_clean.csv"
    links_path = data_dir / "links_clean.csv"        # FULL
    ratings_path = data_dir / "ratings_clean.csv"    # FULL

    connector = DbConnector()
    db = connector.db

    if args.reset:
        print("[reset] dropping collections if they exist...")
        for name in ("movies", "credits", "links", "ratings"):
            if name in db.list_collection_names():
                db[name].drop()

    insert_movies(db.movies, str(movies_path))
    insert_credits(db.credits, str(credits_path))
    insert_links(db.links, str(links_path))
    insert_ratings(db.ratings, str(ratings_path))

    print("[done] All data inserted.")
    connector.close_connection()


if __name__ == "__main__":
    main()
