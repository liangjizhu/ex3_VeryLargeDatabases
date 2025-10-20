import os
import csv
import json
import argparse
import pandas as pd

CHUNK_SIZE = 100_000


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def parse_json_safe(value):
    """Return list/dict if parseable, otherwise [].
    Accepts JSON with double quotes and python-like with single quotes."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, (list, dict)):
        return value
    if not isinstance(value, str):
        return []
    s = value.strip()
    if not s or s.lower() == "null":
        return []
    try:
        return json.loads(s)
    except Exception:
        try:
            return json.loads(s.replace("'", '"'))
        except Exception:
            return []


def clean_movies(path_in: str, path_out: str, limit: int | None = None):
    """
    Clean movies_metadata.csv:
    - numeric id unique, non-null title
    - valid release_date
    - numeric casts for budget/revenue/runtime/votes
    - list/dict columns parsed and re-serialized as valid JSON strings
    """
    first_chunk = True
    seen_ids = set()
    total_in = 0
    total_kept = 0

    for df in pd.read_csv(path_in, chunksize=CHUNK_SIZE, low_memory=False):
        total_in += len(df)

        # keep only numeric ids; use .loc + .copy() to avoid chained-assign warnings
        id_mask = pd.to_numeric(df["id"], errors="coerce").notnull()
        df = df.loc[id_mask].copy()
        df["id"] = df["id"].astype(int)

        # drop intra-chunk duplicates
        df = df.drop_duplicates(subset=["id"])

        # drop inter-chunk duplicates
        df = df[~df["id"].isin(seen_ids)]
        seen_ids.update(df["id"])

        # title required
        df = df[df["title"].notna()]

        # numeric casts
        for col in ["budget", "revenue", "runtime", "vote_average", "vote_count"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # vote_average constraints (0..10 or NaN)
        if "vote_average" in df.columns:
            df = df[df["vote_average"].between(0, 10) | df["vote_average"].isna()]

        # release_date valid
        if "release_date" in df.columns:
            df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
            df = df[df["release_date"].notna()]

        # parse JSON-like columns
        list_cols = ["genres", "production_companies", "production_countries", "spoken_languages"]
        for col in list_cols:
            if col in df.columns:
                df[col] = df[col].apply(parse_json_safe)

        # belongs_to_collection dict or None
        if "belongs_to_collection" in df.columns:
            def parse_collection(x):
                if x is None or (isinstance(x, float) and pd.isna(x)):
                    return None
                if isinstance(x, str) and x.strip().lower() in ("", "null"):
                    return None
                val = parse_json_safe(x)
                return val if isinstance(val, dict) else None
            df["belongs_to_collection"] = df["belongs_to_collection"].apply(parse_collection)

        # serialize list/dict columns to proper JSON strings
        for col in list_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else "")
        if "belongs_to_collection" in df.columns:
            df["belongs_to_collection"] = df["belongs_to_collection"].apply(
                lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, dict) else ""
            )

        # write chunk
        mode = "w" if first_chunk else "a"
        df.to_csv(path_out, index=False, header=first_chunk, mode=mode, quoting=csv.QUOTE_MINIMAL)
        total_kept += len(df)
        first_chunk = False

        if limit and total_kept >= limit:
            break

    kept_ratio = (total_kept / total_in) if total_in else 0.0
    print(f"[movies] in={total_in} out={total_kept} kept={kept_ratio:.2%}")


def clean_credits(path_in: str, path_out: str):
    df = pd.read_csv(path_in, low_memory=False)
    key_col = "id" if "id" in df.columns else "movie_id"
    df = df.drop_duplicates(subset=[key_col])

    for col in ["cast", "crew"]:
        if col in df.columns:
            df[col] = df[col].apply(parse_json_safe)

    # JSON serialize for CSV
    for col in ["cast", "crew"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, list) else "[]")

    df.to_csv(path_out, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"[credits] rows_out={len(df)}")


def clean_links(path_in: str, path_out: str):
    """Clean links.csv (full): keep rows with numeric movieId, imdbId, tmdbId."""
    df = pd.read_csv(path_in, low_memory=False)
    for col in ["movieId", "imdbId", "tmdbId"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["movieId", "imdbId", "tmdbId"])
    df["movieId"] = df["movieId"].astype(int)
    df["imdbId"] = df["imdbId"].astype(int)
    df["tmdbId"] = df["tmdbId"].astype(int)
    df = df.drop_duplicates(subset=["movieId"])  # ensure unique MovieLens movieId

    df.to_csv(path_out, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"[links] rows_out={len(df)}")


def clean_ratings(path_in: str, path_out: str, drop_dups: bool = True):
    """Clean ratings.csv (full): numeric ids, rating in [0,10], timestamp to ISO, drop duplicates per (userId, movieId)."""
    df = pd.read_csv(path_in, low_memory=False)
    for col in ["userId", "movieId"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "rating" in df.columns:
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        # keep MovieLens scale (0.5..5.0) but also accept up to 10 if present
        df = df[df["rating"].between(0, 10)]
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")
        df = df[df["timestamp"].notna()]
    if drop_dups and {"userId", "movieId"}.issubset(df.columns):
        df = df.drop_duplicates(subset=["userId", "movieId"])
    df = df.dropna(subset=["userId", "movieId"])
    df["userId"] = df["userId"].astype(int)
    df["movieId"] = df["movieId"].astype(int)

    df.to_csv(path_out, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"[ratings] rows_out={len(df)}")


def main():
    parser = argparse.ArgumentParser(description="Clean Movies Dataset CSVs (JSON-safe outputs).")
    parser.add_argument("--in-dir", default="data", help="Input directory with original CSVs")
    parser.add_argument("--out-dir", default="data_clean", help="Output directory for cleaned CSVs")
    args = parser.parse_args()

    ensure_dir(args.out_dir)

    movies_in = os.path.join(args.in_dir, "movies_metadata.csv")
    movies_out = os.path.join(args.out_dir, "movies_metadata_clean.csv")

    credits_in = os.path.join(args.in_dir, "credits.csv")
    credits_out = os.path.join(args.out_dir, "credits_clean.csv")

    links_in = os.path.join(args.in_dir, "links.csv")
    links_out = os.path.join(args.out_dir, "links_clean.csv")

    ratings_in = os.path.join(args.in_dir, "ratings.csv")
    ratings_out = os.path.join(args.out_dir, "ratings_clean.csv")

    print("[*] Cleaning movies_metadata...")
    clean_movies(movies_in, movies_out)

    print("[*] Cleaning credits...")
    clean_credits(credits_in, credits_out)

    print("[*] Cleaning links...")
    clean_links(links_in, links_out)

    print("[*] Cleaning ratings...")
    clean_ratings(ratings_in, ratings_out)

    print("[OK] Clean files written to:", args.out_dir)


if __name__ == "__main__":
    main()
