import os
import csv
import json
import argparse
from collections import Counter, defaultdict

import pandas as pd

CHUNK_SIZE = 100_000
# --- MAX RUNTIME ---
MAX_RUNTIME = 873  # Resan (The Journey) https://en.wikipedia.org/wiki/List_of_longest_films

# -----------------------------
# Helpers
# -----------------------------
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def parse_json_safe(value):
    """Return list/dict if parseable, otherwise []/None.
    Tolerates single quotes by re-encoding."""
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


def json_dump_if(obj):
    return json.dumps(obj, ensure_ascii=False) if isinstance(obj, (list, dict)) else ""


# -----------------------------
# Cleaning functions
# -----------------------------
def clean_movies(path_in: str, path_out: str, year_min: int, year_max: int, min_votes: int):
    """
    Clean movies_metadata.csv with chunked processing and detailed counters.
    - numeric unique id, non-null title
    - valid release_date within [year_min, year_max]
    - numeric casts for budget/revenue/runtime/votes
    - remove low-signal titles (vote_count < min_votes or vote_average == 0)
    - JSON columns parsed and re-serialized
    """
    first_chunk = True
    seen_ids = set()
    total_in = 0
    total_keep = 0
    drop_counters = Counter()

    for df in pd.read_csv(path_in, chunksize=CHUNK_SIZE, low_memory=False):
        total_in += len(df)

        # force numeric ids
        id_num = pd.to_numeric(df["id"], errors="coerce")
        drop_counters["movies_id_non_numeric"] += int(id_num.isna().sum())
        df = df.loc[id_num.notna()].copy()
        df["id"] = id_num.loc[id_num.notna()].astype(int)

        # title required
        drop_counters["movies_title_null"] += int(df["title"].isna().sum())
        df = df[df["title"].notna()]

        # drop dupes (intra+inter chunk)
        before = len(df)
        df = df.drop_duplicates(subset=["id"])
        drop_counters["movies_dupes_intra"] += before - len(df)

        before = len(df)
        df = df[~df["id"].isin(seen_ids)]
        drop_counters["movies_dupes_inter"] += before - len(df)
        seen_ids.update(df["id"])

        # numeric casts
        for col in ["budget", "revenue", "runtime", "vote_average", "vote_count", "popularity"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                
        # runtime filters
        if "runtime" in df.columns:
            df.loc[df["runtime"] < 0, "runtime"] = pd.NA
            too_long = df["runtime"].notna() & (df["runtime"] > MAX_RUNTIME)
            df = df[~too_long]

        # vote filters
        if "vote_average" in df.columns:
            bad_va = ~(df["vote_average"].between(0, 10) | df["vote_average"].isna())
            drop_counters["movies_vote_average_out_of_range"] += int(bad_va.sum())
            df = df[~bad_va]

        if "vote_count" in df.columns and min_votes > 0:
            low_votes = (df["vote_count"].fillna(0) < min_votes) | (df["vote_average"].fillna(0) == 0)
            drop_counters["movies_low_votes"] += int(low_votes.sum())
            df = df[~low_votes]

        # dates
        if "release_date" in df.columns:
            df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
            bad_date = df["release_date"].isna()
            drop_counters["movies_release_date_nat"] += int(bad_date.sum())
            df = df[~bad_date]

            df["year"] = df["release_date"].dt.year
            out_of_bounds = ~df["year"].between(year_min, year_max)
            drop_counters["movies_year_out_of_bounds"] += int(out_of_bounds.sum())
            df = df[~out_of_bounds]

        # parse JSON-like columns
        list_cols = ["genres", "production_companies", "production_countries", "spoken_languages"]
        for col in list_cols:
            if col in df.columns:
                df[col] = df[col].apply(parse_json_safe)

        # belongs_to_collection dict or empty
        if "belongs_to_collection" in df.columns:
            def parse_collection(x):
                if x is None or (isinstance(x, float) and pd.isna(x)):
                    return None
                if isinstance(x, str) and x.strip().lower() in ("", "null"):
                    return None
                val = parse_json_safe(x)
                return val if isinstance(val, dict) else None
            df["belongs_to_collection"] = df["belongs_to_collection"].apply(parse_collection)

        # serialize list/dict columns to JSON strings
        for col in list_cols:
            if col in df.columns:
                df[col] = df[col].apply(json_dump_if)
        if "belongs_to_collection" in df.columns:
            df["belongs_to_collection"] = df["belongs_to_collection"].apply(json_dump_if)

        # write
        mode = "w" if first_chunk else "a"
        df.to_csv(path_out, index=False, header=first_chunk, mode=mode, quoting=csv.QUOTE_MINIMAL)
        total_keep += len(df)
        first_chunk = False

    kept_ratio = (total_keep / total_in) if total_in else 0.0
    print(f"[movies] in={total_in} out={total_keep} kept={kept_ratio:.2%}")
    for k, v in drop_counters.items():
        print(f"  drop.{k}: {v}")


def clean_credits(path_in: str, path_out: str):
    """Deduplicate by id keeping the row with highest coverage (len(cast)+len(crew))."""
    df = pd.read_csv(path_in, low_memory=False)

    def to_list(x):
        v = parse_json_safe(x)
        return v if isinstance(v, list) else []
    df["cast"] = df["cast"].apply(to_list)
    df["crew"] = df["crew"].apply(to_list)
    df["coverage"] = df["cast"].str.len() + df["crew"].str.len()

    before = len(df)
    df = df.sort_values(["id", "coverage"], ascending=[True, False]).drop_duplicates(subset=["id"], keep="first")
    print(f"[credits] dedup: {before} -> {len(df)} (kept max coverage per id)")

    df["cast"] = df["cast"].apply(json_dump_if)
    df["crew"] = df["crew"].apply(json_dump_if)
    df.to_csv(path_out, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"[credits] rows_out={len(df)}")


def clean_links(path_in: str, path_out: str, keep_null_tmdb: bool):
    """Clean links.csv: coerce numeric, optionally keep rows with null tmdbId."""
    df = pd.read_csv(path_in, low_memory=False)
    for col in ["movieId", "imdbId", "tmdbId"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if not keep_null_tmdb:
        dropped = int(df["tmdbId"].isna().sum())
        df = df.dropna(subset=["movieId", "imdbId", "tmdbId"])
        print(f"[links] dropped rows with null tmdbId: {dropped}")
    else:
        df = df.dropna(subset=["movieId", "imdbId"])

    df["movieId"] = df["movieId"].astype(int)
    df["imdbId"] = df["imdbId"].astype(int)
    if df["tmdbId"].notna().any():
        df.loc[df["tmdbId"].notna(), "tmdbId"] = df.loc[df["tmdbId"].notna(), "tmdbId"].astype(int)

    before = len(df)
    df = df.drop_duplicates(subset=["movieId"])
    print(f"[links] drop dup movieId: {before} -> {len(df)}")

    df.to_csv(path_out, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"[links] rows_out={len(df)}")


def clean_ratings(path_in: str, path_out: str, keep: str = "last"):
    """
    Clean ratings.csv with chunked processing:
    - numeric ids
    - rating in [0,10]
    - timestamp to ISO
    - dedupe per (userId, movieId) keeping first/last; or keep='all' (no dedupe)
    """
    assert keep in {"first", "last", "all"}

    # We'll stream chunks, normalize types, and append.
    # If dedup needed, we can dedupe per-chunk and then a second pass on the combined file
    # (but that may still be large). Practical approach: keep='last' per user/movie within chunk,
    # and accept minimal residual dupes across chunk boundaries, or post-dedupe with groupby.
    first_chunk = True
    stats = Counter()

    # Temporary store for optional global dedupe (small memory: keys only)
    latest_ts = defaultdict(int) if keep in {"first", "last"} else None

    for df in pd.read_csv(path_in, chunksize=CHUNK_SIZE, low_memory=False):
        stats["in_rows"] += len(df)
        for col in ["userId", "movieId"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        df = df[df["rating"].between(0, 10)]

        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")
        df = df[df["timestamp"].notna()]

        df = df.dropna(subset=["userId", "movieId"])
        df["userId"] = df["userId"].astype(int)
        df["movieId"] = df["movieId"].astype(int)

        if keep != "all":
            # within-chunk dedupe
            df = df.sort_values(["userId", "movieId", "timestamp"])
            if keep == "last":
                df = df.drop_duplicates(subset=["userId", "movieId"], keep="last")
            else:
                df = df.drop_duplicates(subset=["userId", "movieId"], keep="first")

            # cross-chunk dedupe using latest_ts index
            keep_mask = []
            for r in df.itertuples(index=False):
                key = (r.userId, r.movieId)
                ts = int(r.timestamp.value)  # ns
                if keep == "last":
                    if ts >= latest_ts[key]:
                        latest_ts[key] = ts
                        keep_mask.append(True)
                    else:
                        keep_mask.append(False)
                else:  # keep==first
                    if key in latest_ts:
                        keep_mask.append(False)
                    else:
                        latest_ts[key] = ts
                        keep_mask.append(True)
            df = df.loc[keep_mask]

        mode = "w" if first_chunk else "a"
        df.to_csv(path_out, index=False, header=first_chunk, mode=mode, quoting=csv.QUOTE_MINIMAL)
        stats["out_rows"] += len(df)
        first_chunk = False

    print(f"[ratings] in={stats['in_rows']:,} out={stats['out_rows']:,} kept={stats['out_rows']/max(1,stats['in_rows']):.2%} keep={keep}")


def clean_keywords(path_in: str, path_out: str):
    """Keywords normalized (keeps JSON list in one column)."""
    df = pd.read_csv(path_in, low_memory=False)
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    drops = int(df["id"].isna().sum())
    df = df.dropna(subset=["id"]).drop_duplicates(subset=["id"])
    df["id"] = df["id"].astype(int)
    print(f"[keywords] drop id NaN: {drops}")

    df["keywords"] = df["keywords"].apply(parse_json_safe)
    # normalize names: lower + trim
    def norm_list(xs):
        out = []
        if isinstance(xs, list):
            for d in xs:
                if isinstance(d, dict) and d.get("name"):
                    name = d["name"].strip()
                    if name:
                        out.append({"id": d.get("id"), "name": name.lower()})
        return out
    df["keywords"] = df["keywords"].apply(norm_list)
    df["keywords"] = df["keywords"].apply(json_dump_if)

    df.to_csv(path_out, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"[keywords] rows_out={len(df)} -> {path_out}")


def explode_keywords(path_in: str, path_out: str):
    """Exploded keywords (one row per movie-keyword)."""
    src = pd.read_csv(path_in, low_memory=False)
    src["id"] = pd.to_numeric(src["id"], errors="coerce")
    src = src.dropna(subset=["id"])
    src["id"] = src["id"].astype(int)

    rows = []
    for r in src.itertuples(index=False):
        items = parse_json_safe(getattr(r, "keywords"))
        if isinstance(items, list):
            for d in items:
                if isinstance(d, dict) and d.get("name"):
                    name = d["name"].strip().lower()
                    if name:
                        rows.append({"movie_id": getattr(r, "id"), "keyword": name})

    out = pd.DataFrame(rows).drop_duplicates()
    out.to_csv(path_out, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"[keywords_exploded] rows_out={len(out)} -> {path_out}")


# -----------------------------
# CLI
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Clean Movies Dataset CSVs (improved, chunk-safe, with logs).")
    ap.add_argument("--in-dir", default="data", help="Input directory with original CSVs")
    ap.add_argument("--out-dir", default="data_clean", help="Output directory for cleaned CSVs")

    # Movies options
    ap.add_argument("--min-votes", type=int, default=0, help="Minimum vote_count to keep movies (default: 0 = keep all)")
    ap.add_argument("--year-min", type=int, default=1888, help="Min valid release year (default: 1888)")
    ap.add_argument("--year-max", type=int, default=2100, help="Max valid release year (default: 2100)")

    # Links options
    ap.add_argument("--keep-null-tmdb", action="store_true", help="Do NOT drop rows with null tmdbId in links")

    # Ratings options
    ap.add_argument("--ratings-keep", choices=["first", "last", "all"], default="last",
                    help="How to dedupe multiple ratings per (userId,movieId) across time")

    # Keywords options
    ap.add_argument("--explode-keywords", action="store_true", help="Also write exploded keywords file")

    args = ap.parse_args()
    ensure_dir(args.out_dir)

    # Paths
    p_movies_in  = os.path.join(args.in_dir,  "movies_metadata.csv")
    p_movies_out = os.path.join(args.out_dir, "movies_metadata_clean.csv")

    p_credits_in  = os.path.join(args.in_dir,  "credits.csv")
    p_credits_out = os.path.join(args.out_dir, "credits_clean.csv")

    p_links_in  = os.path.join(args.in_dir,  "links.csv")
    p_links_out = os.path.join(args.out_dir, "links_clean.csv")

    p_ratings_in  = os.path.join(args.in_dir,  "ratings.csv")
    p_ratings_out = os.path.join(args.out_dir, "ratings_clean.csv")

    p_keywords_in  = os.path.join(args.in_dir,  "keywords.csv")
    p_keywords_out = os.path.join(args.out_dir, "keywords_clean.csv")
    p_keywords_exp = os.path.join(args.out_dir, "keywords_exploded.csv")

    # Run
    print("[*] Cleaning movies_metadata...")
    clean_movies(p_movies_in, p_movies_out, args.year_min, args.year_max, args.min_votes)

    print("[*] Cleaning credits...")
    clean_credits(p_credits_in, p_credits_out)

    print("[*] Cleaning links...")
    clean_links(p_links_in, p_links_out, keep_null_tmdb=args.keep_null_tmdb)

    print("[*] Cleaning ratings (chunked)...")
    clean_ratings(p_ratings_in, p_ratings_out, keep=args.ratings_keep)

    if os.path.exists(p_keywords_in):
        print("[*] Cleaning keywords...")
        clean_keywords(p_keywords_in, p_keywords_out)
        if args.explode_keywords:
            print("[*] Exploding keywords...")
            explode_keywords(p_keywords_in, p_keywords_exp)
    else:
        print("[*] keywords.csv not found — skipping.")

    print("[OK] Clean files written to:", args.out_dir)


if __name__ == "__main__":
    main()
