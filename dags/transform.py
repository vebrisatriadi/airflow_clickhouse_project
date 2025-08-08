from datetime import datetime
from typing import Any, Dict, List, Tuple

MovieRecord = Dict[str, Any]


def transform_movies(records: List[MovieRecord]) -> Tuple[
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
]:
    """Transform raw TMDB movie records into structured data.

    Returns tuples of lists for fact table entries, unique genres, unique production
    companies, and bridge tables linking movies to genres and companies. Duplicate
    genres and companies are removed. Release dates are converted to
    ``datetime.date`` objects when valid, otherwise ``None``.
    """

    entries: Dict[int, Dict[str, Any]] = {}
    genres: Dict[int, str] = {}
    companies: Dict[int, str] = {}
    entry_genres_set: set[Tuple[int, int]] = set()
    entry_companies_set: set[Tuple[int, int]] = set()

    for data in records:
        if not (data.get("id") and (data.get("title") or data.get("name"))):
            continue

        entry_id = data["id"]
        title = data.get("title") or data.get("name")
        raw_date = data.get("release_date") or data.get("first_air_date")
        release_date_obj = None
        if raw_date:
            try:
                release_date_obj = datetime.strptime(raw_date, "%Y-%m-%d").date()
            except (ValueError, TypeError):
                pass

        entries[entry_id] = {
            "movie_id": entry_id,
            "title": title,
            "release_date": release_date_obj,
            "revenue": data.get("revenue", 0),
            "budget": data.get("budget", 0),
            "popularity": data.get("popularity", 0.0),
            "vote_average": data.get("vote_average", 0.0),
            "vote_count": data.get("vote_count", 0),
            "runtime": data.get("runtime"),
        }

        for g in data.get("genres", []):
            genres[g["id"]] = g["name"]
            entry_genres_set.add((entry_id, g["id"]))

        for c in data.get("production_companies", []):
            companies[c["id"]] = c["name"]
            entry_companies_set.add((entry_id, c["id"]))

    entry_genres = [
        {"movie_id": mid, "genre_id": gid} for (mid, gid) in sorted(entry_genres_set)
    ]
    entry_companies = [
        {"movie_id": mid, "company_id": cid}
        for (mid, cid) in sorted(entry_companies_set)
    ]

    return (
        list(entries.values()),
        [
            {"genre_id": gid, "genre_name": name}
            for gid, name in sorted(genres.items())
        ],
        [
            {"company_id": cid, "company_name": name}
            for cid, name in sorted(companies.items())
        ],
        entry_genres,
        entry_companies,
    )

