from datetime import date

import os
import sys

# Ensure project root is on the Python path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from dags.transform import transform_movies


def test_transform_movies_converts_dates_and_filters_duplicates():
    raw_data = [
        {
            "id": 1,
            "title": "Movie A",
            "release_date": "2020-01-01",
            "genres": [
                {"id": 10, "name": "Action"},
                {"id": 10, "name": "Action"},
            ],
            "production_companies": [
                {"id": 100, "name": "Company A"},
                {"id": 100, "name": "Company A"},
            ],
        },
        {
            "id": 2,
            "title": "Movie B",
            "release_date": "invalid-date",
            "genres": [
                {"id": 10, "name": "Action"},
                {"id": 20, "name": "Drama"},
            ],
            "production_companies": [
                {"id": 100, "name": "Company A"},
                {"id": 200, "name": "Company B"},
            ],
        },
    ]

    entries, genres, companies, _, _ = transform_movies(raw_data)

    assert entries[0]["release_date"] == date(2020, 1, 1)
    assert entries[1]["release_date"] is None

    assert sorted(genres, key=lambda g: g["genre_id"]) == [
        {"genre_id": 10, "genre_name": "Action"},
        {"genre_id": 20, "genre_name": "Drama"},
    ]

    assert sorted(companies, key=lambda c: c["company_id"]) == [
        {"company_id": 100, "company_name": "Company A"},
        {"company_id": 200, "company_name": "Company B"},
    ]


