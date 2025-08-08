import json

def parse_event(raw: str):
    """Parse a JSON string from the stream into a tuple.

    Parameters
    ----------
    raw: str
        JSON string containing ``movie_id`` and ``rating`` fields.

    Returns
    -------
    tuple[int, float]
        ``(movie_id, rating)`` where movie_id is an integer and rating is a float.
    """
    event = json.loads(raw)
    return int(event["movie_id"]), float(event["rating"])
