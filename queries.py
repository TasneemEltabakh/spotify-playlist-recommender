def playlist_seed_tracks_sql(playlist_id: str) -> str:
    return f"""
    SELECT f.track_uri
    FROM default.fact_playlist_track f
    WHERE f.playlist_id = '{playlist_id}'
    ORDER BY f.track_position
    """


def popularity_sql(exclude_track_uris, top_k: int) -> str:
    exclude_clause = ""
    if exclude_track_uris:
        quoted = ",".join([f"'{u}'" for u in exclude_track_uris])
        exclude_clause = f"WHERE t.track_uri NOT IN ({quoted})"

    return f"""
    SELECT t.track_uri, t.track_title, t.artist_name, COUNT(f.playlist_id) as score
    FROM default.fact_playlist_track f
    JOIN default.dim_track t ON f.track_uri = t.track_uri
    {exclude_clause}
    GROUP BY t.track_uri, t.track_title, t.artist_name
    ORDER BY score DESC
    LIMIT {top_k}
    """


def popularity_from_gold_summary_sql(exclude_track_uris, top_k: int, table_name: str = "gold_track_summary") -> str:
        """Fast popularity using an existing gold summary table.

        Expected columns (as used in the provided Frontend):
        - track_uri
        - playlists_count

        This intentionally does not replace `popularity_sql`; callers should fall back
        to `popularity_sql` if the gold table/columns aren't available.
        """
        exclude_clause = ""
        if exclude_track_uris:
                quoted = ",".join([f"'{u}'" for u in exclude_track_uris])
                exclude_clause = f"WHERE t.track_uri NOT IN ({quoted})"

        # Use `t.*` from dim_track for consistent naming.
        return f"""
        SELECT
            t.track_uri,
            t.track_title,
            t.artist_name,
            CAST(s.playlists_count AS BIGINT) AS score
        FROM {table_name} s
        JOIN default.dim_track t ON s.track_uri = t.track_uri
        {exclude_clause}
        ORDER BY score DESC
        LIMIT {top_k}
        """


def popularity_excluding_playlist_sql(playlist_id: str, top_k: int) -> str:
    p = playlist_id.replace("'", "''")
    return f"""
    WITH seed_tracks AS (
        SELECT track_uri
        FROM default.fact_playlist_track
        WHERE playlist_id = '{p}'
    )
    SELECT t.track_uri, t.track_title, t.artist_name, COUNT(DISTINCT f.playlist_id) as score
    FROM default.fact_playlist_track f
    JOIN default.dim_track t ON f.track_uri = t.track_uri
    WHERE t.track_uri NOT IN (SELECT track_uri FROM seed_tracks)
    GROUP BY t.track_uri, t.track_title, t.artist_name
    ORDER BY score DESC
    LIMIT {top_k}
    """


def cooccurrence_sql(seed_track_uris, top_k: int) -> str:
    # seeds should be a list of track_uri strings
    seeds_quoted = ",".join([f"'{s}'" for s in seed_track_uris])
    return f"""
    WITH seed_playlists AS (
        SELECT DISTINCT playlist_id
        FROM default.fact_playlist_track
        WHERE track_uri IN ({seeds_quoted})
    ),
    candidate_counts AS (
        SELECT f.track_uri, COUNT(DISTINCT f.playlist_id) AS cnt
        FROM default.fact_playlist_track f
        JOIN seed_playlists s ON f.playlist_id = s.playlist_id
        WHERE f.track_uri NOT IN ({seeds_quoted})
        GROUP BY f.track_uri
    )
    SELECT t.track_uri, t.track_title, t.artist_name, c.cnt as score
    FROM candidate_counts c
    JOIN default.dim_track t ON c.track_uri = t.track_uri
    ORDER BY score DESC
    LIMIT {top_k}
    """


def cooccurrence_from_playlist_sql(playlist_id: str, top_k: int) -> str:
    p = playlist_id.replace("'", "''")
    return f"""
    WITH seed_tracks AS (
        SELECT track_uri
        FROM default.fact_playlist_track
        WHERE playlist_id = '{p}'
    ),
    seed_playlists AS (
        SELECT DISTINCT f.playlist_id
        FROM default.fact_playlist_track f
        JOIN seed_tracks s ON f.track_uri = s.track_uri
    ),
    candidate_counts AS (
        SELECT f.track_uri, COUNT(DISTINCT f.playlist_id) AS cnt
        FROM default.fact_playlist_track f
        JOIN seed_playlists sp ON f.playlist_id = sp.playlist_id
        WHERE f.track_uri NOT IN (SELECT track_uri FROM seed_tracks)
        GROUP BY f.track_uri
    )
    SELECT t.track_uri, t.track_title, t.artist_name, c.cnt as score
    FROM candidate_counts c
    JOIN default.dim_track t ON c.track_uri = t.track_uri
    ORDER BY score DESC
    LIMIT {top_k}
    """


def search_tracks_by_title_sql(title: str, limit: int = 10) -> str:
    t = title.replace("'", "''")
    return f"""
    SELECT DISTINCT t.track_uri, t.track_title, t.artist_name
    FROM default.dim_track t
    WHERE t.track_title ILIKE '%{t}%'
    LIMIT {limit}
    """


def search_artist_top_tracks_sql(artist_name: str, limit: int = 10) -> str:
    a = artist_name.replace("'", "''")
    return f"""
    SELECT t.track_uri, t.track_title, t.artist_name, COUNT(f.playlist_id) as score
    FROM default.dim_track t
    JOIN default.fact_playlist_track f ON t.track_uri = f.track_uri
    WHERE t.artist_name ILIKE '%{a}%'
    GROUP BY t.track_uri, t.track_title, t.artist_name
    ORDER BY score DESC
    LIMIT {limit}
    """


def search_playlists_by_name_sql(name: str, limit: int = 10) -> str:
    n = name.replace("'", "''")
    return f"""
    SELECT DISTINCT playlist_id, playlist_name
    FROM default.dim_playlist
    WHERE playlist_name ILIKE '%{n}%'
    LIMIT {limit}
    """


def stats_sql() -> str:
    return """
    SELECT
      (SELECT COUNT(DISTINCT track_uri) FROM default.dim_track) as tracks,
      (SELECT COUNT(DISTINCT playlist_id) FROM default.fact_playlist_track) as playlists,
      (SELECT COUNT(DISTINCT artist_name) FROM default.dim_track) as artists
    """


def top_artists_sql(limit: int = 10) -> str:
    return f"""
    SELECT t.artist_name, COUNT(DISTINCT f.track_uri) as n_tracks
    FROM default.dim_track t
    JOIN default.fact_playlist_track f ON t.track_uri = f.track_uri
    GROUP BY t.artist_name
    ORDER BY n_tracks DESC
    LIMIT {limit}
    """


def cooccurrence_pairs_sql(seed_track_uri: str, limit: int = 100) -> str:
        s = seed_track_uri.replace("'", "''")
        return f"""
        WITH seed_playlists AS (
            SELECT DISTINCT playlist_id
            FROM default.fact_playlist_track
            WHERE track_uri = '{s}'
        )
        SELECT
            f2.track_uri AS other_track_uri,
            COUNT(DISTINCT f2.playlist_id) AS cnt
        FROM default.fact_playlist_track f2
        JOIN seed_playlists sp ON f2.playlist_id = sp.playlist_id
        WHERE f2.track_uri != '{s}'
        GROUP BY f2.track_uri
        ORDER BY cnt DESC
        LIMIT {limit}
        """


def tracks_metadata_sql(track_uris, limit=None) -> str:
    if not track_uris:
        return "SELECT track_uri, track_title, artist_name FROM default.dim_track WHERE 1 = 0"
    quoted = ",".join([f"'{str(u).replace("'", "''")}'" for u in track_uris])
    lim = f"LIMIT {int(limit)}" if limit else ""
    return f"""
    SELECT track_uri, track_title, artist_name
    FROM default.dim_track
    WHERE track_uri IN ({quoted})
    {lim}
    """


def track_popularity_for_uris_sql(track_uris) -> str:
    if not track_uris:
        return "SELECT track_uri, 0 as popularity FROM default.dim_track WHERE 1 = 0"
    quoted = ",".join([f"'{str(u).replace("'", "''")}'" for u in track_uris])
    return f"""
    SELECT f.track_uri, COUNT(DISTINCT f.playlist_id) AS popularity
    FROM default.fact_playlist_track f
    WHERE f.track_uri IN ({quoted})
    GROUP BY f.track_uri
    """


def seed_candidate_cooccurrence_sql(seed_track_uris, candidate_track_uris) -> str:
    if not seed_track_uris or not candidate_track_uris:
        return """
        SELECT
          CAST(NULL AS STRING) AS seed_track_uri,
          CAST(NULL AS STRING) AS candidate_track_uri,
          CAST(0 AS BIGINT) AS shared_playlists
        WHERE 1 = 0
        """

    seeds_quoted = ",".join([f"'{str(s).replace("'", "''")}'" for s in seed_track_uris])
    cands_quoted = ",".join([f"'{str(c).replace("'", "''")}'" for c in candidate_track_uris])

    return f"""
    WITH seed_in_playlists AS (
        SELECT DISTINCT playlist_id, track_uri AS seed_track_uri
        FROM default.fact_playlist_track
        WHERE track_uri IN ({seeds_quoted})
    ),
    cand_in_playlists AS (
        SELECT DISTINCT playlist_id, track_uri AS candidate_track_uri
        FROM default.fact_playlist_track
        WHERE track_uri IN ({cands_quoted})
    )
    SELECT
        s.seed_track_uri,
        c.candidate_track_uri,
        COUNT(DISTINCT s.playlist_id) AS shared_playlists
    FROM seed_in_playlists s
    JOIN cand_in_playlists c ON s.playlist_id = c.playlist_id
    GROUP BY s.seed_track_uri, c.candidate_track_uri
    """

