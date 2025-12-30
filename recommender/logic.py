from typing import List, Optional
import pandas as pd

from queries import (
    playlist_seed_tracks_sql,
    popularity_sql,
    popularity_from_gold_summary_sql,
    cooccurrence_sql,
    cooccurrence_from_playlist_sql,
    popularity_excluding_playlist_sql,
    cooccurrence_pairs_sql,
    search_tracks_by_title_sql,
    search_artist_top_tracks_sql,
    search_playlists_by_name_sql,
    stats_sql,
    top_artists_sql,
    tracks_metadata_sql,
    track_popularity_for_uris_sql,
    seed_candidate_cooccurrence_sql,
)
from db import execute_sql


def fetch_playlist_seed_tracks(playlist_id: str) -> pd.DataFrame:
    q = playlist_seed_tracks_sql(playlist_id)
    return execute_sql(q)


def search_tracks_by_title(title: str, limit: int = 10) -> pd.DataFrame:
    q = search_tracks_by_title_sql(title, limit)
    return execute_sql(q)


def search_artist_top_tracks(artist_name: str, limit: int = 10) -> pd.DataFrame:
    q = search_artist_top_tracks_sql(artist_name, limit)
    return execute_sql(q)


def search_playlists_by_name(name: str, limit: int = 10) -> pd.DataFrame:
    q = search_playlists_by_name_sql(name, limit)
    return execute_sql(q)


def get_stats() -> dict:
    q = stats_sql()
    df = execute_sql(q)
    if df.empty:
        return {}
    row = df.iloc[0].to_dict()
    return row


def top_artists(limit: int = 10) -> pd.DataFrame:
    q = top_artists_sql(limit)
    return execute_sql(q)


def fetch_cooccurrence_pairs(seed_track_uri: str, top_k: int = 100) -> pd.DataFrame:
    """Return co-occurring tracks and counts for a given seed track URI."""
    q = cooccurrence_pairs_sql(seed_track_uri, limit=top_k)
    df = execute_sql(q)
    if df.empty:
        return df
    # Join to dim_track to fetch titles
    # Build a query to fetch titles for the returned URIs
    uris = df['other_track_uri'].astype(str).tolist()
    if not uris:
        return pd.DataFrame()
    quoted = ",".join([f"'{u}'" for u in uris])
    title_q = f"SELECT track_uri, track_title, artist_name FROM default.dim_track WHERE track_uri IN ({quoted})"
    titles = execute_sql(title_q)
    if titles.empty:
        df['track_title'] = df['other_track_uri']
        df['artist_name'] = None
        df = df.rename(columns={'other_track_uri': 'track_uri', 'cnt': 'weight'})
        return df[['track_uri','track_title','artist_name','weight']]
    merged = df.merge(titles, left_on='other_track_uri', right_on='track_uri', how='left')
    merged = merged.rename(columns={'cnt': 'weight'})
    return merged[['other_track_uri','track_title','artist_name','weight']].rename(columns={'other_track_uri':'track_uri'})


def fetch_tracks_metadata(track_uris: List[str]) -> pd.DataFrame:
    q = tracks_metadata_sql(track_uris)
    return execute_sql(q)


def track_popularity_for_uris(track_uris: List[str]) -> pd.DataFrame:
    q = track_popularity_for_uris_sql(track_uris)
    return execute_sql(q)


def seed_candidate_cooccurrence(seed_track_uris: List[str], candidate_track_uris: List[str]) -> pd.DataFrame:
    q = seed_candidate_cooccurrence_sql(seed_track_uris, candidate_track_uris)
    return execute_sql(q)


def recommend_by_popularity(seed_track_ids: Optional[List[str]], top_k: int) -> pd.DataFrame:
    # Fast path: use gold summary if available; fall back to counting the fact table.
    df = pd.DataFrame()
    try:
        # Prefer unqualified table name (matches the provided Frontend queries).
        q_fast = popularity_from_gold_summary_sql(seed_track_ids, top_k, table_name='gold_track_summary')
        df = execute_sql(q_fast)
    except Exception:
        df = pd.DataFrame()

    if df is None or df.empty:
        try:
            # Try schema-qualified variant as a secondary fast attempt.
            q_fast2 = popularity_from_gold_summary_sql(seed_track_ids, top_k, table_name='default.gold_track_summary')
            df = execute_sql(q_fast2)
        except Exception:
            df = pd.DataFrame()

    if df is None or df.empty:
        q = popularity_sql(seed_track_ids, top_k)
        df = execute_sql(q)
    if df.empty:
        return df
    df = df.rename(columns={'cnt': 'score'}) if 'cnt' in df.columns else df
    df = df[['track_uri','track_title','artist_name','score']]
    df['rank'] = range(1, len(df)+1)
    return df[['rank','track_uri','track_title','artist_name','score']]


def recommend_global_popularity(top_k: int) -> pd.DataFrame:
    """Return global popularity ranking (no seeds excluded)."""
    # Reuse the same fast path by passing an empty exclude list.
    df = recommend_by_popularity([], top_k)
    if df.empty:
        return df
    return df


def recommend_by_cooccurrence(seed_track_ids: List[str], top_k: int) -> pd.DataFrame:
    q = cooccurrence_sql(seed_track_ids, top_k)
    df = execute_sql(q)
    if df.empty:
        return df
    df = df[['track_uri','track_title','artist_name','score']]
    df['rank'] = range(1, len(df)+1)
    return df[['rank','track_uri','track_title','artist_name','score']]


def recommend_by_cooccurrence_from_playlist(playlist_id: str, top_k: int) -> pd.DataFrame:
    q = cooccurrence_from_playlist_sql(playlist_id, top_k)
    df = execute_sql(q)
    if df.empty:
        return df
    df = df[['track_uri', 'track_title', 'artist_name', 'score']]
    df['rank'] = range(1, len(df) + 1)
    return df[['rank', 'track_uri', 'track_title', 'artist_name', 'score']]


def recommend_by_popularity_excluding_playlist(playlist_id: str, top_k: int) -> pd.DataFrame:
    # Avoid a full-table aggregation with a correlated NOT IN; instead, fetch the
    # playlist tracks and reuse the popularity recommender's exclude list.
    seeds_df = fetch_playlist_seed_tracks(playlist_id)
    playlist_tracks = seeds_df['track_uri'].astype(str).tolist() if not seeds_df.empty else []
    return recommend_by_popularity(playlist_tracks, top_k)


def get_recommendations(seed_track_ids: Optional[List[str]] = None,
                        playlist_id: Optional[str] = None,
                        model: str = 'co-occurrence',
                        top_k: int = 10) -> pd.DataFrame:
    model_l = (model or '').lower()

    # Playlist-based seed: avoid huge IN (...) lists for large playlists.
    if playlist_id and not seed_track_ids:
        if model_l.startswith('pop'):
            return recommend_by_popularity_excluding_playlist(playlist_id, top_k)
        return recommend_by_cooccurrence_from_playlist(playlist_id, top_k)

    # Track-based seed
    if model_l.startswith('pop'):
        return recommend_by_popularity(seed_track_ids or [], top_k)

    if not seed_track_ids:
        raise ValueError('Co-occurrence model requires at least one seed track URI')
    return recommend_by_cooccurrence(seed_track_ids, top_k)
