import streamlit as st
import pandas as pd
from typing import List, Optional, Tuple

from recommender import logic as rlogic


SESSION_KEYS = {
    'seeds': 'seed_track_uris',
    'playlist_id': 'playlist_id',
    'playlist_name': 'playlist_name',
    'seed_mode': 'seed_mode',
    'model': 'model',
    'top_k': 'top_k',
    'seen': 'seen_track_uris',
    'explain_seeds': 'explain_seed_track_uris',
    'recs': 'recommendations_df',
}


@st.cache_data(ttl=900, show_spinner=False)
def _cached_fetch_playlist_seed_tracks(playlist_id: str) -> pd.DataFrame:
    return rlogic.fetch_playlist_seed_tracks(playlist_id)


@st.cache_data(ttl=900, show_spinner=False)
def _cached_get_recommendations(
    seed_track_uris: Tuple[str, ...],
    playlist_id: Optional[str],
    model: str,
    top_k: int,
) -> pd.DataFrame:
    return rlogic.get_recommendations(
        seed_track_ids=list(seed_track_uris) if seed_track_uris else None,
        playlist_id=playlist_id or None,
        model=(model or '').lower(),
        top_k=int(top_k),
    )


def save_inputs_to_session(
    seed_track_uris: List[str],
    playlist_id: Optional[str],
    playlist_name: Optional[str],
    seed_mode: str,
    model: str,
    top_k: int,
):
    st.session_state[SESSION_KEYS['seeds']] = seed_track_uris or []
    st.session_state[SESSION_KEYS['playlist_id']] = playlist_id
    st.session_state[SESSION_KEYS['playlist_name']] = playlist_name
    st.session_state[SESSION_KEYS['seed_mode']] = seed_mode
    st.session_state[SESSION_KEYS['model']] = model
    st.session_state[SESSION_KEYS['top_k']] = int(top_k)

    # Clear any previous run outputs.
    for k in (SESSION_KEYS['seen'], SESSION_KEYS['explain_seeds'], SESSION_KEYS['recs']):
        if k in st.session_state:
            del st.session_state[k]


def load_inputs_from_session() -> Tuple[List[str], Optional[str], Optional[str], str, str, int]:
    return (
        st.session_state.get(SESSION_KEYS['seeds'], []),
        st.session_state.get(SESSION_KEYS['playlist_id'], None),
        st.session_state.get(SESSION_KEYS['playlist_name'], None),
        st.session_state.get(SESSION_KEYS['seed_mode'], 'Track name'),
        st.session_state.get(SESSION_KEYS['model'], 'Co-occurrence'),
        int(st.session_state.get(SESSION_KEYS['top_k'], 10)),
    )


def clear_session_inputs():
    for k in SESSION_KEYS.values():
        if k in st.session_state:
            del st.session_state[k]


def run_recommender_and_store(max_explain_seeds: int = 8):
    seed_track_uris, playlist_id, _, _, model, top_k = load_inputs_from_session()

    # Resolve "seen" tracks so we can guarantee they never appear in results.
    seen: set[str] = set([str(u) for u in (seed_track_uris or [])])
    explain_seeds: List[str] = []

    if playlist_id:
        seeds_df = _cached_fetch_playlist_seed_tracks(playlist_id)
        playlist_tracks = seeds_df['track_uri'].astype(str).tolist() if not seeds_df.empty else []
        seen.update(playlist_tracks)
        explain_seeds = playlist_tracks[:max_explain_seeds]
    else:
        explain_seeds = [str(u) for u in (seed_track_uris or [])][:max_explain_seeds]

    recs = _cached_get_recommendations(
        seed_track_uris=tuple([str(u) for u in (seed_track_uris or [])]),
        playlist_id=playlist_id or None,
        model=model,
        top_k=int(top_k),
    )

    if recs is None or recs.empty:
        st.session_state[SESSION_KEYS['recs']] = pd.DataFrame()
        st.session_state[SESSION_KEYS['seen']] = list(seen)
        st.session_state[SESSION_KEYS['explain_seeds']] = explain_seeds
        return

    # Defensive filtering: exclude any already-seen tracks.
    recs = recs[~recs['track_uri'].astype(str).isin(seen)].copy()
    recs = recs.reset_index(drop=True)
    recs['rank'] = range(1, len(recs) + 1)

    st.session_state[SESSION_KEYS['recs']] = recs
    st.session_state[SESSION_KEYS['seen']] = list(seen)
    st.session_state[SESSION_KEYS['explain_seeds']] = explain_seeds


def get_cached_recommendations() -> pd.DataFrame:
    df = st.session_state.get(SESSION_KEYS['recs'], None)
    if df is None:
        return pd.DataFrame()
    return df


def get_seen_track_uris() -> List[str]:
    return st.session_state.get(SESSION_KEYS['seen'], [])


def get_explain_seed_track_uris() -> List[str]:
    return st.session_state.get(SESSION_KEYS['explain_seeds'], [])


def generate_recommendations(seed_track_uris: List[str], playlist_id: Optional[str], model: str, top_k: int) -> pd.DataFrame:
    return rlogic.get_recommendations(
        seed_track_ids=seed_track_uris or None,
        playlist_id=playlist_id or None,
        model=(model or '').lower(),
        top_k=int(top_k),
    )


def cooccurrence_matrix(seed_track_uris: List[str], neighbor_k: int = 50) -> pd.DataFrame:
    rows = []
    for s in seed_track_uris:
        df = rlogic.fetch_cooccurrence_pairs(s, top_k=neighbor_k)
        if df is None or df.empty:
            continue
        for _, r in df.iterrows():
            rows.append(
                {
                    'seed': s,
                    'neighbor': r['track_uri'],
                    'weight': int(r.get('weight', 0)),
                    'neighbor_title': r.get('track_title'),
                }
            )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)
