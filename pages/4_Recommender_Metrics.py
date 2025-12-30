import streamlit as st
import pandas as pd

from db import missing_credentials
from recommender import logic as rlogic
from recommender import ui_helpers as uihelpers


st.set_page_config(page_title="Recommender Metrics", layout="wide")

st.title("Recommender Metrics")
st.caption("Page 4/4 — Lightweight metrics to sanity-check the recommendation run.")

missing = missing_credentials()
if missing:
    st.error("Databricks connection is not configured for this run.")
    st.code("\n".join(missing))
    st.stop()

seed_track_uris, playlist_id, playlist_name, seed_mode, model, top_k = uihelpers.load_inputs_from_session()
recs = uihelpers.get_cached_recommendations()

if recs is None or recs.empty:
    st.info("No cached recommendations found. Go to the input page (Home) and click 'Generate recommendations'.")
    st.stop()

model_l = (model or "").lower()

rec_track_uris = recs["track_uri"].astype(str).tolist()
unique_artists = recs["artist_name"].dropna().astype(str).nunique()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Recommendations", f"{len(recs):,}")
c2.metric("Unique artists", f"{unique_artists:,}")
c3.metric("Avg score", f"{float(recs['score'].mean()):.2f}" if not recs.empty else "—")
c4.metric("Min/Max score", f"{int(recs['score'].min())}/{int(recs['score'].max())}" if not recs.empty else "—")

st.divider()

st.subheader("Coverage")

# Artist coverage is an interpretable proxy for diversity.
artist_coverage = unique_artists / max(len(recs), 1)

seed_for_metrics = uihelpers.get_explain_seed_track_uris() or (seed_track_uris[:8] if seed_track_uris else [])
seed_meta = rlogic.fetch_tracks_metadata(seed_for_metrics) if seed_for_metrics else pd.DataFrame()
seed_artists = set(seed_meta.get("artist_name", pd.Series([], dtype=str)).dropna().astype(str).tolist())
rec_artists = set(recs["artist_name"].dropna().astype(str).tolist())
new_artist_rate = (len(rec_artists - seed_artists) / max(len(rec_artists), 1)) if rec_artists else 0.0

st.write(
    {
        "artist_coverage (unique artists / recommendations)": round(artist_coverage, 3),
        "new_artist_rate (recommended artists not in seed subset)": round(new_artist_rate, 3),
    }
)

st.divider()

st.subheader("Popularity bias (indication)")
st.caption("Compares average playlist reach of recommendations vs. the seed subset.")

pop_recs = rlogic.track_popularity_for_uris(rec_track_uris)
pop_seeds = rlogic.track_popularity_for_uris(seed_for_metrics) if seed_for_metrics else pd.DataFrame(columns=["track_uri", "popularity"])

avg_pop_recs = float(pop_recs["popularity"].mean()) if not pop_recs.empty else 0.0
avg_pop_seeds = float(pop_seeds["popularity"].mean()) if not pop_seeds.empty else 0.0
ratio = (avg_pop_recs / avg_pop_seeds) if avg_pop_seeds > 0 else None

st.write(
    {
        "avg_popularity_recommendations": round(avg_pop_recs, 2),
        "avg_popularity_seed_subset": round(avg_pop_seeds, 2),
        "ratio (recs / seeds)": (round(ratio, 2) if ratio is not None else "n/a"),
    }
)

if ratio is not None:
    if ratio > 1.25:
        st.info("Indication: recommendations skew more popular than the seed subset.")
    elif ratio < 0.8:
        st.info("Indication: recommendations skew less popular than the seed subset.")
    else:
        st.info("Indication: recommendation popularity is broadly similar to the seed subset.")

st.divider()

st.subheader("Average co-occurrence strength")

if model_l.startswith("co"):
    st.write(
        {
            "avg_shared_playlists": round(float(recs["score"].mean()), 2),
            "median_shared_playlists": round(float(recs["score"].median()), 2),
        }
    )
else:
    st.info("Not applicable: the selected model is popularity.")

if seed_mode == "Playlist name" and playlist_id:
    st.caption(
        "Note: for metrics that compare against a seed set, we use a small seed subset for practicality and readability."
    )
