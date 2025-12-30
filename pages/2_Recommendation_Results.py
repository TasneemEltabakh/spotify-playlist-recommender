import streamlit as st
import pandas as pd

from db import missing_credentials
from recommender import ui_helpers as uihelpers


st.set_page_config(page_title="Recommendation Results", layout="wide")

st.title("Recommendation Results")
st.caption("Page 2/4 — Ranked recommendations (no inputs on this page).")

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

context = {
    "Seed type": seed_mode,
    "Model": model,
    "Top-K requested": int(top_k),
}
if playlist_id:
    context["Playlist"] = playlist_name or playlist_id

with st.expander("Run context", expanded=False):
    st.write(context)
    st.write({"seed_track_uris": seed_track_uris, "playlist_id": playlist_id})

score_label = "Shared playlists (seed ↔ recommended)" if (model or "").lower().startswith("co") else "Playlist appearances (global popularity)"

show = recs[["rank", "track_title", "artist_name", "score"]].copy()
show = show.rename(columns={"score": score_label})

st.subheader("Recommended tracks")
st.dataframe(show, width="stretch", height=520)

seen = uihelpers.get_seen_track_uris()
if seen:
    st.caption(f"Already-seen tracks excluded: {len(seen):,}")
