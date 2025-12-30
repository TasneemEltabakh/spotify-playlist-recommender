import streamlit as st
import pandas as pd
import altair as alt

from db import missing_credentials
from recommender import logic as rlogic
from recommender import ui_helpers as uihelpers
from recommender import viz


st.set_page_config(page_title="Explanation / Relationships", layout="wide")

st.title("Explanation / Relationships")
st.caption("Page 3/4 — Relationship-based evidence for why items are recommended.")

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

# For explanation visuals, we intentionally keep the seed set small so the visuals remain readable.
explain_seed_uris = uihelpers.get_explain_seed_track_uris()

if not explain_seed_uris and seed_track_uris:
    explain_seed_uris = seed_track_uris[:8]

if not explain_seed_uris:
    st.warning("No seed tracks available to explain relationships. Try using Track or Artist seed input, or a playlist with at least one track.")
    st.stop()

# Candidates to explain (limit to keep charts readable)
max_candidates = 20
cand_uris = recs["track_uri"].astype(str).head(max_candidates).tolist()

seed_meta = rlogic.fetch_tracks_metadata(explain_seed_uris)
cand_meta = rlogic.fetch_tracks_metadata(cand_uris)

seed_label = {
    str(r["track_uri"]): f"{r.get('track_title','')} — {r.get('artist_name','')}"
    for _, r in seed_meta.iterrows()
}
cand_label = {
    str(r["track_uri"]): f"{r.get('track_title','')} — {r.get('artist_name','')}"
    for _, r in cand_meta.iterrows()
}

# Base relationship signal: shared playlist counts between each seed track and each recommended track.
edges_raw = rlogic.seed_candidate_cooccurrence(explain_seed_uris, cand_uris)
if edges_raw is None:
    edges_raw = pd.DataFrame()

if not edges_raw.empty:
    edges_raw = edges_raw.rename(columns={"seed_track_uri": "seed_uri", "candidate_track_uri": "cand_uri", "shared_playlists": "shared"})
else:
    edges_raw = pd.DataFrame(columns=["seed_uri", "cand_uri", "shared"])

# Build a dense matrix (fill missing pairs with 0)
all_pairs = pd.MultiIndex.from_product([explain_seed_uris, cand_uris], names=["seed_uri", "cand_uri"]).to_frame(index=False)
mat = all_pairs.merge(edges_raw, on=["seed_uri", "cand_uri"], how="left").fillna({"shared": 0})
mat["shared"] = pd.to_numeric(mat["shared"], errors="coerce").fillna(0).astype(int)
mat["seed"] = mat["seed_uri"].map(seed_label).fillna(mat["seed_uri"])
mat["recommended"] = mat["cand_uri"].map(cand_label).fillna(mat["cand_uri"])

st.subheader("Shared-playlist evidence")
if model_l.startswith("co"):
    st.write(
        "The co-occurrence model ranks tracks by how many playlists contain both the seed set and the candidate track. "
        "Higher counts mean stronger evidence the community curates them together."
    )
else:
    st.write(
        "The popularity model ranks tracks by global playlist appearances. "
        "Relationship visuals below show shared-playlist connections as *context* (not used for ranking)."
    )

bar_df = recs.head(max_candidates).copy()
bar_df["label"] = bar_df["track_title"].astype(str) + " — " + bar_df["artist_name"].astype(str)
bar = (
    alt.Chart(bar_df)
    .mark_bar()
    .encode(
        y=alt.Y("label:N", sort='-x', title=None),
        x=alt.X("score:Q", title=("Shared playlists" if model_l.startswith("co") else "Playlist appearances (global)")),
        tooltip=["track_title", "artist_name", "score"],
    )
    .properties(height=460)
)
st.altair_chart(bar, width="stretch")

st.subheader("Track–Track co-occurrence (heatmap)")
heat = viz.heatmap_rect(
    mat[["seed", "recommended", "shared"]],
    x="seed",
    y="recommended",
    value="shared",
    title="How strongly each recommended track co-occurs with each seed track",
    height=520,
)
if heat is None:
    st.info("No co-occurrence edges found for the selected seeds.")
else:
    st.altair_chart(heat, width="stretch")

st.subheader("Track–Track co-occurrence (network)")
st.caption("Edges represent shared playlists between a seed track and a recommended track.")

nodes = []
for u in explain_seed_uris:
    nodes.append({"id": u, "label": seed_label.get(u, u), "group": "Seed"})
for u in cand_uris:
    nodes.append({"id": u, "label": cand_label.get(u, u), "group": "Recommended"})

edges = edges_raw.copy()
if not edges.empty:
    edges = edges.rename(columns={"seed_uri": "src", "cand_uri": "dst", "shared": "weight"})

fig = viz.network_figure(pd.DataFrame(nodes), edges, title="Seed ↔ Recommended relationship network")
if fig is None:
    hint = viz.network_deps_hint()
    if hint:
        st.info(hint)
    else:
        st.info("Not enough relationship edges to build a network graph.")
else:
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Artist–Artist co-occurrence")
st.caption("Aggregated from the track–track shared-playlist counts above.")

seed_artist = {str(r["track_uri"]): str(r.get("artist_name") or "") for _, r in seed_meta.iterrows()}
cand_artist = {str(r["track_uri"]): str(r.get("artist_name") or "") for _, r in cand_meta.iterrows()}

artist_edges = edges_raw.copy()
artist_edges["seed_artist"] = artist_edges["seed_uri"].map(seed_artist).fillna("")
artist_edges["rec_artist"] = artist_edges["cand_uri"].map(cand_artist).fillna("")
artist_edges = artist_edges[artist_edges["seed_artist"].ne("") & artist_edges["rec_artist"].ne("")]

if artist_edges.empty:
    st.info("No artist-level edges available for the current selection.")
else:
    aa = (
        artist_edges.groupby(["seed_artist", "rec_artist"], as_index=False)["shared"].sum()
        .rename(columns={"shared": "shared_playlists"})
    )
    aa = aa.sort_values("shared_playlists", ascending=False)

    heat2 = viz.heatmap_rect(
        aa.rename(columns={"seed_artist": "Seed artist", "rec_artist": "Recommended artist", "shared_playlists": "Shared playlists"}),
        x="Seed artist",
        y="Recommended artist",
        value="Shared playlists",
        title="Artist–artist co-occurrence (sum of shared playlists across track pairs)",
        height=420,
    )
    if heat2 is not None:
        st.altair_chart(heat2, width="stretch")

st.subheader("One recommendation, explained")

options = [
    f"#{int(r['rank'])} {r['track_title']} — {r['artist_name']}"
    for _, r in recs.head(max_candidates).iterrows()
]
choice = st.selectbox("Pick a recommended track", options=options, index=0)
chosen_rank = int(choice.split()[0].lstrip("#"))
chosen_uri = recs.loc[recs["rank"] == chosen_rank, "track_uri"].iloc[0]

contrib = edges_raw[edges_raw["cand_uri"].astype(str) == str(chosen_uri)].copy()
contrib["seed_track"] = contrib["seed_uri"].map(seed_label).fillna(contrib["seed_uri"])
contrib = contrib.sort_values("shared", ascending=False)

if contrib.empty:
    st.info("No direct shared-playlist evidence found for this item (for the displayed seed subset).")
else:
    st.write("Top contributing seed tracks (shared-playlist counts):")
    st.dataframe(contrib[["seed_track", "shared"]].rename(columns={"shared": "shared playlists"}), width="stretch", height=260)

if seed_mode == "Playlist name" and playlist_id:
    st.caption(
        "Note: for readability, the visuals use only a small subset of tracks from the seed playlist. "
        "The recommendation ranking itself still uses the full playlist seed in the backend query."
    )
