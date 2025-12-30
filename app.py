import streamlit as st

import os

from db import databricks_preflight, missing_credentials
from recommender import logic as rlogic
from recommender import ui_helpers as uihelpers


st.set_page_config(page_title="Playlist Recommender — Input", layout="wide")

st.title("Playlist Recommender")
st.caption(
    "Page 1/4 — Provide a seed (track / artist / playlist) and generate recommendations. "
    "Other pages focus on results, relationships, and lightweight metrics."
)

missing = missing_credentials()
if missing:
    st.error("Databricks connection is not configured for this run.")
    st.write("Set these environment variables (or provide them via Streamlit secrets / a `.env` file):")
    st.code("\n".join(missing))
    st.caption(
        "After setting them, restart Streamlit. Example: create a `.env` in the repo root with those keys."
    )
    st.stop()


def _ensure_databricks_reachable() -> bool:
    # Cache the preflight result per session to avoid repeated socket checks,
    # but re-run if the hostname changes.
    host = os.environ.get("DATABRICKS_SERVER_HOSTNAME")
    cached_host = st.session_state.get("db_preflight_host")
    if ("db_preflight_ok" in st.session_state) and (cached_host == host):
        return bool(st.session_state["db_preflight_ok"])

    ok, msg = databricks_preflight(timeout_seconds=3.0)
    st.session_state["db_preflight_ok"] = bool(ok)
    st.session_state["db_preflight_msg"] = msg
    st.session_state["db_preflight_host"] = host
    return bool(ok)

seed_mode = st.radio(
    "Choose a seed type",
    ["Track name", "Artist name", "Playlist name"],
    horizontal=True,
)

col_a, col_b = st.columns([2, 1])
with col_a:
    model = st.selectbox("Model", ["Co-occurrence", "Popularity"], index=0)
with col_b:
    top_k = st.slider("Top-K", min_value=5, max_value=50, value=10)

st.divider()

seed_track_uris = []
playlist_id = None
playlist_name = None

TRACK_MULTISELECT_KEY = "seed_track_choices__track"
ARTIST_MULTISELECT_KEY = "seed_track_choices__artist"
TRACK_SELECT_KEY = "seed_track_choice__single"
ARTIST_TOPN_KEY = "artist_seed_top_n"

if seed_mode == "Track name":
    q = st.text_input("Search track title (partial)")
    if not q:
        st.info("Type a few characters to search tracks.")
    else:
        try:
            if not _ensure_databricks_reachable():
                st.error("Cannot reach Databricks right now.")
                st.write(st.session_state.get("db_preflight_msg", ""))
                st.caption("Check internet/VPN/firewall. You can also test connectivity with `Test-NetConnection <hostname> -Port 443`.")
                matches = None
                raise RuntimeError("Databricks preflight failed")
            with st.spinner("Searching tracks..."):
                matches = rlogic.search_tracks_by_title(q, limit=25)
        except Exception as e:
            if "Databricks preflight failed" not in str(e):
                st.error("Track search failed.")
                st.exception(e)
            matches = None

        if matches is None:
            pass
        elif matches.empty:
            st.warning("No tracks matched that query.")
        else:
            st.caption(f"Found {len(matches):,} matching tracks.")
            options = [
                f"{r['track_title']} — {r['artist_name']} ({r['track_uri']})"
                for _, r in matches.iterrows()
            ]
            st.caption("Pick one seed track (default = top match).")
            if options and not st.session_state.get(TRACK_SELECT_KEY):
                st.session_state[TRACK_SELECT_KEY] = options[0]
            chosen_one = st.selectbox(
                "Seed track",
                options,
                key=TRACK_SELECT_KEY,
            )
            seed_track_uris = [chosen_one.split("(")[-1].rstrip(")")] if chosen_one else []

            with st.expander("Advanced: add more seed tracks", expanded=False):
                if options and not st.session_state.get(TRACK_MULTISELECT_KEY):
                    st.session_state[TRACK_MULTISELECT_KEY] = []
                chosen_extra = st.multiselect(
                    "Optional additional seed tracks",
                    options,
                    key=TRACK_MULTISELECT_KEY,
                )
                extra_uris = [c.split("(")[-1].rstrip(")") for c in chosen_extra]
                seed_track_uris = list(dict.fromkeys(seed_track_uris + extra_uris))

elif seed_mode == "Artist name":
    q = st.text_input("Search artist name (partial)")
    if not q:
        st.info("Type a few characters to search artists.")
    else:
        try:
            if not _ensure_databricks_reachable():
                st.error("Cannot reach Databricks right now.")
                st.write(st.session_state.get("db_preflight_msg", ""))
                st.caption("Check internet/VPN/firewall. You can also test connectivity with `Test-NetConnection <hostname> -Port 443`.")
                df = None
                raise RuntimeError("Databricks preflight failed")
            with st.spinner("Searching artist tracks..."):
                df = rlogic.search_artist_top_tracks(q, limit=40)
        except Exception as e:
            if "Databricks preflight failed" not in str(e):
                st.error("Artist search failed.")
                st.exception(e)
            df = None

        if df is None:
            pass
        elif df.empty:
            st.warning("No tracks found for that artist query.")
        else:
            st.caption(
                "Artist seed uses the artist’s top tracks (by playlist presence) as seeds. "
                "You can override with manual selection below."
            )
            top_n_default = 5
            top_n = st.slider(
                "How many top tracks to use as seeds",
                min_value=1,
                max_value=min(10, int(len(df))),
                value=min(top_n_default, int(len(df))),
                key=ARTIST_TOPN_KEY,
            )

            preview = df[["track_title", "artist_name", "track_uri", "score"]].head(int(top_n)).copy()
            preview = preview.rename(columns={"score": "playlist_count"})
            st.dataframe(preview, width="stretch", height=240)

            seed_track_uris = df["track_uri"].astype(str).head(int(top_n)).tolist()

            with st.expander("Advanced: manually choose seed tracks", expanded=False):
                options = [
                    f"{r['track_title']} — {r['artist_name']} ({r['track_uri']})"
                    for _, r in df.iterrows()
                ]
                chosen = st.multiselect(
                    "Override seed tracks",
                    options,
                    key=ARTIST_MULTISELECT_KEY,
                )
                if chosen:
                    seed_track_uris = [c.split("(")[-1].rstrip(")") for c in chosen]

else:  # Playlist name
    q = st.text_input("Search playlist name (partial)")
    if not q:
        st.info("Type a few characters to search playlists.")
    else:
        try:
            if not _ensure_databricks_reachable():
                st.error("Cannot reach Databricks right now.")
                st.write(st.session_state.get("db_preflight_msg", ""))
                st.caption("Check internet/VPN/firewall. You can also test connectivity with `Test-NetConnection <hostname> -Port 443`.")
                pls = None
                raise RuntimeError("Databricks preflight failed")
            with st.spinner("Searching playlists..."):
                pls = rlogic.search_playlists_by_name(q, limit=25)
        except Exception as e:
            if "Databricks preflight failed" not in str(e):
                st.error("Playlist search failed.")
                st.exception(e)
            pls = None

        if pls is None:
            pass
        elif pls.empty:
            st.warning("No playlists matched that query.")
        else:
            st.caption(f"Found {len(pls):,} matching playlists.")
            pls_options = [
                f"{r['playlist_name']} ({r['playlist_id']})" for _, r in pls.iterrows()
            ]
            chosen_pl = st.selectbox("Choose a playlist", options=pls_options)
            playlist_id = chosen_pl.split("(")[-1].rstrip(")")
            playlist_name = chosen_pl.rsplit("(", 1)[0].strip()

st.divider()

with st.expander("Ready to generate?", expanded=False):
    st.write(
        {
            "seed_mode": seed_mode,
            "model": model,
            "top_k": int(top_k),
            "n_seed_tracks_selected": len(seed_track_uris),
            "playlist_id": playlist_id,
        }
    )

if st.button("Generate recommendations", type="primary"):
    if (not seed_track_uris) and (not playlist_id):
        st.error("Select a seed track (Track/Artist mode) or choose a playlist (Playlist mode) before generating.")
        st.stop()
    uihelpers.save_inputs_to_session(
        seed_track_uris=seed_track_uris,
        playlist_id=playlist_id,
        playlist_name=playlist_name,
        seed_mode=seed_mode,
        model=model,
        top_k=top_k,
    )
    try:
        with st.spinner("Generating recommendations and caching explanation signals..."):
            uihelpers.run_recommender_and_store()
        st.success(
            "Done. Open '2 — Recommendation Results' for the ranked list, then '3 — Explanation / Relationships' to see why items were recommended."
        )

        c1, c2 = st.columns(2)
        with c1:
            st.page_link("pages/2_Recommendation_Results.py", label="Go to Recommendation Results")
        with c2:
            st.page_link("pages/3_Explanation_Relationships.py", label="Go to Explanation / Relationships")
    except Exception as e:
        st.error("Recommendation failed.")
        st.exception(e)

st.caption(
    "No plots or metrics are shown here by design. "
    "This page only captures inputs and generates a session-scoped recommendation run."
)
