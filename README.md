# Playlist Continuation Recommender

Minimal Streamlit app that exposes two simple recommenders (popularity and co-occurrence) using a Databricks SQL Gold star schema.

Usage (local):

1. Set environment variables:

   - `DATABRICKS_SERVER_HOSTNAME`
   - `DATABRICKS_HTTP_PATH`
   - `DATABRICKS_TOKEN`

2. Install requirements: `pip install -r requirements.txt`
3. Run: `streamlit run app.py`

On Streamlit Cloud, add the Databricks credentials as secrets (same names) and deploy the repo.

Multi-page app:

- Open the app and use the sidebar pages: `Recommendations`, `Visualizations`, and `About`.
- `Recommendations` supports search by track title, artist, playlist name, or entering URIs.
- `Visualizations` shows dataset badges, top artists, popularity distribution, and co-occurrence visualizations inspired by the Phase 2 notebook.

