import os
import pandas as pd
import socket


try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

try:
    from databricks import sql
except Exception:
    sql = None


if load_dotenv is not None:
    # Allow local development via a .env file. Make the path deterministic
    # so running Streamlit from a different working directory still works.
    base_dir = os.path.dirname(os.path.abspath(__file__))
    root_env = os.path.join(base_dir, ".env")
    frontend_env = os.path.join(base_dir, "Frontend", ".env")

    if os.path.exists(root_env):
        load_dotenv(dotenv_path=root_env, override=False)
    elif os.path.exists(frontend_env):
        load_dotenv(dotenv_path=frontend_env, override=False)


def _get_credential(name: str):
    # Prefer env vars. In Streamlit Cloud, secrets are typically exposed as env vars,
    # but we also support st.secrets when available.
    v = os.environ.get(name)
    if v:
        return v

    try:
        import streamlit as st

        if hasattr(st, "secrets") and name in st.secrets:
            return st.secrets.get(name)
    except Exception:
        pass

    return None


def missing_credentials():
    required = [
        'DATABRICKS_SERVER_HOSTNAME',
        'DATABRICKS_HTTP_PATH',
        'DATABRICKS_TOKEN',
    ]
    return [k for k in required if not _get_credential(k)]


def databricks_preflight(timeout_seconds: float = 3.0):
    """Fast sanity-check that Databricks hostname resolves and port 443 is reachable.

    Returns (ok: bool, message: str).
    """
    missing = missing_credentials()
    if missing:
        return False, f"Missing credentials: {', '.join(missing)}"

    host = _get_credential('DATABRICKS_SERVER_HOSTNAME')
    if not host:
        return False, "Missing DATABRICKS_SERVER_HOSTNAME"

    # DNS resolution
    try:
        socket.getaddrinfo(host, 443)
    except Exception as e:
        return False, f"Hostname does not resolve: {host} ({e})"

    # TCP connectivity
    try:
        sock = socket.create_connection((host, 443), timeout=float(timeout_seconds))
        sock.close()
    except Exception as e:
        return False, f"Cannot reach {host}:443 ({e})"

    return True, ""


def _get_connection_uncached():
    host = _get_credential('DATABRICKS_SERVER_HOSTNAME')
    path = _get_credential('DATABRICKS_HTTP_PATH')
    token = _get_credential('DATABRICKS_TOKEN')
    if not all([host, path, token]):
        missing = missing_credentials()
        hint = ", ".join(missing) if missing else "(unknown)"
        raise RuntimeError(f"Databricks credentials not found. Missing: {hint}")
    if sql is None:
        raise RuntimeError('databricks-sql-connector is not installed.')
    return sql.connect(server_hostname=host, http_path=path, access_token=token)


def _get_connection_cached():
    try:
        import streamlit as st

        @st.cache_resource
        def _cached():
            return _get_connection_uncached()

        return _cached()
    except Exception:
        return _get_connection_uncached()


def get_connection():
    return _get_connection_cached()


def execute_sql(query: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=cols)
    finally:
        # If Streamlit cached the connection, do not close it.
        try:
            import streamlit as st

            # When cached, the connection is reused across reruns.
            # We keep it open and let Streamlit clear it when the cache is cleared.
            if not st.runtime.exists():
                conn.close()
        except Exception:
            conn.close()
