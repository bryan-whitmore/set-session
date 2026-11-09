"""Environment loading and session helpers for the split-count demo."""

import pathlib

ROOT = pathlib.Path(__file__).resolve().parent


def load_env(path: pathlib.Path | None = None) -> dict[str, str]:
    """Read set-session/.env into a dict without exporting it process-wide.

    Keeping the secret out of os.environ stops a stray subprocess or traceback
    from surfacing it.

    Args:
        path: Override for the .env location.

    Returns:
        Mapping of key to value, comments and blank lines dropped.
    """
    path = path or (ROOT / ".env")
    if not path.exists():
        raise SystemExit(
            f"{path} not found. Copy .env.example to .env and fill it in "
            "(mode 600; it is gitignored)."
        )
    env: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def require(env: dict[str, str], *keys: str) -> list[str]:
    missing = [k for k in keys if not env.get(k)]
    if missing:
        raise SystemExit(f"missing from .env: {', '.join(missing)}")
    return [env[k] for k in keys]


def connect(env: dict[str, str]):
    """Open a VASTDB tabular-RPC session.

    Args:
        env: Parsed .env mapping.

    Returns:
        A `vastdb.session.Session`.
    """
    import vastdb

    access, secret, endpoint = require(
        env, "VASTDB_ACCESS_KEY", "VASTDB_SECRET_KEY", "VASTDB_ENDPOINT"
    )
    return vastdb.connect(
        access=access,
        secret=secret,
        endpoint=endpoint,
        ssl_verify=env.get("VASTDB_SSL_VERIFY", "true").lower() != "false",
    )


def data_endpoints(env: dict[str, str]) -> list[str] | None:
    """CNode URLs for QueryConfig.data_endpoints, or None for the session URL."""
    raw = env.get("VASTDB_DATA_ENDPOINTS", "")
    urls = [u.strip() for u in raw.split(",") if u.strip()]
    return urls or None
