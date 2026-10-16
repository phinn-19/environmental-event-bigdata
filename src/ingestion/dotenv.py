"""Minimal local .env loader for two API tokens (standard library only).

The project never writes or prints secrets. Existing process environment wins.
CDS credentials remain in the user's private ~/.cdsapirc, outside the repository.
"""
from pathlib import Path
import os

KEYS = frozenset({'OPENAQ_API_KEY', 'FIRMS_MAP_KEY'})


def load_project_env(root: Path) -> None:
    env_path = root / '.env'
    if not env_path.is_file():
        return
    for line in env_path.read_text(encoding='utf-8-sig').splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        name, value = line.split('=', 1)
        name, value = name.strip(), value.strip()
        if name not in KEYS or not value or os.environ.get(name):
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if value:
            os.environ[name] = value
