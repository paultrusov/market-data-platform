import os
import sys
from pathlib import Path

# The services read their configuration at import time, which is what makes them
# fail fast in a container. Tests supply it before importing.
os.environ.setdefault("PG_DSN", "postgresql://mdp:test@localhost:5432/market")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingest"))
