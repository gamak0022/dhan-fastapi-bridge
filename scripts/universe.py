import json
from functools import lru_cache
from pathlib import Path

UNIVERSE_FILE = Path("data/universe_nse_eq.json")

@lru_cache(maxsize=1)
def load_universe():
    data = json.loads(UNIVERSE_FILE.read_text(encoding="utf-8"))
    return data["universe"]
