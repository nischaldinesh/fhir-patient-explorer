import sys
from pathlib import Path

DAGS = Path(__file__).resolve().parent.parent / "dags"
sys.path.insert(0, str(DAGS))
