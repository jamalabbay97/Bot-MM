"""
alpha_engine.__main__ — Package execution entrypoint for python -m alpha_engine
"""

import sys
from pathlib import Path

# Auto-bootstrap local .venv site-packages if invoked via system python
_ROOT = Path(__file__).resolve().parent.parent
for _p in (_ROOT / ".venv" / "lib").glob("python*/site-packages"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from alpha_engine.cli import main

if __name__ == "__main__":
    sys.exit(main())
