"""
alpha_engine.__main__ — Package execution entrypoint for python -m alpha_engine
"""

import sys
from alpha_engine.cli import main

if __name__ == "__main__":
    sys.exit(main())
