"""
Root-level load data curator — delegates to the main module.
Run this script: python forecasting/load_data_curator.py
Or use the module directly: python -m forecasting.load.data_curator
"""

import os
import sys

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from forecasting.load.data_curator import main

if __name__ == "__main__":
    main()
