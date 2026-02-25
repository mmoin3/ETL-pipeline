import os, sys
import logging

# src/config.py
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Logger settings
LOG_FILE = os.path.join(PROJECT_ROOT, "logs", "etl.log")
LOG_LEVEL = "INFO"

# Example project paths
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
