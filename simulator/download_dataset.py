"""
simulator/download_dataset.py
Downloads the NASA CMAPSS dataset from the GitHub repository mirror.
"""
import urllib.request
from pathlib import Path

DATA_DIR = Path("data/cmapss")
DATA_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://raw.githubusercontent.com/edwardzjl/CMAPSSData/master/"

FILES = [
    "train_FD001.txt", "test_FD001.txt", "RUL_FD001.txt",
    "train_FD002.txt", "test_FD002.txt", "RUL_FD002.txt",
    "train_FD003.txt", "test_FD003.txt", "RUL_FD003.txt",
    "train_FD004.txt", "test_FD004.txt", "RUL_FD004.txt",
]

print("Downloading NASA CMAPSS dataset files from GitHub mirror...\n")

for f in FILES:
    dest = DATA_DIR / f
    url = BASE_URL + f
    print(f"Downloading {f:<18} ...", end=" ", flush=True)
    try:
        urllib.request.urlretrieve(url, dest)
        size_kb = dest.stat().st_size / 1024
        print(f"OK ({size_kb:.1f} KB)")
    except Exception as e:
        print(f"FAILED: {e}")

print("\nDataset ready in data/cmapss/")
