import pandas as pd
import glob
import os
import json
import re
from difflib import SequenceMatcher

# ----------------------------
# Config
# ----------------------------
INPUT_DIR = "./listing_status"
OUTPUT_FILE = "merged_listings.csv"
CONFLICT_LOG = "conflicts.json"

# ----------------------------
# Helpers
# ----------------------------
def normalize_name(name):
    if pd.isna(name):
        return ""
    name = str(name).upper()
    name = re.sub(r"[^A-Z0-9 ]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def name_similarity(a, b):
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def row_diffs(a, b):
    diffs = {}
    for col in a.index:
        av, bv = a[col], b[col]
        if pd.isna(av) and pd.isna(bv):
            continue
        if str(av) != str(bv):
            diffs[col] = [av, bv]
    return diffs


def json_safe(obj):
    if isinstance(obj, float) and pd.isna(obj):
        return None
    if isinstance(obj, dict):
        return {k: json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [json_safe(v) for v in obj]
    return obj


# ----------------------------
# Load files
# ----------------------------
files = sorted(glob.glob(os.path.join(INPUT_DIR, "listing_status_*.csv")))

merged = pd.DataFrame()
conflicts = []

for f in files:
    print(f"Processing {os.path.basename(f)}")
    df = pd.read_csv(f)

    if merged.empty:
        merged = df.copy()
        continue

    for _, incoming in df.iterrows():
        mask = (
                (merged["symbol"] == incoming["symbol"]) &
                (merged["exchange"] == incoming["exchange"])
        )

        if not mask.any():
            merged = pd.concat([merged, incoming.to_frame().T], ignore_index=True)
            continue

        matched = False

        for _, existing in merged[mask].iterrows():
            diffs = row_diffs(existing, incoming)

            # No differences → same
            if not diffs:
                matched = True
                break

            # Only IPO date differs → same
            if set(diffs.keys()) == {"ipoDate"}:
                matched = True
                break

            name_existing = normalize_name(existing["name"])
            name_incoming = normalize_name(incoming["name"])

            if name_similarity(name_existing, name_incoming) >= 0.90:
                matched = True
                break

        if not matched:
            conflicts.append(json_safe({
                "symbol": incoming["symbol"],
                "exchange": incoming["exchange"],
                "source_file": os.path.basename(f),
                "diffs": row_diffs(merged[mask].iloc[0], incoming),
                "existing_rows": merged[mask].to_dict(orient="records"),
                "incoming_row": incoming.to_dict()
            }))

# ----------------------------
# Save outputs
# ----------------------------
merged.to_csv(OUTPUT_FILE, index=False)

with open(CONFLICT_LOG, "w", encoding="utf-8") as fh:
    json.dump(conflicts, fh, indent=2)

print("Done.")
print(f"Merged rows: {len(merged)}")
print(f"Conflicts logged: {len(conflicts)}")
