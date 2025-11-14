# Load required libraries and establish project root directory
import pandas as pd
from pyscbwrapper import SCB
from pathlib import Path

# Resolve project ROOT (parent directory of this script)
ROOT = Path(__file__).resolve().parents[1]

# Select taxonomy (SSYK 2012 or SSYK 1996)
TAX_ID = "ssyk2012"

# Mapping: taxonomy → SCB table parameters
TABLES = {
    "ssyk2012_tab": ("en", "AM", "AM0208", "AM0208E", "YREG51BAS"),
    "ssyk96_tab": ("en", "AM", "AM0208", "AM0208E", "YREG33"),
}

# Initialise SCB connection for selected taxonomy
scb = SCB(*TABLES[f"{TAX_ID}_tab"])

# Fetch available variable metadata from SCB table
var_ = scb.get_variables()

# Extract the first variable (occupation variable) and its values
occupations_key, occupations = next(iter(var_.items()))

# Remove spaces because pyscbwrapper requires clean variable names
clean_key = occupations_key.replace(" ", "")


# -----------------------------
# Determine the latest year
# -----------------------------


# Helper to safely convert year labels to integers
def coerce_year(y):
    try:
        return int(y)
    except Exception:
        return None


# Extract, coerce, and filter valid years
years = [coerce_year(y) for y in var_["year"]]
years = [y for y in years if y is not None]

# Choose the most recent year available in the API
latest_year = str(max(years))


# -----------------------------
# Build and run SCB query
# -----------------------------

# Request all occupations for the latest year
scb.set_query(
    **{
        clean_key: occupations,
        "year": [latest_year],
    }
)

# Retrieve API response
scb_data = scb.get_data()
scb_fetch = scb_data["data"]

# Extract occupation codes used in the query and match to occupation names
codes = scb.get_query()["query"][0]["selection"]["values"]
occ_dict = dict(zip(codes, occupations))


# -----------------------------
# Build raw level-4 DataFrame
# -----------------------------

records = []
for r in scb_fetch:
    # SCB returns key=[code, year]; extract both
    code, year = r["key"][:2]
    name = occ_dict.get(code, code)
    value = r["values"][0]  # employment count (as string)

    records.append({"code_4": code, "occupation": name, "year": year, "value": value})

# Construct DataFrame
df = pd.DataFrame(records)

# Remove unidentified or non-useful occupation code
df = df[df["code_4"] != "0002"].reset_index(drop=True)

# Drop textual occupation label (only codes needed downstream)
df = df.drop(columns="occupation", errors="ignore")

# Standardize code formats and ensure numeric values
df["code_4"] = df["code_4"].astype(str).str.zfill(4)
df["code_3"] = df["code_4"].str[:3]
df["code_2"] = df["code_4"].str[:2]
df["code_1"] = df["code_4"].str[0]
df["value"] = df["value"].astype(int)


# -----------------------------
# Aggregate to all taxonomy levels (1,2,3,4)
# -----------------------------

# Mapping: level → column name
level_map = {4: "code_4", 3: "code_3", 2: "code_2", 1: "code_1"}

level_frames = []
for level, column in level_map.items():
    # Sum values (employment) within each parent group
    level_df = (
        df.groupby(["year", column], as_index=False)["value"]
        .sum()
        .rename(columns={column: "code"})
    )
    level_df["level"] = level
    level_frames.append(level_df)

# Combine all levels 1–4 into unified taxonomy format
df = (
    pd.concat(level_frames, ignore_index=True)
    .assign(taxonomy=TAX_ID)[["taxonomy", "year", "level", "code", "value"]]
    .sort_values(["year", "level", "code"])
    .reset_index(drop=True)
)


# -----------------------------
# Save output CSV
# -----------------------------

# Construct output path and ensure folder exists
out_path = ROOT / "data" / "02_scb_data" / f"{TAX_ID}_en_{latest_year}.csv"
out_path.parent.mkdir(parents=True, exist_ok=True)

# Write final taxonomy table
df.to_csv(out_path, index=False)
print(f"Wrote: {out_path.resolve()}")
