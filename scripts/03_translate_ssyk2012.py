from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict

import pandas as pd

try:
    ROOT = Path(__file__).resolve().parents[1]
except NameError:  # pragma: no cover - interactive fallback
    ROOT = Path.cwd().resolve()

DATA_DIR = ROOT / "data"
RAW_FILE = DATA_DIR / "01_daioe_raw" / "daioe_ssyk2012.csv"
TRANSLATION_FILE = DATA_DIR / "04_translation_files" / "ssyk2012_en.xlsx"

# column -> (sheet name, digits)
LEVEL_SPECS: dict[str, tuple[str, int]] = {
    "ssyk2012_1": ("1-digit", 1),
    "ssyk2012_2": ("2-digit", 2),
    "ssyk2012_3": ("3-digit", 3),
    "ssyk2012_4": ("4-digit", 4),
}


def load_translation_map(sheet: str, digits: int, path: Path) -> Dict[str, str]:
    df = pd.read_excel(path, sheet_name=sheet, skiprows=3, names=["code", "name"])
    df = df.dropna(subset=["code", "name"])
    df["code"] = df["code"].apply(lambda value: str(int(value)).zfill(digits))
    return dict(zip(df["code"], df["name"]))


def translate_value(value: str, mapping: dict[str, str], digits: int) -> str:
    if pd.isna(value):
        return value

    text = str(value).strip()
    if not text:
        return value

    raw_code = text.split(maxsplit=1)[0]
    normalized_code = raw_code.zfill(digits) if raw_code.isdigit() else raw_code
    english_name = mapping.get(normalized_code) or mapping.get(raw_code.zfill(digits))

    if not english_name:
        return value

    return f"{normalized_code} {english_name}"


def translate_dataframe(df: pd.DataFrame, translation_file: Path) -> pd.DataFrame:
    translations = {
        column: load_translation_map(sheet, digits, translation_file)
        for column, (sheet, digits) in LEVEL_SPECS.items()
    }

    translated = df.copy()
    for column, (sheet, digits) in LEVEL_SPECS.items():
        if column not in translated.columns:
            raise KeyError(f"Expected column '{column}' not found in input file")

        translated[column] = translated[column].apply(
            translate_value, args=(translations[column], digits)
        )

    return translated


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Translate Swedish SSYK2012 labels in daioe_ssyk2012.csv to English "
            "using data/04_translation_files/ssyk2012_en.xlsx."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=RAW_FILE,
        help=f"Path to the SSYK2012 DAIOE file (default: {RAW_FILE})",
    )
    parser.add_argument(
        "--translation-file",
        type=Path,
        default=TRANSLATION_FILE,
        help=f"Path to the translation workbook (default: {TRANSLATION_FILE})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional output path. Defaults to overwriting the input file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path: Path = args.input
    translation_path: Path = args.translation_file
    output_path: Path = args.output or input_path

    df = pd.read_csv(input_path, sep="\t")
    translated = translate_dataframe(df, translation_path)
    translated.to_csv(output_path, sep="\t", index=False)

    print(f"Wrote translated file to {output_path}")


if __name__ == "__main__":  # pragma: no cover
    main()
