from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import plotly.express as px
from shiny import reactive
from shiny.express import input, ui
from shinywidgets import render_widget
from shinyswatch import theme

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent / "data" / "03_daioe_aggregated"

TAXONOMY_OPTIONS = [
    ("SSYK 2012", "ssyk2012"),
    ("SSYK 1996", "ssyk96"),
]

METRIC_OPTIONS: List[Tuple[str, str]] = [
    ("📚 All Applications", "allapps"),
    ("♟️ Abstract strategy games", "stratgames"),
    ("🎮 Real-time video games", "videogames"),
    ("🖼️🔎 Image recognition", "imgrec"),
    ("🧩🖼️ Image comprehension", "imgcompr"),
    ("🖌️🖼️ Image generation", "imggen"),
    ("📖 Reading comprehension", "readcompr"),
    ("✍️🤖 Language modelling", "lngmod"),
    ("🌐🔤 Translation", "translat"),
    ("🗣️🎙️ Speech recognition", "speechrec"),
    ("🧠✨ Generative AI", "genai"),
]

WEIGHTING_OPTIONS = [
    ("Employment weighted", "emp_weighted"),
    ("Simple average", "simple_avg"),
]

LEVEL_OPTIONS = [
    ("Level 4 (4-digit)", 4),
    ("Level 3 (3-digit)", 3),
    ("Level 2 (2-digit)", 2),
    ("Level 1 (1-digit)", 1),
]


def load_data() -> Dict[str, pd.DataFrame]:
    """Read both weighting versions for each taxonomy into tidy frames."""
    frames: Dict[str, pd.DataFrame] = {}
    for _, taxonomy in TAXONOMY_OPTIONS:
        dfs = []
        for label, suffix in WEIGHTING_OPTIONS:
            path = DATA_DIR / f"daioe_{taxonomy}_{suffix}.csv"
            if not path.exists():
                continue
            df = pd.read_csv(path)
            df["weighting"] = suffix
            df["weighting_label"] = label
            dfs.append(df)
        if dfs:
            frames[taxonomy] = pd.concat(dfs, ignore_index=True)
    if not frames:
        raise FileNotFoundError(
            "No aggregated DAIOE datasets found. Run main.py to regenerate them."
        )
    return frames


DATA = load_data()

ALL_YEARS = sorted(
    {int(year) for frame in DATA.values() for year in frame["year"].unique()}
)
GLOBAL_YEAR_MIN = ALL_YEARS[0]
GLOBAL_YEAR_MAX = ALL_YEARS[-1]
DEFAULT_YEAR_START = max(GLOBAL_YEAR_MIN, GLOBAL_YEAR_MAX - 8)


def metric_mapping() -> Dict[str, str]:
    return {value: label for label, value in METRIC_OPTIONS}


def weighting_mapping() -> Dict[str, str]:
    return {value: label for label, value in WEIGHTING_OPTIONS}


def taxonomy_mapping() -> Dict[str, str]:
    return {value: label for label, value in TAXONOMY_OPTIONS}


# ---------------------------------------------------------------------------
# Sidebar UI
# ---------------------------------------------------------------------------
with ui.sidebar(open="open"):
    ui.input_select(
        "taxonomy",
        "Taxonomy",
        taxonomy_mapping(),
        selected=TAXONOMY_OPTIONS[0][1],
    )
    ui.input_select(
        "level",
        "Level",
        {str(value): label for label, value in LEVEL_OPTIONS},
        selected=str(LEVEL_OPTIONS[0][1]),
    )
    ui.input_select(
        "metric",
        "Sub-index",
        metric_mapping(),
        selected=METRIC_OPTIONS[0][1],
    )
    ui.input_select(
        "weighting",
        "Weighting",
        weighting_mapping(),
        selected=WEIGHTING_OPTIONS[0][1],
    )

    ui.input_slider(
        "year_range",
        "Year range",
        min=GLOBAL_YEAR_MIN,
        max=GLOBAL_YEAR_MAX,
        value=(DEFAULT_YEAR_START, GLOBAL_YEAR_MAX),
        step=1,
        sep="",
    )

    ui.input_slider(
        "top_n",
        "Occupations to display (0 = all)",
        min=0,
        max=30,
        value=10,
        step=1,
    )

    ui.input_switch("sort_desc", "Sort descending", value=False)
    ui.input_text(
        "search", "Search occupation (Swedish)", placeholder="e.g. statistiker"
    )


ui.page_opts(
    title="DAIOE Occupation Explorer",
    fillable=True,
    full_width=True,
    theme=theme.flatly,
)

with ui.card(full_screen=True):
    ui.card_header("Trend by occupation")

    @render_widget
    def trend_plot():
        df = filtered_data()
        if df.empty:
            return px.line()
        metric_col = metric_name()
        ascending = not input.sort_desc()
        latest_year = df["year"].max()
        order = (
            df[df["year"] == latest_year]
            .sort_values(metric_col, ascending=ascending)["label"]
            .tolist()
        )
        fig = px.line(
            df,
            x="year",
            y=metric_col,
            color="label",
            markers=True,
            category_orders={"label": order},
            labels={
                "label": "Occupation",
                "year": "Year",
                metric_col: metric_label(),
            },
        )
        fig.update_layout(hovermode="x unified")
        return fig


with ui.card(full_screen=True):
    ui.card_header("Latest year comparison")

    @render_widget
    def bar_plot():
        df = filtered_data()
        if df.empty:
            return px.bar()
        metric_col = metric_name()
        ascending = not input.sort_desc()
        latest = df["year"].max()
        latest_df = df[df["year"] == latest].sort_values(
            metric_col, ascending=ascending
        )
        order = latest_df["label"].tolist()
        fig = px.bar(
            latest_df,
            x=metric_col,
            y="label",
            orientation="h",
            category_orders={"label": order},
            labels={"label": "Occupation", metric_col: metric_label()},
        )
        return fig


# ---------------------------------------------------------------------------
# Reactive helpers
# ---------------------------------------------------------------------------
@reactive.Calc
def current_data() -> pd.DataFrame:
    taxonomy = input.taxonomy()
    if taxonomy not in DATA:
        return pd.DataFrame()
    return DATA[taxonomy].copy()


@reactive.Calc
def metric_name() -> str:
    return f"daioe_{input.metric()}"


@reactive.Calc
def metric_label() -> str:
    return metric_mapping()[input.metric()]


@reactive.Calc
def filtered_data() -> pd.DataFrame:
    df = current_data()
    if df.empty:
        return df

    metric_col = metric_name()
    level = int(input.level())
    weight = input.weighting()
    df = df[(df["weighting"] == weight) & (df["level"] == level)].copy()
    df = df.dropna(subset=[metric_col])

    year_min, year_max = input.year_range()
    df = df[(df["year"] >= year_min) & (df["year"] <= year_max)]

    search_term = input.search().strip().lower()
    if search_term:
        df = df[df["label"].str.lower().str.contains(search_term, na=False)]

    # Determine which occupations to show based on latest year values
    if not df.empty:
        latest_year = df["year"].max()
        latest_slice = df[df["year"] == latest_year].sort_values(
            metric_col,
            ascending=not input.sort_desc(),
        )
        top_n = input.top_n()
        if top_n > 0:
            keep_codes = latest_slice.head(top_n)["code"].tolist()
        else:
            keep_codes = latest_slice["code"].tolist()
        df = df[df["code"].isin(keep_codes)]

    return df


if __name__ == "__main__":
    ui.run()
