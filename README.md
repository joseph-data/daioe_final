DAIOE Occupation Explorer
==========================

The DAIOE Occupation Explorer is a Shiny (Python) dashboard that combines DAIOE
sub‑indices with Swedish SCB employment data to highlight how different
occupations stack up against AI capabilities. This repository includes:

* `scripts/01_scbPull_AI.py` – pulls the latest SCB employment weights for a
  selected taxonomy (SSYK 2012 or SSYK 1996).
* `scripts/02_weighting_AI.py` – merges SCB weights with DAIOE raw sub‑indices
  and produces employment‑weighted plus simple‑average aggregates for every SSYK
  level (1–4).
* `main.py` – orchestrates both scripts so you can refresh all datasets with a
  single command.
* `app.py` – Shiny application that visualises the results via interactive
  controls and Plotly charts.

Formulas
--------

For each metric `D` (e.g. “All Applications”), the pipeline aggregates from
4‑digit occupations (`k`) to higher level groups (`g`) using two methods:

* Employment‑weighted mean:

  \[
  D^{(w)}_g = \frac{\sum_{k \in g} E_k D_k}{\sum_{k \in g} E_k}
  \]

  where `E_k` is the SCB employment count for occupation `k`. Missing DAIOE
  scores are excluded from both numerator and denominator.

* Simple average:

  \[
  D^{(s)}_g = \frac{1}{|S_g|} \sum_{k \in S_g} D_k
  \]

  where `S_g` is the set of 4‑digit occupations under group `g` that have
  non‑null DAIOE values.

Percentile ranks are computed per `(year, level)` slice after stacking the
levels so you can compare an occupation’s relative position among its peers.

Refreshing the datasets
-----------------------

1. Ensure the raw DAIOE files (`data/01_daioe_raw/`) are present and the SCB
   weights are absent or outdated.
2. Run `python main.py` to pull the latest SCB data for both taxonomies and to
   rebuild the employment‑weighted/simple‑average tables in
   `data/03_daioe_aggregated/`.
3. (Optional) Limit the refresh to a single taxonomy with
   `python main.py --taxonomy ssyk96`.

Launching the Shiny app
-----------------------

1. Install dependencies (e.g. `pip install -r requirements.txt` or
   `uv pip install`).
2. From the repo root, run `shiny run --reload app.py` (or use `shiny run app.py`
   if hot reload is not needed).
3. Visit the provided URL; the default is `http://127.0.0.1:8000`.

Using the dashboard
-------------------

* **Taxonomy** – choose `SSYK 2012` or `SSYK 1996`.
* **Level** – pick 4‑digit down to 1‑digit groupings to see either granular
  occupations or broader families.
* **Sub‑index** – select which DAIOE metric (All Applications, Generative AI,
  etc.) to display.
* **Weighting** – switch between employment‑weighted vs. simple average views.
* **Year range** – drag the slider to focus on specific years (thousands
  separators are disabled for clarity).
* **Occupations to display** – limit the charts to the top `N` occupations based
  on the latest year’s score; `0` shows all.
* **Sort descending** – toggles the ordering of both the trend legend and the
  bar chart (ascending by default).
* **Search occupation** – filter the dataset by substring (case insensitive).

Visual outputs
--------------

* **Line chart** – shows trends over time for the filtered occupations. The
  legend order matches the latest year ranking and reacts to the sort toggle.
* **Bar chart** – horizontal comparison of the same occupations for the most
  recent year within the selected window.

Prerequisites
-------------

* Python 3.10+
* `pyscbwrapper`, `pandas`, `plotly`, `shiny`, `shinywidgets`, `shinyswatch`
* DAIOE raw files plus SCB's `pyscbwrapper`

Troubleshooting
---------------

* **Missing data warning** – run `python main.py` to regenerate the aggregated
  CSVs.
* **SCB API errors** – confirm your internet connection and that the `pyscbwrapper`
  dependency is installed/configured.
* **Plotly not rendering** – ensure `shinywidgets` is available; the app uses
  `render_widget` instead of `render.plotly`.


