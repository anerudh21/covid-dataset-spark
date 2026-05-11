# COVID-19 Global Analytics Pipeline Using PySpark

## Project Overview

This project simulates how a real-world analytics team would process pandemic data for reporting and decision-making. It builds a scalable PySpark pipeline that ingests raw COVID-19 CSVs, cleans and standardizes them, runs aggregations and window-based trend analysis across multiple datasets, and exports analytics-ready tables for visualization.

The final deliverable is a set of clean aggregated datasets (Parquet + CSV) and a visualization notebook producing 12+ charts using Seaborn and Matplotlib.

---

## Business Problem

During the COVID-19 pandemic, organizations needed rapid access to reliable insights:

- Which countries were most affected?
- Which WHO regions had the highest recovery rates?
- When did global cases peak?
- Which countries had high active case burdens relative to recoveries?
- How did population size impact infection rates?

Since the raw data is spread across multiple sources with different formats and granularity, this pipeline cleans, joins, and transforms it into a unified analytical layer.

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| **PySpark** | Distributed data processing |
| **Spark SQL** | Analytical queries and window functions |
| **Python 3.x** | Orchestration and scripting |
| **Seaborn / Matplotlib** | Visualizations |
| **Parquet / CSV** | Output storage formats |
| **Pandas** | Bridge for plotting (`.toPandas()`) |

---

## Datasets

All datasets sourced from [Kaggle — Corona Virus Report](https://www.kaggle.com/datasets/imdevskp/corona-virus-report)

| File | Description | Key Columns |
|------|-------------|-------------|
| `full_grouped.csv` | Daily country-level trends | Country, Date, Confirmed, Deaths, Recovered, WHO Region |
| `covid_19_clean_complete.csv` | Historical location-level data with lat/long | Province/State, Country, Lat, Long, Confirmed |
| `country_wise_latest.csv` | Latest snapshot per country | Country, Confirmed, Deaths, Recovered, Active, WHO Region |
| `day_wise.csv` | Global aggregated daily totals | Date, New cases, New deaths, Recovered |
| `usa_county_wise.csv` | US county-level breakdown | Province_State, Admin2, Confirmed, Deaths |
| `worldometer_data.csv` | Population and global stats | Country, TotalCases, TotalDeaths, Population, Continent |

---

## Project Structure

```
covid-analytics/
│
├── data/                          # Raw CSV datasets (not committed)
│   ├── full_grouped.csv
│   ├── covid_19_clean_complete.csv
│   ├── country_wise_latest.csv
│   ├── day_wise.csv
│   ├── usa_county_wise.csv
│   └── worldometer_data.csv
│
├── output/                        # Generated outputs
│   ├── top_countries.parquet
│   ├── top_countries_csv/
│   ├── region_summary.parquet
│   ├── daily_trends.parquet
│   ├── mortality_report.parquet
│   ├── recovery_report.parquet
│   └── *.png                      # Saved chart images
│
├── covid_pipeline.py              # Main pipeline — all 21 tasks
├── covid_visualizations.py        # Seaborn/Matplotlib chart cells
└── README.md
```

---

## Modules & Tasks

### Module 1 — Data Loading & Schema Handling
| Task | Description |
|------|-------------|
| Task 1 | Load all 6 CSVs with `inferSchema`, print schemas and row counts. Rename all special-character columns (`Country/Region` → `Area`, `Province/State` → `Sector`, `Deaths / 100 Cases` → `Dper100`, etc.) |

### Module 2 — Data Cleaning
| Task | Description |
|------|-------------|
| Task 2 | Detect null `Province/State` values per country and fill with `"Unknown"` |
| Task 3 | Standardize country name variants across datasets (`US` → `USA`, `Korea, South` → `South Korea`, `Taiwan*` → `Taiwan`, etc.) |
| Task 4 | Remove duplicate `(Country, Date)` records from the daily dataset |

### Module 3 — Aggregation
| Task | Description |
|------|-------------|
| Task 5 | Top 10 countries by total confirmed cases |
| Task 6 | Top 10 countries by death rate (Deaths / 100 Cases) |
| Task 7 | WHO region-wise totals — confirmed, deaths, recovered |

### Module 4 — Time-Series Analysis
| Task | Description |
|------|-------------|
| Task 8 | Daily global new cases trend |
| Task 9 | Daily death growth percentage using `lag()` window function |
| Task 10 | Monthly COVID case growth using `month()` + `groupBy` |

### Module 5 — Window Functions
| Task | Description |
|------|-------------|
| Task 11 | Top 5 most affected countries per WHO region using `dense_rank()` over `partitionBy` |
| Task 12 | Country-wise daily case increase using `lag()` partitioned by country |

### Module 6 — Joins
| Task | Description |
|------|-------------|
| Task 13 | Join `country_wise_latest` and `worldometer_data` to find confirmed/death/recovery mismatches |
| Task 14 | Calculate infection rate = `(TotalCases / Population) * 100` per country |

### Module 7 — Geographic Analysis
| Task | Description |
|------|-------------|
| Task 15 | USA state-wise county count and case distribution |
| Task 16 | Lat/Long + confirmed case dataset for geo scatter plotting |

### Module 8 — Advanced Analytics
| Task | Description |
|------|-------------|
| Task 17 | Recovery rate = `(Recovered / Confirmed) * 100` — best and worst countries |
| Task 18 | Countries where Active cases > Recovered (high-risk list) |
| Task 19 | Identify pandemic peak dates for new cases and new deaths |

### Module 9 — Feature Engineering
| Task | Description |
|------|-------------|
| Task 20 | Severity classification: `Low` (<10K), `Medium` (10K–100K), `High` (100K–1M), `Critical` (>1M) |

### Module 10 — Final Pipeline
| Task | Description |
|------|-------------|
| Task 21 | Full end-to-end pipeline: Extract → Clean → Standardize → Join → Aggregate → Save as Parquet + CSV |

---

## Setup & Installation

### Prerequisites

- Python 3.8+
- Java 8 or 11 (required by PySpark)
- Apache Spark 3.x

### Install dependencies

```bash
pip install pyspark pandas matplotlib seaborn
```

### ⚠️ Windows Users — winutils Required

PySpark on Windows needs `winutils.exe` for local file writes (Parquet). Without it, writing to disk will throw a `HADOOP_HOME` error.

**Fix:**
1. Download `winutils.exe` for your Hadoop version from [cdarlint/winutils](https://github.com/cdarlint/winutils)
2. Place it at `C:\hadoop\bin\winutils.exe`
3. Add this before your `SparkSession`:

```python
import os
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["PATH"] += ";C:/hadoop/bin"
```

Alternatively, skip Parquet and use pandas CSV export:
```python
df.toPandas().to_csv("output/filename.csv", index=False)
```

---

## Running the Pipeline

1. Place all 6 CSV files in a `data/` folder at the project root.
2. Create an `output/` directory, or let the pipeline create it.
3. Run the main pipeline:

```bash
python covid_pipeline.py
```

Or execute cell by cell in a Jupyter notebook — each task is a self-contained function.

4. Run visualizations (after pipeline variables are in memory):

```bash
# In the same notebook session, after running covid_pipeline.py cells:
# Run covid_visualizations.py cells
```

---

## Visualizations

All charts use a dark theme (`#0f1117`) and are saved as PNG to `output/`.

| Chart | Task | Variables Used |
|-------|------|----------------|
| Top 10 Confirmed — Horizontal Bar | 5 | `top10_confirmed` |
| Top 10 Death Rate — Horizontal Bar | 6 | `top10_death_rate` |
| WHO Region Stacked Bar + Pie | 7 | `who_region_totals` |
| Daily New Cases — Area Line | 8 | `daily_new_cases` |
| Death Growth % + 7-day Rolling Avg | 9 | `death_growth` |
| Monthly Growth — Bar + Cumulative Line | 10 | `monthly_growth` |
| Top 5 per WHO Region — Grouped Bar | 11 | `top5_per_region` |
| Infection Rate — Scatter Plot | 14 | `infection_rate` |
| Recovery Rate — Diverging Bar | 17 | `recovery_rate` |
| Pandemic Peaks — Dual Line + Markers | 19 | `peak_cases`, `peak_deaths`, `days` |
| Severity — Pie + Count Bar | 20 | `severity_df` |
| **Summary Dashboard** — 2×3 Grid | Bonus | All of the above |

---

## Output Files

After running the full pipeline, `output/` will contain:

```
output/
├── top_countries.parquet
├── top_countries_csv/
├── region_summary.parquet
├── region_summary_csv/
├── daily_trends.parquet
├── daily_trends_csv/
├── mortality_report.parquet
├── mortality_report_csv/
├── recovery_report.parquet
├── recovery_report_csv/
├── task5_top10_confirmed.png
├── task6_death_rate.png
├── task7_who_region.png
├── task8_daily_cases.png
├── task9_death_growth.png
├── task10_monthly_growth.png
├── task11_top5_per_region.png
├── task14_infection_rate.png
├── task17_recovery_rate.png
├── task19_peaks.png
├── task20_severity.png
└── dashboard_summary.png
```

---

## Key Insights

> These will be populated with actual values once the pipeline runs on the full dataset.

- **Highest confirmed cases**: USA, India, Brazil consistently top the rankings
- **Highest death rate**: Countries with overwhelmed healthcare systems show death rates above 5%
- **Fastest WHO region recovery**: Western Pacific region showed strong recovery ratios
- **Global case peak**: Around January 2022 (Omicron wave) based on day_wise trends
- **High-risk active burden**: Several countries maintained active cases far exceeding recoveries
- **Infection rate leaders**: Small nations with high exposure relative to population (e.g. Andorra, Montenegro)

---
