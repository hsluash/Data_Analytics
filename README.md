# User Data Analytics

## Overview

This document describes the design, assumptions, and operational guidance for a utility that ingests a large **CSV** dataset of user activity (fields: `user_id`, `session_id`, `platform`, `activity_time`, `activity_type`) and returns the **top *N* most active users on each platform** based on **unique session counts**. The output is a structured table (e.g., a DataFrame/DataTable) with columns: `platform`, `user_id`, `session_count`.

> **No source code is included here**. This README focuses on *how* the solution is designed, validated, and operated at scale.

---

## Repository Structure

```
user_data_analytics/
├── data
│   ├── dataset_converter.py      # Converts CSV to Parquet for efficient storage
│   ├── dataset_generator.py      # Generates synthetic/fake datasets
│   ├── dataset_inspector.py      # Main logic to compute top-N active users per platform
│   ├── data_supplier.py          # Simulates real-time data feeds
│   ├── datasets/                 # Stores generated raw CSV datasets
│   ├── dataset_parquets/         # Stores Parquet outputs partitioned by platform
│   │   ├── Dataset_...parquet/
│   │   │   ├── _common_metadata
│   │   │   ├── _metadata
│   │   │   ├── platform=Android/part.0.parquet
│   │   │   ├── platform=iOS/part.0.parquet
│   │   │   └── platform=Web/part.0.parquet
│   └── __init__.py
├── __init__.py
├── __main__.py                   # Entry point (CLI)
├── pyproject.toml                # Project configuration for uv package manager
├── README.md                     # Documentation
├── Result_*.csv                  # Example output result files
---

## Setup Instructions

This project uses **[uv](https://github.com/astral-sh/uv)** as the package manager.

### Clone the repository

```bash
git clone https://github.com/<your-username>/user_data_analytics.git
cd user_data_analytics
```

### Install dependencies with uv

```bash
uv sync
```

This will create a virtual environment and install all dependencies specified in `pyproject.toml`.

### Run the package

```bash
uv run python -m user_data_analytics --help
```
---

## Problem Statement

* Count **unique `session_id`** per `(platform, user_id)`.
* For each `platform`, return the **top `n` users** by `session_count`.
* Handle datasets with **millions of rows** efficiently and robustly.

### Input Schema (CSV)

Required columns:

* `user_id`
* `session_id`
* `platform`
* `activity_time`
* `activity_type`

### Output Schema

* `platform`
* `user_id`
* `session_count`
---

## Design Choices & Rationale

1. **Polars for efficient computation**

   * Faster and more memory-efficient than Pandas.
   * Supports lazy execution for query optimization.

2. **Dask for parallel processing**

   * Enables out-of-core and distributed computation.
   * Scales from a single machine to clusters.

3. **Parquet for columnar storage**

   * Efficient for analytics, supports compression, selective reads, and partitioning.

4. **Clear file roles**

   * `dataset_generator.py` → data generation
   * `dataset_converter.py` → format conversion
   * `dataset_inspector.py` → top-N computation
   * `data_supplier.py` → real-time streaming simulation

---
## Usage Examples

* **Generate a dataset**

  ```bash
  uv run python3 -m user_data_analytics --op generator
  ```

* **Convert CSV → Parquet**

  ```bash
  uv run python -m user_data_analytics --op converter --filepath ./data/datasets/sample.csv --mode parquet
  ```

* **Inspect dataset (top-N)**

  ```bash
  uv run python -m user_data_analytics --op inspector --filepath ./data/datasets/sample.csv --mode csv --n 50
  uv run python -m user_data_analytics --op inspector --filepath ./data/dataset_parquets/sample.parquet --mode parquet --n 50

  ```

* **Simulate streaming feed**

  ```bash
  uv run python -m user_data_analytics --op supplier
  ```

---

## Data Quality & Error Handling

* Schema validation: check required columns.
* Deduplication: drop duplicate `(user_id, session_id, platform)` rows.
* Platform normalization: unify platform strings.
* Error handling: configurable `fail-fast` or `lenient` modes.
* Logging of malformed records.

---

## Performance & Scalability

* Polars provides fast group-by and distinct counts.
* Dask handles parallel/distributed workloads.
* Parquet enables efficient reprocessing and partitioned output.
* Chunked reads and lazy evaluation reduce memory pressure.

---

## Storage Assumptions

* **Raw input**: CSV in `data/datasets/`
* **Intermediate**: Parquet in `data/dataset_parquets/`, partitioned by `platform`
* **Results**: CSV/Parquet output stored at project root or specified via CLI flag
* **Repo hygiene**: Large datasets ignored or tracked with Git LFS (`*.csv filter=lfs diff=lfs merge=lfs -text`)

---
