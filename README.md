![dbt](https://img.shields.io/badge/dbt-core-blue)
![Python](https://img.shields.io/badge/python-3.11-green)
![DuckDB](https://img.shields.io/badge/DuckDB-database-yellow)

# 📦 Project: Commodity Price Trends in Gaza
## Overview

Since October 2023, Gaza has faced severe geopolitical disruption. Humanitarian organisations need reliable data to understand how commodity prices have evolved - but raw data is messy, inconsistent, and hard to analyze. This project builds a clean, reliable data foundation and answers critical questions about price trends, inflationary pressure, and the impact of aid.

Reports can be found in the [analyses](https://github.com/AkbarMoledina/humanitarian_needs_in_palestine/tree/main/analyses) folder: 

- **Commodity Price Analysis** – Analysing price trends
- **Aid Received Analysis** – Understanding aid flows and constraints

## Key Achievements

- Built a robust analytics engineering pipeline using dbt and DuckDB
- Created a clean, reusable star-schema with multiple fact, dimension (SCD Type 1) and bridging tables
- Implemented 40+ dbt tests to ensure data quality throughout the pipeline
- Preparing analysis-ready mart tables to support downstream exploration and storytelling

This repository is structured to reflect modern analytics engineering best practices, separating raw ingestion, staging, core facts/dimensions, and analytics marts.

### 🛠️ Tech Stack

- DuckDB – analytical database
- dbt – data modelling, testing, and documentation
- Python – raw data ingestion
- SQL – transformations and analytics models
- Jupyter Notebook - analysis

# 🗂️ Data Model Overview
## Raw Layer

Contains minimally transformed source data, loaded via Python scripts.

## Staging Layer

Cleans and standardises raw data:

- Normalises commodity names
- Extracts and standardises unit amounts
- Casts data types
- Preserves original values where needed for traceability

Example:

- stg_aid_received
- stg_commodity_prices_gaza

## Core Models (Facts & Dimensions)

### dim_commodity

- One row per unique commodity + unit
- Uses a surrogate key for stability and joins

### dim_crossing

- One row per crossing
- Uses a surrogate key for stability and joins

### dim_date

- One row per date
- Uses date id for stability and joins

### dim_organisation

- One row per organisation
- Uses a surrogate key for stability and joins

### bridge_organisation

- Handles the many-to-many relationship between aid event and organisation
- Contains aid_event_id, org_id and org_role (donor/recipient)

### fct_aid_received

- One row per cargo
- Includes cargo description, category and quantity 

### fct_commodity_prices_gaza

- One row per commodity, unit, and date
- Includes both observed monthly prices and a synthetic baseline price

A synthetic baseline price dated 2023-10-01 is included to support before/after comparisons.

# 📊 Analytics Marts

The marts layer is designed for direct querying and analysis, without additional business logic.

1. Commodity Price Time Series

A thin mart exposing price observations over time. Use cases:

- Trend analysis of commodity prices over time
- Visualisation
- Index construction

2. Price Change from Baseline

Calculates percentage change relative to the October 2023 baseline price. Use cases:

- Measuring inflationary pressure
- Comparing relative price shocks across commodities
- Supporting narrative analysis

3. Aid Events

A simple mart featuring a single aid event per row, with cargo category and crossing details. Use cases:

- Aid flow by cargo category over time
- Border crossing usage over time


# 🧪 Data Quality & Testing Methodology

dbt tests are applied to key models, including:

- Unique and not_null tests on primary columns and surrogate keys
- Relationship tests on all foreign keys
- Accepted value tests for fields that a restricted to certain values or ranges
- Custom tests for price ratios and date consistency
- Referential integrity between fact and dimension tables

## 📁 Project Structure

```
├── analyses/          # Jupyter notebooks for analysis
├── models/            # dbt models (staging, dimensions, facts, marts)
├── scripts/           # Python data ingestion scripts
├── seeds/             # Reference data (crossings, categories)
├── tests/             # Custom singular tests
└── dbt_project.yml    # dbt project configuration
```

# 🚧 Work in Progress / Next Steps

Planned next steps include:

- Aid delivered against commodity prices
- Visualisation layer (Tableau or similar)

# 🎯 Project Goal

This project is intended as a portfolio-grade analytics engineering case study, demonstrating:

- Strong SQL modelling skills
- dbt best practices
- Analytical thinking
- Clear separation between data engineering and analysis layers
