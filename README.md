# 📦 Project: Commodity Price Trends in Gaza
## Overview

This project analyses monthly commodity price data in Gaza to understand how prices have evolved since October 2023, following significant geopolitical disruption.

The focus so far is on:

Building a robust analytics engineering pipeline using dbt and DuckDB

Creating clean, reusable fact and dimension models

Preparing analysis-ready mart tables to support downstream exploration and storytelling

This repository is structured to reflect modern analytics engineering best practices, separating raw ingestion, staging, core facts/dimensions, and analytics marts.

### 🛠️ Tech Stack

DuckDB – analytical database

dbt – data modelling, testing, and documentation

Python – raw data ingestion

SQL – transformations and analytics models

(Jupyter Notebook analysis coming next)

# 🗂️ Data Model Overview
## Raw Layer (raw schema)

Contains minimally transformed source data, loaded via Python scripts.

## Staging Layer (stg schema)

Cleans and standardises raw data:

- Normalises commodity names
- Extracts and standardises unit amounts
- Casts data types
- Preserves original values where needed for traceability

Example:

stg_commodity_prices_gaza

## Core Models (Facts & Dimensions)

### dim_commodity

- One row per unique commodity + unit
- Uses a surrogate key for stability and joins

Columns:

- commodity_id
- commodity_name
- unit_amount

### fct_commodity_prices_gaza

- One row per commodity, unit, and date
- Includes both observed monthly prices and a synthetic baseline price

Columns:

- commodity_name
- unit_amount
- price_date
- price

A synthetic baseline price dated 2023-10-01 is included to support before/after comparisons.

# 📊 Analytics Marts

The marts layer is designed for direct querying and analysis, without additional business logic.

1. Commodity Price Time Series

A thin mart exposing price observations over time.

SELECT
    commodity_name,
    unit_amount,
    price_date,
    price
FROM {{ ref('fct_commodity_prices_gaza') }}


Use cases:

- Trend analysis
- Visualisation
- Index construction

2. Price Change from Baseline

Calculates percentage change relative to the October 2023 baseline price.

WITH baseline AS (
    SELECT
        commodity_name,
        unit_amount,
        price AS baseline_price
    FROM {{ ref('fct_commodity_prices_gaza') }}
    WHERE price_date = '2023-10-01'
),

actual AS (
    SELECT
        commodity_name,
        unit_amount,
        price_date,
        price
    FROM {{ ref('fct_commodity_prices_gaza') }}
    WHERE price_date > '2023-10-01'
)

SELECT
    a.commodity_name,
    a.unit_amount,
    a.price_date,
    a.price,
    b.baseline_price,
    ROUND(
        ((a.price - b.baseline_price) / b.baseline_price) * 100,
        2
    ) AS pct_change_from_baseline
FROM actual a
INNER JOIN baseline b
    ON a.commodity_name = b.commodity_name
   AND a.unit_amount = b.unit_amount


Use cases:

- Measuring inflationary pressure
- Comparing relative price shocks across commodities
- Supporting narrative analysis

# 🔍 Data Quality & Testing

dbt tests are applied to key models, including:

- not_null tests on primary columns
- unique tests on surrogate keys
- Referential integrity between fact and dimension tables

# 🚧 Work in Progress / Next Steps

Planned next steps include:

- Jupyter Notebook analysis and storytelling
- Index-based price normalisation
- Commodity grouping (e.g. staples vs fresh produce)
- Visualisation layer (Tableau or similar)

# 🎯 Project Goal

This project is intended as a portfolio-grade analytics engineering case study, demonstrating:

- Strong SQL modelling skills
- dbt best practices
- Analytical thinking
- Clear separation between data engineering and analysis layers
