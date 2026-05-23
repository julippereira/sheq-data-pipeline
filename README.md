# 🧠 SHEQ Data Platform (End-to-End Data Engineering & Analytics Project)

This project is an end-to-end data platform designed to ingest, transform, integrate, and analyze SHEQ (Safety, Health, Environment & Quality) data from multiple sources.

It combines data engineering and analytics to deliver a complete solution for risk data processing, time-series modeling, and business intelligence, enabling full visibility into risk lifecycle and supporting data-driven decision-making.

---

## 🚀 Overview

The solution processes data from:

- Historical datasets (CSV files)
- External APIs
- Action plan systems

It transforms raw data into structured, analytics-ready datasets using a multi-layer architecture.

---

## 🏗️ Architecture

```
DATA SOURCES
├── CSV (historical data)
├── External APIs
└── CSV (action plans)
    ↓

INGESTION LAYER
├── historical/
└── api/
    ↓

INTEGRATION LAYER
└── action_plan/
    ↓

CORE PROCESSING
└── sheq_complete/
    ↓

ANALYTICS LAYER
├── sheq_monthly/
└── sheq_status/
    ↓

OUTPUT
→ Structured Delta Tables (Ready for BI)
```

---

## 📂 Project Structure

```
sheq-data-pipeline/
│
├── historical/         → Historical CSV ingestion
├── api/                → API data ingestion
├── action_plan/        → Multi-source integration (API + CSV)
├── sheq_complete/      → Unified dataset (core layer)
├── sheq_monthly/       → Monthly snapshot logic
├── sheq_status/        → State machine (risk evolution)
│
├── config/             → Configuration templates (no secrets)
│
├── architecture.png    → High-level diagram
└── README.md           → Project documentation
```

---


---

## ⚙️ Pipeline Modules

### 🔹 1. Historical Pipeline
- Processes CSV-based historical data
- Standardizes schema and cleans inputs

---

### 🔹 2. API Pipeline
- Extracts data from external REST APIs
- Handles pagination and retries
- Transforms semi-structured JSON into structured format

---

### 🔹 3. Action Plan Pipeline
- Integrates API and CSV data sources
- Normalizes schemas
- Applies business rules

---

### 🔹 4. SHEQ Complete Pipeline
- Combines historical and API datasets
- Applies deduplication logic
- Enriches data with action plan information

---

### 🔹 5. Monthly Snapshot Pipeline
- Generates latest state per record per month
- Supports full and incremental loads
- Optimized for time-series reporting

---

### 🔹 6. Status Pipeline (State Machine)
- Builds a complete time series
- Tracks record evolution over time
- Classifies transitions:

| Status     | Description |
|-----------|------------|
| NEW       | First occurrence or increased risk |
| UNCHANGED | No change |
| MITIGATED | Risk reduction |
| DELETED   | Record removed |

---

## 📊 BI & Analytics Layer

A Power BI dashboard was developed on top of the processed datasets to enable interactive analysis and decision-making.

### Key Features

- Risk trend analysis over time
- Monthly evolution tracking
- Status transitions (New, Mitigated, Deleted)
- Plant-level comparison

### 📷 Dashboard Preview

![Dashboard](bi/dashboard_overview.png)

--

## 🧠 Key Features

### ✅ End-to-End Data Pipeline
- Covers the full data lifecycle:
  ingestion → transformation → integration → analytics

---

### ✅ Multi-Source Data Integration
- Combines data from:
  - CSV (historical)
  - REST APIs
  - External action plan systems
- Handles schema inconsistencies across sources

---

### ✅ Robust Data Processing
- Advanced data cleaning and normalization
- Resilient handling of nulls, inconsistent formats, and multilingual inputs
- Dynamic schema alignment using PySpark

---

### ✅ Advanced Transformations
- Window-based deduplication
- Dynamic pivoting and reshaping
- Forward-fill logic for time continuity

---

### ✅ Time-Series Modeling
- Monthly snapshot generation
- Complete timeline reconstruction for each record
- Partitioned datasets optimized for performance

---

### ✅ State Machine Logic
- Tracks evolution of each record over time
- Classifies transitions (NEW, MITIGATED, DELETED, UNCHANGED)
- Enables lifecycle analysis of risk events

---

### ✅ Analytics-Ready Architecture
- Structured datasets designed for BI consumption
- Optimized for Power BI and dashboarding tools
- Supports KPI tracking and trend analysis

---

## 🔐 Security & Configuration

Sensitive data is not stored in the repository.

Configuration template:
[config/api_keys.example.json](config/api_keys.example.json)


To run locally:

1. Create:
`config/api_keys.json`

2. Copy structure from the example file

3. Insert real credentials (do not commit)

---

## 📊 Sample Data

Example datasets are provided:
[action_plan/sample_data.csv](action_plan/sample_data.csv)

All sample data is:

- Anonymized
- Simplified
- Structurally equivalent to real data

---

## 📈 Output

The project generates structured, analytics-ready datasets stored as Delta tables, including:

---

### 🔹 Unified Dataset (sheq_complete)
- Consolidated data from all sources
- Enriched with action plan information
- Standardized schema for consistent analysis

---

### 🔹 Monthly Snapshot (sheq_monthly)
- Latest state of each record per month
- Time-partitioned for efficient queries
- Ideal for time-series reporting

---

### 🔹 Status Tracking Dataset (sheq_status)
- Full timeline of each record
- State transitions (NEW, MITIGATED, DELETED, UNCHANGED)
- Includes analytical metrics such as:
  - status_code
  - weight (impact indicator)

---

### 📊 BI Layer

These datasets power an interactive Power BI dashboard that provides:

- Risk evolution over time
- Monthly trend analysis
- Status transition tracking
- Performance comparison across plants
- Data-driven insights for decision-making

---

### 🎯 Business Value

The final solution enables:

- End-to-end visibility of risk lifecycle
- Monitoring of mitigation effectiveness
- Identification of trends and recurring issues
- Support for strategic and operational decision-making

---

## 🧩 Technologies Used

- **PySpark**
- **Delta Lake**
- **Databricks**
- **REST APIs**
- **Python**
- **Power BI**

---

## 🧠 What This Project Demonstrates

- Real-world data engineering pipelines
- Multi-layer architecture (ingestion → transformation → serving)
- Handling of messy, real-world datasets
- Business logic implementation in data pipelines
- Analytical modeling for risk tracking

---

## 🔒 Disclaimer

This repository contains a **sanitized version** of a real-world project:

- No sensitive data is included
- All names and identifiers are anonymized
- API endpoints and credentials are abstracted

---

## ⭐ Final Notes

This project was designed to simulate a production-grade data pipeline environment, focusing on scalability, maintainability, and data quality.

---

## 👤 Author
Data Analytics Intern

Focus: Data Engineering, Automation & Business Intelligence

## 📬 Contact

Feel free to reach out for questions or discussions about this project.









