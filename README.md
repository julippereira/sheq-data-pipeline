# 🧠 SHEQ Data Pipeline (End-to-End Data Engineering Project)

This project is an end-to-end data pipeline designed to ingest, transform, integrate, and analyze SHEQ (Safety, Health, Environment & Quality) data across multiple sources.

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

## 🧠 Key Features

### ✅ Multi-Source Ingestion
- Handles API and CSV data integration

### ✅ Data Standardization
- Normalizes inconsistent schemas
- Handles multilingual inputs

### ✅ Resilient Processing
- Safe date parsing
- Handling of nulls and inconsistent values

### ✅ Advanced Transformations
- Dynamic pivoting
- Deduplication with window functions
- Forward-fill logic

### ✅ Time-Series Modeling
- Monthly snapshots
- Full timeline reconstruction

### ✅ State Machine Logic
- Tracks risk evolution over time
- Enables advanced analytics

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

The project generates structured Delta tables ready for:

- Power BI dashboards
- Risk analysis
- Time-series monitoring
- KPI reporting

---

## 🧩 Technologies Used

- **PySpark**
- **Delta Lake**
- **Databricks**
- **REST APIs**
- **Python**

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

## 📬 Contact

Feel free to reach out for questions or discussions about this project.









