# 📅 SHEQ Monthly Snapshot Pipeline

This module generates a monthly snapshot of the SHEQ dataset, capturing the latest state of each record for every month.

---

## 📌 Overview

This pipeline transforms the consolidated SHEQ dataset into a time-based snapshot table.

It ensures that, for each record:

- Only the most recent entry per month is retained
- Data is structured for time-series analysis

---

## 🏗️ Pipeline Flow
```
Complete Dataset
       ↓
Data Cleaning
       ↓
Date Normalization
       ↓
Snapshot Logic (Latest per Month)
       ↓
Data Enrichment
       ↓
Partitioned Delta Table
```

---

## ⚙️ Key Features

### ✅ Snapshot Logic
- Uses window functions to retain the latest record per month

---

### ✅ Time-Based Partitioning
- Data is partitioned by:
  - `year_ref`
  - `month_ref`

---

### ✅ Data Cleaning
- Removes invalid IDs and timestamps
- Standardizes date formats

---

### ✅ Flexible Load Strategy
- Supports:
  - Full load
  - Incremental load (current month only)

---

### ✅ Analytical Enrichment
- Risk classification (numerical)
- SAF category extraction

---

## 🧠 Processing Highlights

- Robust handling of inconsistent timestamps
- Efficient deduplication using window functions
- Optimized for time-series analytics

---

## 🚀 Output

The pipeline produces a dataset optimized for:

- Monthly reporting
- Trend analysis
- Power BI dashboards

---

## 🔒 Disclaimer

This is a sanitized version of a production pipeline:

- No sensitive data is exposed
- Table and column names are generalized
