# 🧩 SHEQ Complete Data Pipeline (Unified Layer)

This module consolidates multiple data sources into a single, analytics-ready dataset.

---

## 📌 Overview

This pipeline integrates:

- Historical data (CSV ingestion)
- API data (real-time ingestion)
- Action Plan data (multi-source integration)

---

## 🏗️ Pipeline Flow
```
Historical Data      API Data       Action Plan
       ↓                ↓                ↓
        Data Unification (unionByName)
                     ↓
           Data Cleaning & Filtering
                     ↓
             Deduplication Logic
                     ↓
           Business Rules Enrichment
                     ↓
             Join with Action Plan
                     ↓
          Final Dataset (Gold Layer)
```
---

## ⚙️ Key Features

### ✅ Multi-Source Integration
- Combines datasets from independent pipelines

---

### ✅ Schema Flexibility
- Handles evolving schemas safely

---

### ✅ Data Deduplication
- Uses window functions for consistent record selection

---

### ✅ Business Rule Application
- Risk classification consolidation
- ID generation and standardization

---

### ✅ Analytical Enrichment
- Integrates action plan information
- Creates time-based reference columns

---

## 🧠 Processing Highlights

- Robust handling of inconsistent date formats
- Safe joins using normalized keys
- Scalable data transformations in PySpark

---

## 🚀 Output

The pipeline produces a unified dataset:

- Ready for BI tools (Power BI, dashboards)
- Structured for analytics and reporting
- Optimized for downstream consumption

---

## 🔒 Disclaimer

This module is a sanitized version of a production-grade pipeline.

- All identifiers are generalized
- No sensitive data is included
- Data sources are abstracted
