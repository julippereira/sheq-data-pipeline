# 🌐 API Data Pipeline

This module ingests and processes data from a REST API into a structured Delta Lake table.

---

## 📌 Overview

The pipeline performs:

- API extraction with pagination
- Retry logic for resilience
- Recursive parsing of nested JSON
- Data normalization and transformation
- Pivoting dynamic fields into structured format

---

## 🏗️ Pipeline Flow
```
REST API
   ↓
Data Extraction (Pagination + Retry)
   ↓
JSON Parsing (Recursive)
   ↓
Transformation (PySpark)
   ↓
Delta Table
```
---

## ⚙️ Features

- Robust HTTP session with retries
- Dynamic schema handling
- Safe column selection
- Data standardization

---

## 🔐 Security

Credentials are stored externally and not included in this repository.

See: config/api_keys.example.json

---

## 🚀 Output

- Cleaned dataset ready for analytics
- Structured schema from semi-structured API
