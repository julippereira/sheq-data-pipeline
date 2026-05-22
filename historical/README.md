# 📊 Historical Data Pipeline (CSV → Delta Lake)

This module handles the ingestion and processing of historical data from CSV files into a structured Delta Lake table using PySpark.

---

## 📌 Overview

The historical pipeline is responsible for:

- Reading raw CSV datasets
- Normalizing column names
- Cleaning and standardizing data types
- Applying business rules
- Loading the processed data into a Delta table

---

## 🏗️ Pipeline Flow
```
CSV File
↓
Data Ingestion (PySpark)
↓
Column Normalization
↓
Data Cleaning & Transformation
↓
Business Rules Application
↓
Delta Table (Historical Layer)
```
---

## ⚙️ Key Features

### ✅ Column Normalization
- Removes accents and special characters
- Standardizes naming to snake_case

---

### ✅ Data Cleaning
- Numeric field normalization (handling commas and dots)
- String trimming and normalization

---

### ✅ Date Conversion
- Supports multiple date formats
- Converts values to standard timestamp format

---

### ✅ Business Logic
- Standardization of categorical values
- Data harmonization across regions

---

### ✅ Data Lineage
- Inserts metadata field (`data_source = CSV`)
- Supports traceability of records

---

## 📊 Sample Data

A sample input file is available in:
sample_data.csv

---

## 🔐 Notes

This is a sanitized version of a real-world data pipeline.

- No sensitive data is included
- File paths and table names are generic
- Business logic is simplified for demonstration purposes

---

## 🚀 Output

The pipeline generates a structured table ready for analytics:

- Cleaned and standardized dataset
- Schema aligned for downstream consumption (e.g., BI tools)




