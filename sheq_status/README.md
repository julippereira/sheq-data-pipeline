# 🔄 SHEQ Status Pipeline (State Machine)

This module tracks the evolution of risk records over time by applying a state-transition logic.

---

## 📌 Overview

This pipeline transforms monthly snapshots into a continuous time series and applies a state machine to classify the evolution of each record.

---

## 🏗️ Pipeline Flow
```
Monthly Snapshot
       ↓
Timeline Completion (Full Month Grid)
       ↓
Forward Fill (Missing Data Handling)
       ↓
Lag Analysis (Previous State)
       ↓
State Machine
       ↓
Final Status Dataset
```
---

## ⚙️ Key Features

### ✅ Time Series Completion
- Generates a full month-by-month timeline for each record

---

### ✅ Forward Fill Logic
- Propagates last known values to ensure continuity

---

### ✅ State Machine

Tracks transitions using:

| Status     | Description |
|------------|------------|
| NEW        | First occurrence or increased risk |
| UNCHANGED  | No change in risk |
| MITIGATED  | Risk reduction |
| DELETED    | Record no longer present |

---

### ✅ Analytical Metrics

- `status_code` → numerical encoding
- `weight` → directional impact (+1 / -1)

---

## 🧠 Processing Highlights

- Use of window functions (`lag`, `last`, `row_number`)
- Time-series reconstruction using `sequence()`
- Business-rule-driven state classification

---

## 🚀 Output

The pipeline produces:

- A complete time-based view of risk evolution
- Dataset ready for trend analysis and KPIs
- Structured input for dashboards (Power BI)

---

## 🔒 Disclaimer

This is a sanitized version of a production pipeline:

- All identifiers are generalized
- No sensitive data is exposed
