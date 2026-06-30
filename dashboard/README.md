## 📊 Dashboard Preview

The analytics layer transforms curated datasets into interactive Power BI dashboards designed to support risk visibility, action tracking and operational decision-making.

The solution combines historical analysis, risk classification and action plan governance, providing stakeholders with a centralized view of operational performance and mitigation activities.

### Key Capabilities

- Risk monitoring and classification
- Action plan governance
- Historical trend analysis
- Risk lifecycle monitoring
- Operational performance tracking
- Executive decision support
- Interactive filtering and drill-down analysis

---

### Executive Overview

A consolidated view of operational risks by plant, segment and area, allowing users to identify critical concentrations, compare risk levels and monitor historical trends.

<p align="center">
  <img src="executive_overview.png" width="1000">
</p>

---

### Action Plan Management

Dedicated monitoring of corrective and preventive actions, including status tracking, overdue actions, responsibilities and completion progress.

<p align="center">
  <img src="action_plan_management.png" width="1000">
</p>

---

### Risk Group Analysis

Detailed analysis of specific risk groups, supporting investigation of risk classifications, historical evolution and operational records.

<p align="center">
  <img src="risk_group_analysis.png" width="1000">
</p>

---

### Risk Lifecycle Monitoring

End-to-end monitoring of risk status transitions, including open, in-progress, completed and cancelled records. This view helps evaluate mitigation effectiveness and identify operational trends over time.

<p align="center">
  <img src="risk_lifecycle_monitoring.png" width="1000">
</p>

---

### End-to-End Analytics Flow

```text
Data Sources
├─ Historical CSV Files
├─ Action Plan Records
└─ External APIs
          │
          ▼
Ingestion Layer
├─ historical/
└─ api/
          │
          ▼
Integration Layer
└─ action_plan/
          │
          ▼
Core Processing
└─ sheq_complete/
          │
          ▼
Analytics Layer
├─ sheq_monthly/
└─ sheq_status/
          │
          ▼
Structured Delta Tables
          │
          ▼
Power BI Dashboards
          │
          ▼
Business Insights & Decision Support
```

---

## 📥 Power BI Report

The Power BI report used in this project is available for download.

### Download

📄 SHEQ_Risk_Analytics.pbix

### Requirements

- Power BI Desktop
- Sample datasets included in this repository

### Notes

- The report uses anonymized and mock data.
- No confidential or proprietary information is included.
- Visuals and measures were created exclusively for demonstration and portfolio purposes.

---
