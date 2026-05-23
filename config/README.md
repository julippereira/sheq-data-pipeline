# 🔐 Configuration & Secrets Management

This folder contains configuration templates used by the data pipelines.

---

## 📌 Overview

To ensure security and flexibility, all sensitive information such as API credentials is stored **outside of the codebase**.

This approach prevents accidental exposure of secrets and follows best practices for production-grade data pipelines.

---

## 📂 Files

### ✅ `api_keys.example.json`

Template file showing the expected structure for API authentication.

Example:

```json
{
  "Plant A": {
    "api_key": "your_api_key_here",
    "tenant_key": "your_tenant_key_here"
  },
  "Plant B": {
    "api_key": "your_api_key_here",
    "tenant_key": "your_tenant_key_here"
  }
}
```

## ⚙️ Setup Instructions

### Create a new file:
[./api_keys.json](./api_keys.json)

### Copy the structure from:
[./api_keys.example.json](./api_keys.example.json)

### Replace placeholder values with real credentials

---

## 🔒 Security Best Practices

- Never commit real credentials to the repository
- Always use .gitignore to exclude sensitive files
- Store secrets securely (e.g., Databricks Volumes, Key Vault, or environment variables)

## Example .gitignore:
[./api_keys.json](./api_keys.json)

---

## 🧠 Design Principle

### This project follows the principle of:
Separation of Configuration from Code

### Benefits:
- Improved security
- Easier environment configuration
- Better maintainability
- Reusable pipelines across environments

---

## ⚠️ Disclaimer
All examples provided in this repository are sanitized and do not contain real credentials or sensitive data.
