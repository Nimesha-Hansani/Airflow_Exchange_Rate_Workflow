# Airflow Exchange Rate Workflow

## Overview

The **Airflow Exchange Rate Workflow** is an automated pipeline designed to extract daily exchange rates from an external API, store them in a local SQL Server database, and perform weekly comparisons to detect significant changes. When a significant change is detected, an email alert is triggered to notify users.

## Workflow Details

### 1. **Data Extraction**
- The workflow will **extract daily exchange rate data** from an external API.
- It retrieves the exchange rates for various currencies, such as AUD to LKR and AUD to USD, and stores the data in a **local SQL Server database**.

### 2. **Data Loading**
- The extracted data is **loaded into the SQL Server** database.
- A database table will store the historical exchange rates, allowing comparisons over time.

### 3. **Weekly Comparison**
- Every week, the workflow compares the **current exchange rates** with those from the **previous week**.
- The system checks if the change in the exchange rates exceeds a predefined threshold (e.g., 0.06 for AUD to LKR or AUD to USD).

### 4. **Trigger Email Notification**
- If the comparison finds a **significant change** in the exchange rate, the system will automatically trigger an **email notification** to alert stakeholders.
- The email contains details of the detected changes and is sent via **SMTP** configured within Airflow.
