# Data Warehouse and Analytics Project

## 📌 Project Overview

This project implements a **Data Warehouse** using the **Medallion Architecture (Bronze → Silver → Gold)** on **SQL Server**.
The main focus of the project is to ensure **strong data security**, **clean data processing**, and a **scalable analytics structure**.

---

## 🎯 Purpose of the Project

* Improve **data security** and controlled access.
* Perform **data cleaning**, **data normalization**, and **data enrichment**.
* Build a structured analytics layer that supports reliable reporting.

---

## 🗂️ Data Sources

* **CSV files** imported into the Bronze layer.

---

## 🛠️ Tools & Technologies

* **SQL Server**
* **T-SQL**
* **Local storage for CSV ingestion**

---

## 🏛️ Medallion Architecture

### 🥉 Bronze Layer

**Load:**

* Raw CSV ingestion
* No transformations performed

**Purpose:**

* Store raw data exactly as received
* Maintain a secure “single source of truth”

---

### 🥈 Silver Layer

**Load:**

* Batch processing
* Full Load (**Truncate and Load**)

**Transformations:**

* Data cleaning
* Data normalization
* Data enrichment
* Derived columns

**Purpose:**

* Convert raw data into clean, analysis-ready tables

---

### 🥇 Gold Layer

**Load:**

* None (directly sourced from Silver)

**Transformations:**

* Data integration
* Aggregations
* Business logic applications

**Purpose:**

* Provide final analytical tables for dashboards and reporting

---

## 📐 Data Model

* **Star Schema**

  * Fact tables for measurable data
  * Dimension tables for descriptive attributes

---

## 👤 Author

**Aphiwe Cebo Sfiso Khuzwayo**
Created as part of a personal learning and analytics development journey.

