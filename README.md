# 🏥 Data Lakehouse for MedicalData with MIMIC-IV

This project implements a full-scale data lakehouse architecture to process, store, and analyze healthcare data using the [MIMIC-IV v3.1](https://physionet.org/content/mimiciv/3.1/) dataset. The system is designed to optimize performance and modularity while enabling SQL-based querying and rich visualization for medical data insights.

---

## 📌 Objectives

- Build a robust data pipeline using open-source tools for healthcare datasets.
- Enable structured, ACID-compliant storage using Delta Lake.
- Provide fast and flexible querying with Trino.
- Support decision-making via interactive dashboards powered by Superset.

---

## 🧱 Architecture Overview

<img width="1531" height="851" alt="kientruchethong" src="https://github.com/user-attachments/assets/6e5e2dec-662a-4dda-88ef-08341e597fba" />

- **MinIO**: Object storage for raw and processed healthcare data.
- **Apache Spark**: Data processing and ETL pipeline.
- **Delta Lake**: ACID-compliant format enabling versioned data lake storage.
- **Hive Metastore**: Centralized metadata and schema management.
- **Trino (PrestoSQL)**: Fast, distributed SQL engine for querying data at scale.
- **Superset**: Open-source BI platform for dashboarding and exploration.

---

## 🧪 Dataset

- **Source**: [MIMIC-IV v3.1](https://physionet.org/content/mimiciv/3.1/)
- **Description**: De-identified electronic health records from ICU patients collected between 2008–2019 at Beth Israel Deaconess Medical Center.
- **Scale**: ~7 million patients, 50+ structured tables.

---

##🎯 Future Improvements
- Integrate Apache Airflow for pipeline orchestration.
- Add CI/CD pipeline via GitHub Actions.
- Support CDC & incremental load.
- Deploy to Kubernetes (for production readiness).
- Add more advanced analytics (e.g., ML integration with PySpark).

---

##📊 Sample Dashboard
<img width="945" height="669" alt="image" src="https://github.com/user-attachments/assets/44087247-4116-493c-8bfd-302cd17ab9ca" />
<img width="945" height="634" alt="image" src="https://github.com/user-attachments/assets/c511ec1d-a7f0-4024-856d-efdeea02171f" />


