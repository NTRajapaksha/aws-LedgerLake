<div align="center">
  <h1>🌊 LedgerLake: AWS Serverless Data Pipeline</h1>
  <p><i>An end-to-end, serverless data engineering architecture for credit risk analysis built entirely on AWS.</i></p>
  
  [![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)](https://aws.amazon.com/)
  [![Apache Spark](https://img.shields.io/badge/apache%20spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
  [![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)](https://www.python.org/)
</div>

---

## 🎥 Project Demo
[Click here to watch the Demo Video](https://lnkd.in/p/gfzgYva9)

---

## 📖 Overview
**LedgerLake** is a fully automated, serverless batch ETL pipeline designed to process messy historical loan applicant data and transform it into an interactive Business Intelligence dashboard. 

This project demonstrates the implementation of a **Medallion Data Architecture (Bronze/Silver/Gold)** using highly optimized, columnar Parquet files, reducing AWS storage and query costs to fractions of a penny.

## 🏗️ Architecture
The pipeline requires zero provisioned infrastructure and scales automatically on demand.

```mermaid
graph LR
    A[Raw CSV Data] -->|Upload| B(Amazon S3<br>Raw/Bronze)
    B -.-> C{AWS Glue Crawler<br>Custom Classifier}
    C -.-> D[(AWS Glue<br>Data Catalog)]
    B -->|Reads Data| E[AWS Glue ETL<br>PySpark Job]
    D -->|Schema Enforcement| E
    E -->|Writes Parquet| F(Amazon S3<br>Curated/Gold)
    F --> G[Amazon Athena<br>Serverless SQL]
    G --> H[Amazon QuickSight<br>BI Dashboard]
```

## 🛠️ Tech Stack & Key Services
* **Amazon S3**: Acts as the central Data Lake, utilizing customized S3 Lifecycle rules to automate the cleanup of temporary Athena query results and control costs.
* **AWS Glue (Data Catalog & Crawlers)**: Automates schema inference. Built with Custom CSV Classifiers to override default inference behaviors and handle edge-case header metadata.
* **AWS Glue (PySpark ETL)**: Distributed data processing. Script utilizes PySpark `resolveChoice` methods to elegantly handle `Struct` data type mismatches caused by messy CSV headers, casting them to Doubles.
* **Amazon Athena**: Serverless interactive querying engine used to validate the Parquet output.
* **Amazon QuickSight**: Enterprise BI layer connected to Athena via strictly scoped IAM Service Roles.

## 🧠 Technical Highlights
* **Schema Evolution Handling**: Handled complex Glue `Choice` types by dynamically casting inferred string headers to `null` floats during the PySpark transformation phase.
* **Cost Optimization**: Replaced default AWS behaviors with cost-saving S3 Lifecycle rules (auto-expiring Athena outputs after 7 days) and utilized minimum required Glue worker nodes (`G.1X`).
* **Strict IAM Security**: Configured isolated IAM roles for Glue execution, and manually scoped QuickSight permissions to only allow `s3:PutObject` on specific Athena-Workgroup buckets, preventing over-privileged service access.

---

## 🚀 Try It Yourself!
Want to build this exact architecture in your own AWS account? 

I have written a highly detailed, click-by-click walkthrough guide that will take you from an empty AWS account to a fully working dashboard, complete with cost-saving warnings and troubleshooting tips.

👉 **[Click here to view the Step-by-Step Guide](aws-ledgerlake-stepbystep-guide.md)** 👈

---

<div align="center">
  <i>Built with ☕ and AWS by [Your Name]</i>
</div>
