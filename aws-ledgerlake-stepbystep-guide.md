# Step-by-Step Build Guide: AWS Project 1 — "LedgerLake"
## Batch ETL + Serverless BI (S3 → Glue → Athena → QuickSight)

A fully detailed, click-by-click walkthrough for the foundation project in the
AWS lineup. Assumes you've already completed the account setup steps (Free
Plan chosen, root MFA, admin IAM user, budget alert, five onboarding tasks) —
if not, do those first.

**Dataset used throughout this guide**: "Give Me Some Credit" (the same one
used elsewhere in your project series) — a single CSV of ~150,000 historical
loan applicants. Swap in a different dataset if you prefer; the steps are the
same regardless of which CSV you use.

---

## 1. What You're Building

```
Raw CSV → S3 (raw/) → Glue Crawler → Glue Data Catalog
                                          │
                                          ▼
                                   Glue ETL Job (PySpark)
                                          │
                          ┌───────────────┴───────────────┐
                          ▼                                ▼
                   S3 (curated/)                    S3 (analytics/)
                   cleaned Parquet                  Gold Parquet
                                                          │
                                                          ▼
                                                     Amazon Athena
                                                   (serverless SQL)
                                                          │
                                                          ▼
                                                    Amazon QuickSight
                                                      (dashboard)
```

---

## 2. Step 1 — Create the S3 Bucket and Folder Structure

> **Why are we doing this?** Amazon S3 is the foundational storage layer of our Data Lake. By creating `raw`, `curated`, and `analytics` folders, we are establishing a "Medallion Architecture" (Bronze, Silver, and Gold layers). This ensures our messy raw data is kept strictly separated from our cleaned, analytics-ready data.

1. Sign in to the AWS Console as your **admin IAM user** (not root).
2. Search **S3** → **Create bucket**.
3. Bucket name: something globally unique, e.g. `ledgerlake-<yourname>-2026`
   (S3 bucket names are globally unique across all AWS accounts, so add a
   personal suffix).
4. Region: pick one close to you and remember it — you'll need to use the
   same region for Glue, Athena, and QuickSight later.
5. Leave **Block all public access** checked (default) — this data doesn't
   need to be public.
6. Click **Create bucket**.
7. Open the bucket, click **Create folder**, and create three folders:
   - `raw/`
   - `curated/`
   - `analytics/`
   (These act as your Bronze/Silver/Gold layers.)

---

## 3. Step 2 — Upload the Raw Dataset

> **Why are we doing this?** We need to get our starting data into the cloud so AWS services can access it. In a real-world scenario, this data might be streamed in or dropped here automatically by another application.

1. Download the "Give Me Some Credit" CSV from Kaggle to your local machine.
2. In the S3 console, navigate into the `raw/` folder.
3. Click **Upload** → **Add files** → select the CSV → **Upload**.
4. Confirm the file now appears at
   `s3://ledgerlake-<yourname>-2026/raw/cs-training.csv` (or whatever the
   filename is).

---

## 4. Step 3 — Create an IAM Role for Glue

> **Why are we doing this?** Security is paramount in AWS. By default, AWS services cannot interact with each other. We must explicitly create an Identity and Access Management (IAM) Role to give AWS Glue the "keys" to securely read from and write to our specific S3 buckets.

1. Search **IAM** → **Roles** → **Create role**.
2. Trusted entity type: **AWS service**.
3. Use case: search for and select **Glue**.
4. On permissions, attach:
   - `AWSGlueServiceRole` (AWS managed policy)
   - `AmazonS3FullAccess` (fine for a personal learning project; in a real
     org you'd scope this to just your bucket)
5. Name it `LedgerLake-GlueRole`, create it.

---

## 5. Step 4 — Crawl the Raw Data into the Glue Data Catalog

> **Why are we doing this?** To transform our data, we need to know its schema (column names and data types). Instead of typing this out manually, a Glue Crawler automatically scans the CSV file, figures out the schema, and registers it in a central metadata repository (the Glue Data Catalog). We use a Custom Classifier here to force the crawler to recognize our CSV headers correctly!

*Note: To ensure Glue reads your CSV headers correctly (instead of naming columns col0, col1), we will create a custom classifier.*

1. Search **AWS Glue** → in the left sidebar, under **Data Catalog**, click **Classifiers** → **Add classifier**.
2. Name it `csv-with-headers`, choose **CSV** as the type, set **Column headings** to **Has headings**, and click **Create**.
3. Now, in the left sidebar, go to **Crawlers** → **Create crawler**.
4. Name: `ledgerlake-raw-crawler`.
5. **Data source**: Add a data source → S3 → browse to
   `s3://ledgerlake-<yourname>-2026/raw/`.
6. **Custom classifiers**: Under the "Choose data sources and classifiers" step, expand Custom classifiers, click **Add**, and select `csv-with-headers`.
7. **IAM role**: select `LedgerLake-GlueRole` created above.
8. **Output**: create a new database (Glue Data Catalog database) called
   `ledgerlake_db`.
9. Leave the schedule as **On demand** (you'll run it manually for now).
10. Review and **Create crawler**.
11. Select the crawler and click **Run**. Wait for status to show
    **Completed** (usually under a minute for a single CSV).
12. Go to **Databases → ledgerlake_db → Tables** — you should see a new table
    (named after your CSV file). Check its schema and confirm it has columns 
    like `age` and `MonthlyIncome` instead of `col0` and `col1`.

---

## 6. Step 5 — Write the Glue ETL Job (PySpark)

> **Why are we doing this?** Raw CSV files are messy and inefficient to query. This PySpark script (running on serverless AWS Glue workers) cleans the data, drops bad rows, resolves data-type mismatches, and converts the data into Parquet. Parquet is a highly compressed, columnar storage format that makes analytics querying drastically faster and cheaper.

1. In Glue, go to **ETL jobs → Jobs → Create job** → choose **Spark script
   editor** (gives you full control over the PySpark code rather than the
   visual builder).
2. Name the job `ledgerlake-clean-and-model`.
3. Set the **IAM role** to `LedgerLake-GlueRole`.
4. Replace the script editor contents with something like this (adjust column
   names to match your actual CSV header):

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, count, isnan

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from the Glue Data Catalog
raw_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="ledgerlake_db",
    table_name="cs_training_csv"  # check the exact table name in your catalog
)

# Resolve mixed types (Choice types) caused by the CSV header row
resolved_dyf = raw_dyf.resolveChoice(choice="cast:double")
raw_df = resolved_dyf.toDF()

# --- Silver: basic cleaning ---
silver_df = raw_df.dropDuplicates() \
    .withColumn("MonthlyIncome",
        when(col("MonthlyIncome").isNull(), 0.0).otherwise(col("MonthlyIncome"))) \
    .withColumn("NumberOfDependents",
        when(col("NumberOfDependents").isNull(), 0.0).otherwise(col("NumberOfDependents"))) \
    .filter(col("age") > 0)  # This drops bad rows and naturally filters out the text header!

silver_df.write.mode("overwrite").parquet(
    "s3://ledgerlake-<yourname>-2026/curated/applicants/"
)

# --- Gold: a simple aggregated view for the dashboard ---
gold_df = silver_df.groupBy("NumberOfDependents") \
    .agg(
        count("*").alias("applicant_count"),
        (count(when(col("SeriousDlqin2yrs") == 1.0, True)) / count("*")).alias("default_rate")
    )

gold_df.write.mode("overwrite").parquet(
    "s3://ledgerlake-<yourname>-2026/analytics/default_rate_by_dependents/"
)

job.commit()
```

5. Under **Job details**, set:
   - **Type**: Spark
   - **Glue version**: latest available
   - **Worker type**: `G.1X`, **Number of workers**: `2` (minimum, keeps cost
     low for a small dataset)
6. **Save**, then **Run**.
7. Watch the run in **Runs** tab — wait for **Succeeded**. If it fails, click
   into the run to see the CloudWatch log output and error message (common
   issues: wrong table name, wrong column names, bucket path typo).

---

## 7. Step 6 — Verify the Output in S3

> **Why are we doing this?** Trust, but verify. We need to physically check our S3 bucket to ensure the Glue job actually succeeded in writing our new optimized `.parquet` files to the `curated` and `analytics` folders before we try to query them.

1. Go back to S3, navigate to `curated/applicants/` — you should see
   `.parquet` part-files.
2. Navigate to `analytics/default_rate_by_dependents/` — same, smaller
   aggregated output.

---

## 8. Step 7 — Crawl the Curated/Gold Output (so Athena can see it)

> **Why are we doing this?** Even though our new Parquet files exist in S3, our querying engine (Athena) doesn't know about them yet. We run a second crawler to discover the schema of these new Parquet files and register them as a new table in our Data Catalog.

1. Back in Glue, create a second crawler, `ledgerlake-gold-crawler`, pointed
   at `s3://ledgerlake-<yourname>-2026/analytics/`, using the same
   `LedgerLake-GlueRole`, outputting into the same `ledgerlake_db` database.
2. Run it. Confirm a new table (named `analytics`) appears in
   `ledgerlake_db`.

---

## 9. Step 8 — Query with Amazon Athena

> **Why are we doing this?** We want to prove our data is clean and ready for business use. Athena allows us to run standard SQL queries directly against the files sitting in S3 without ever provisioning or paying for a database server!

1. Search **Athena** → if this is your first time, it'll ask you to **set a
   query result location** — create or pick an S3 path for this, e.g.
   `s3://ledgerlake-<yourname>-2026/athena-results/`. Set it and save.
   *(COST SAVING TIP: Go back to the S3 console, open your bucket, go to the **Management** tab, and create a **Lifecycle rule**. Apply it to the prefix `athena-results/` and set it to **Expire current versions of objects** after 7 days. This stops query results from piling up and costing you money over time).*
2. In the query editor, select `ledgerlake_db` as the database (left sidebar).
3. Confirm your tables are listed (`cs_training_csv`,
   `analytics`).
4. Run a test query:

```sql
SELECT numberofdependents, applicant_count, default_rate
FROM analytics
ORDER BY default_rate DESC;
```

5. Confirm results return. This is Athena reading Parquet directly from S3,
   serverless, no data movement — no infrastructure to manage.

---

## 10. Step 9 — Sign Up for QuickSight

> **Why are we doing this?** QuickSight is AWS's enterprise Business Intelligence (BI) tool. We need to create an account for it and, more importantly, securely grant it permission to read our Athena results so it can build visualizations.

1. Search **QuickSight** → **Sign up for QuickSight**.
2. Choose **Standard** edition.
3. Set a QuickSight account name and notification email.
4. On the permissions screen, make sure **Amazon S3** and **Amazon Athena**
   are checked. **CRITICAL:** When you click "Select S3 buckets", you MUST check the boxes for BOTH your main `ledgerlake` bucket AND your `athena-results` bucket. For the `athena-results` bucket, you must also check the box in the second column for **"Write permission for Athena Workgroup"** or your dashboard visuals will fail to load!
5. Finish sign-up.

---

## 11. Step 10 — Build the QuickSight Dashboard

> **Why are we doing this?** Raw tables of numbers are hard for executives to interpret. By connecting QuickSight to Athena, we can build interactive, visual dashboards that allow stakeholders to instantly understand credit risk trends and make business decisions.

1. In QuickSight, click **Datasets → New dataset**.
2. Choose **Athena** as the source.
3. Name the data source (e.g. `LedgerLake-Athena`), select the Athena
   workgroup (usually `primary`), **Create data source**.
4. Choose database `ledgerlake_db`, table `analytics`.
5. Choose **Directly query your data** (simplest for this small dataset;
   SPICE import is the alternative for larger/cached data).
6. Click **Visualize** — this opens the analysis canvas.
7. Add a visual: drag `NumberOfDependents` to X-axis, `default_rate` to
   Y-axis, choose a bar chart visual type.
8. Add a second visual with `applicant_count` if you want a volume view
   alongside the risk view.
9. **Publish dashboard** (top right) → name it `LedgerLake Overview` →
   Publish.

---

## 12. Step 11 — Validate End to End

> **Why are we doing this?** We need to ensure that the entire pipeline flows correctly from start to finish. If new data lands in S3, does the dashboard update correctly when refreshed? This proves the architecture is robust.

1. Confirm the QuickSight dashboard renders both visuals with real numbers.
2. Go back to S3 and confirm nothing looks broken (correct folder structure,
   files present in each layer).
3. Re-run the Glue job once more end to end (crawler → job → crawler → Athena
   query → QuickSight refresh) to confirm the whole chain works repeatably,
   not just once.

---

## 13. Step 12 — Clean Up / Cost Hygiene (CRITICAL)

Since this project uses mostly serverless, pay-per-use services, there's
little to actively "turn off," but **QuickSight is an exception**. Follow these steps to ensure you do not receive unexpected bills:

- [ ] **DELETE QUICKSIGHT (Highest Priority):** QuickSight has a subscription fee after the 30-day free trial. If you forget it, you will be billed ~$18-24/month. To cancel: In the QuickSight console, click your user profile icon (top right) → **Manage QuickSight** → **Account Settings** → **Delete Account**. Do this as soon as you are done with the dashboard.
- [ ] **Verify Glue Schedules:** Glue jobs don't run continuously, but confirm there is no schedule accidentally attached to either crawler or the ETL job. They should only run when you manually trigger them.
- [ ] **Check AWS Budgets:** Ensure you have an AWS Budget set up (e.g., a $1 alert) in the Billing Dashboard. If it fires, investigate immediately.
- [ ] **Tear Down Infrastructure:** If you want to fully tear this project down, delete the Glue crawlers, the Glue job, and empty/delete the S3 bucket. S3 storage costs for this project are pennies, but keeping a clean environment is a good habit.

---

## 14. Troubleshooting Notes

- **Crawler finds 0 tables**: double-check the S3 path points to the folder
  containing the CSV, not the CSV file itself, and that the IAM role has S3
  read access to that path.
- **Glue job fails with a column name error**: the crawler infers column
  names from your CSV header exactly as written (case-sensitive) — check the
  actual table schema in the Data Catalog (Tables → click your table →
  Schema tab) and match the script's column references to it exactly.
- **Athena says "table not found"**: confirm you selected the correct
  database in the left sidebar dropdown, and that the Gold crawler actually
  ran successfully after the Glue job wrote its output.
- **QuickSight can't see the Athena table**: re-check the permissions step
  (Step 10.4) — QuickSight needs explicit access granted to the S3 bucket
  Athena is reading from, not just to Athena itself.

---

This guide gets you from an empty AWS account to a working, serverless,
end-to-end batch pipeline: S3 as the data lake, Glue for cataloging and
transformation, Athena for serverless SQL, and QuickSight for the BI layer —
the foundation the rest of the AWS project series builds on.
