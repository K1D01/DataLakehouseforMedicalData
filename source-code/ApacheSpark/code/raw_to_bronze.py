
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MIMIC-IV Raw to Bronze") \
    .master("spark://spark-master:7077")\
    .config("spark.executor.memory", "3g")\
    .config("spark.executor.cores", "2")\
    .config("spark.executor.instances", "1")\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("✅ Spark session created successfully!")

folders = {
    "hosp": [
        "admissions", "d_hcpcs", "d_icd_diagnoses", "d_icd_procedures", "d_labitems",
        "diagnoses_icd", "drgcodes", "emar_detail", "emar", "hcpcsevents", "labevents",
        "microbiologyevents", "omr", "patients", "pharmacy", "poe_detail", "poe",
        "prescriptions", "procedures_icd", "provider", "services", "transfers"
    ],
    "icu": [
        "caregiver","chartevents", "d_items", "datetimeevents", "icustays", "ingredientevents",
        "inputevents", "outputevents", "procedureevents"
    ]
}

for folder, tables in folders.items():
    for table in tables:
        raw_path = f"s3a://mimic-raw/mimic-iv-3.1/{folder}/{table}.csv"
        bronze_path = f"s3a://mimic-bronze/{folder}/{table}"
        print(f"🔄 Processing: {table}")
        df = spark.read.option("header", True).csv(raw_path)
        df.write.format("delta").mode("ignore").save(bronze_path)

print("✅ All raw to bronze conversions completed.")
spark.stop()
