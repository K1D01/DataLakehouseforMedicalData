from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, sequence, to_date, col, dayofmonth, month, year, date_format, monotonically_increasing_id
from pyspark.sql.types import DateType
from pyspark.sql.functions import col, to_date, broadcast, monotonically_increasing_id

spark = SparkSession.builder \
    .appName("MIMIC-IV Silver to Gold Medication Usage and Administration") \
    .master("spark://spark-master:7077")\
    .config("spark.executor.memory", "4g")\
    .config("spark.executor.cores", "2")\
    .config("spark.executor.instances", "1")\
    .config("spark.sql.shuffle.partitions", "50")\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# start_date = "2109-01-01"
# end_date = "2215-12-31"

# # Sinh chuỗi ngày
# date_df = spark.sql(f"SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as date_seq") \
#                .withColumn("date", expr("explode(date_seq)")) \
#                .drop("date_seq")

# # Thêm các trường cần thiết
# dim_date = (
#     date_df
#     .withColumn("date_sk", date_format("date", "yyyyMMdd").cast("int"))
#     .withColumn("day", dayofmonth("date"))
#     .withColumn("month", month("date"))
#     .withColumn("year", year("date"))
#     .withColumn("weekday", date_format(col("date"), "E"))
#     .select("date_sk", "date", "day", "month", "year", "weekday")
# )

# # Ghi ra Gold Layer
# dim_date.write.format("delta").mode("ignore").save("s3a://mimic-gold/MedicationUsageAdministration/dim_date")

# # Đọc bảng patients từ Silver
# patients = spark.read.format("delta").load("s3a://mimic-silver/PatientTracking/patients")

# # Sinh surrogate key và chọn cột cần thiết
# dim_patient = (
#     patients
#     .select("subject_id", "gender", "anchor_age", "anchor_year")
#     .dropna(subset=["subject_id"])  # Loại bỏ dòng lỗi nếu có
#     .dropDuplicates(["subject_id"])
#     .withColumn("subject_sk", monotonically_increasing_id())
#     .select("subject_sk", "subject_id", "gender",  "anchor_age", "anchor_year")
# )

# # Ghi ra Gold layer
# dim_patient.write.format("delta").mode("ignore").save("s3a://mimic-gold/MedicationUsageAdministration/dim_patient")

# # Đọc bảng services từ Silver
# services = spark.read.format("delta").load("s3a://mimic-silver/Administration/services")

# # Trích distinct service
# service_dim = (
#     services
#     .select("curr_service")
#     .dropna()
#     .dropDuplicates()
#     .withColumn("service_sk", monotonically_increasing_id())
#     .withColumn("specialty", col("curr_service"))  # Giả định dùng curr_service làm specialty ban đầu
#     .select("service_sk", "curr_service", "specialty")
# )

# # Ghi ra Gold layer
# service_dim.write.format("delta").mode("ignore").save("s3a://mimic-gold/MedicationUsageAdministration/dim_service")

# # Đọc từ bảng emar_detail trong Silver
# emar_detail = spark.read.format("delta").load("s3a://mimic-silver/Medication/emar_detail")

# # Trích distinct thuốc
# dim_medication = (
#     emar_detail
#     .select(
#         col("product_description").alias("drug_name"),
#         col("route"),
#         col("product_unit").alias("form")
#     )
#     .dropna(subset=["drug_name"])      # Bỏ dòng không có tên thuốc
#     .dropDuplicates()
#     .withColumn("drug_sk", monotonically_increasing_id())
#     .withColumn("route", col("route").cast("string")) \
#     .fillna({"route": "unknown"})
#     .select("drug_sk", "drug_name", "route", "form")
# )


# # Ghi ra Gold Layer
# dim_medication.write.format("delta").mode("ignore").save("s3a://mimic-gold/MedicationUsageAdministration/dim_medication")

# Load Silver layer
emar_detail = spark.read.format("delta").load("s3a://mimic-silver/Medication/emar_detail") \
    .select("emar_id", "emar_seq", "route", "product_description", "product_unit", "dose_given", "administration_type")

emar = spark.read.format("delta").load("s3a://mimic-silver/Medication/emar") \
    .select("emar_id", "subject_id", "charttime")

# Join emar_detail + emar → lấy chart_date
emar_joined = emar_detail \
    .join(emar, on="emar_id", how="left") \
    .withColumn("chart_date", to_date("charttime"))

# Load dimension tables (nhỏ → broadcast)
dim_patient = spark.read.format("delta").load("s3a://mimic-gold/MedicationUsageAdministration/dim_patient")
dim_medication = spark.read.format("delta").load("s3a://mimic-gold/MedicationUsageAdministration/dim_medication")
dim_date = spark.read.format("delta").load("s3a://mimic-gold/MedicationUsageAdministration/dim_date")

# Join với dim tables
fact_light = emar_joined \
    .join(broadcast(dim_patient), on="subject_id", how="left") \
    .join(broadcast(dim_medication),
          (emar_joined["product_description"] == dim_medication["drug_name"]) &
          (emar_joined["route"] == dim_medication["route"]) &
          (emar_joined["product_unit"] == dim_medication["form"]),
          how="left") \
    .join(broadcast(dim_date), emar_joined["chart_date"] == dim_date["date"], how="left")

# Select final schema
fact_medication_administration = fact_light.withColumn("med_admin_sk", monotonically_increasing_id()) \
    .select(
        "med_admin_sk",
        "emar_id",
        "emar_seq",
        "subject_sk",
        "drug_sk",
        "date_sk",
        "dose_given",
        col("administration_type").alias("status")
    )




# Ghi ra Gold Layer
fact_medication_administration.write.format("delta").mode("ignore").save("s3a://mimic-gold/MedicationUsageAdministration/fact_medication_administration")

fact_medication_administration.show(5)