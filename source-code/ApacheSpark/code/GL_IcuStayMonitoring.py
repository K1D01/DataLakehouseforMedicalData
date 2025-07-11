from pyspark.sql.functions import col, to_date, dayofmonth, month, year, weekofyear, quarter, dayofweek, monotonically_increasing_id
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("MIMIC-IV Silver to Gold Icu Stay Monitoring") \
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


#IcuStayMonitoring

#dim date
# Tạo danh sách các bảng và cột thời gian tương ứng
datetime_sources = [
    ("ICU-PatientTracking/icustays", ["intime", "outtime"]),
    ("ICU-Measurement/chartevents", ["charttime"]),
    ("ICU-Measurement/datetimeevents", ["charttime"]),
    ("ICU-Measurement/inputevents", ["starttime", "endtime"]),
    ("ICU-Measurement/outputevents", ["charttime"]),
    ("ICU-Measurement/procedureevents", ["starttime", "endtime"]),
    ("ICU-Measurement/ingredientevents", ["starttime", "endtime"]),
    ("ICU-Measurement/labevents", ["charttime"]),
    ("ICU-Measurement/microbiologyevents", ["chartdate"]),
    ("ICU-Measurement/omr", ["chartdate"]),
    ("ICU-Caregiver/emar", ["charttime"]),
    ("ICU-Caregiver/emar_detail", ["charttime"]),
]

# Duyệt và union tất cả các ngày
all_dates = None
for rel_path, time_cols in datetime_sources:
    try:
        df = spark.read.format("delta").load(f"s3a://mimic-silver/{rel_path}")
        for time_col in time_cols:
            date_df = df.select(to_date(col(time_col)).alias("date")).dropna().distinct()
            all_dates = date_df if all_dates is None else all_dates.union(date_df)
    except Exception as e:
        print(f"[Warning] Failed to load or parse {rel_path}: {e}")

# Lọc ngày duy nhất
unique_dates = all_dates.dropna().distinct()

# Gán surrogate key và tạo các cột thời gian
dim_date = unique_dates.withColumn("date_sk", monotonically_increasing_id()) \
    .select(
        col("date_sk"),
        col("date"),
        dayofmonth("date").alias("day"),
        month("date").alias("month"),
        year("date").alias("year"),
        weekofyear("date").alias("week"),
        quarter("date").alias("quarter"),
        dayofweek("date").alias("day_of_week")
    )

dim_date.write.format("delta") \
    .mode("ignore") \
    .save("s3a://mimic-gold/IcuStayMonitoring/dim_date")

# dim patient
# Load bảng 'patients' từ Silver Layer
patients_df = spark.read.format("delta").load("s3a://mimic-silver/PatientTracking/patients")

# Chọn cột cần thiết, loại bỏ null và trùng lặp
dim_patient = patients_df.select(
    "subject_id", "gender", "anchor_age", "anchor_year"
).dropna(subset=["subject_id"]).dropDuplicates(["subject_id"])

# Gán surrogate key
dim_patient = dim_patient.withColumn("subject_sk", monotonically_increasing_id())

# Sắp xếp lại thứ tự cột
dim_patient = dim_patient.select(
    "subject_sk", "subject_id", "gender", "anchor_age", "anchor_year"
)

# Ghi ra Gold Layer
dim_patient.write.format("delta").mode("ignore").save("s3a://mimic-gold/IcuStayMonitoring/dim_patient")\

# dim service
# Load bảng services từ Silver Layer
services_df = spark.read.format("delta").load("s3a://mimic-silver/Administration/services")

window_spec = Window.partitionBy("hadm_id").orderBy("transfertime")

first_service_df = services_df.withColumn("row_num", F.row_number().over(window_spec)) \
    .filter(F.col("row_num") == 1) \
    .select("hadm_id", "curr_service") \
    .dropna().dropDuplicates(["hadm_id"])

# Gán surrogate key
dim_service = first_service_df.withColumn("service_sk", monotonically_increasing_id()) \
    .select("service_sk", "hadm_id", "curr_service")

# Ghi ra Gold Layer
dim_service.write.format("delta").mode("ignore").save("s3a://mimic-gold/IcuStayMonitoring/dim_service")

#fact_icu
# Load icustays
icu = spark.read.format("delta").load("s3a://mimic-silver/ICU-PatientTracking/icustays") \
    .select("stay_id", "subject_id", "hadm_id", "intime", "outtime") \
    .withColumn("icu_los_hours", (F.unix_timestamp("outtime") - F.unix_timestamp("intime")) / 3600)

# Load dimension tables
admissions = spark.read.format("delta").load("s3a://mimic-silver/PatientTracking/admissions") \
    .select("hadm_id", "dischtime", "hospital_expire_flag")
dim_patient = spark.read.format("delta").load("s3a://mimic-gold/IcuStayMonitoring/dim_patient")
dim_service = spark.read.format("delta").load("s3a://mimic-gold/IcuStayMonitoring/dim_service")
dim_date = spark.read.format("delta").load("s3a://mimic-gold/IcuStayMonitoring/dim_date")

# Join dimension keys
icu = icu.join(dim_patient.select("subject_id", "subject_sk"), on="subject_id", how="left")
icu = icu.join(dim_service.select("hadm_id", "service_sk"), on="hadm_id", how="left")

# Use alias to avoid ambiguity in date join
dim_date_admit = dim_date.alias("dim_admit")
dim_date_discharge = dim_date.alias("dim_discharge")

icu = icu \
    .join(dim_date_admit.select(F.col("date").alias("admit_date"), F.col("date_sk").alias("admit_date_sk")),
          F.to_date("intime") == F.col("admit_date"), "left") \
    .join(dim_date_discharge.select(F.col("date").alias("discharge_date"), F.col("date_sk").alias("discharge_date_sk")),
          F.to_date("outtime") == F.col("discharge_date"), "left")

# Join admissions for ICU mortality
icu = icu.join(admissions, on="hadm_id", how="left")
icu = icu.withColumn("icu_mortality",
    F.when((F.col("hospital_expire_flag") == 1) &
           (F.abs(F.unix_timestamp("outtime") - F.unix_timestamp("dischtime")) <= 600), 1)
     .otherwise(0))

# Load measurements for HR and SpO2
chartevents = spark.read.format("delta").load("s3a://mimic-silver/ICU-Measurement/chartevents") \
    .select("stay_id", "itemid", "value")

HR_ITEMIDS = [211, 220045]
SPO2_ITEMIDS = [220277, 646]

avg_hr = chartevents.filter(F.col("itemid").isin(HR_ITEMIDS)) \
    .groupBy("stay_id").agg(F.avg(F.col("value").cast("double")).alias("avg_heart_rate"))

avg_spo2 = chartevents.filter(F.col("itemid").isin(SPO2_ITEMIDS)) \
    .groupBy("stay_id").agg(F.avg(F.col("value").cast("double")).alias("avg_spo2"))

icu = icu.join(avg_hr, on="stay_id", how="left")
icu = icu.join(avg_spo2, on="stay_id", how="left")

# Input & Output Events
inputevents = spark.read.format("delta").load("s3a://mimic-silver/ICU-Measurement/inputevents") \
    .select("stay_id", "amount")
total_input = inputevents.groupBy("stay_id").agg(F.sum("amount").alias("total_input_ml"))

outputevents = spark.read.format("delta").load("s3a://mimic-silver/ICU-Measurement/outputevents") \
    .select("stay_id", F.col("value").cast("double").alias("value"))
total_output = outputevents.groupBy("stay_id").agg(F.sum("value").alias("total_output_ml"))

icu = icu.join(total_input, on="stay_id", how="left")
icu = icu.join(total_output, on="stay_id", how="left")

# Procedures
procedureevents = spark.read.format("delta").load("s3a://mimic-silver/ICU-Measurement/procedureevents")
num_procedures = procedureevents.groupBy("stay_id").agg(F.count("*").alias("num_procedures"))

# Medications: count unique hadm_id from prescriptions as proxy for ICU meds
prescriptions = spark.read.format("delta").load("s3a://mimic-silver/Medication/prescriptions").select("hadm_id")
meds_per_stay = icu.select("stay_id", "hadm_id").distinct().join(prescriptions, on="hadm_id", how="inner")
num_medications = meds_per_stay.groupBy("stay_id").agg(F.count("hadm_id").alias("num_medications"))

icu = icu.join(num_procedures, on="stay_id", how="left")
icu = icu.join(num_medications, on="stay_id", how="left")

# Finalize fact table
icu = icu.withColumn("icu_monitoring_sk", F.monotonically_increasing_id())

fact = icu.select(
    "icu_monitoring_sk", "stay_id", "subject_sk", "service_sk",
    "admit_date_sk", "discharge_date_sk", "icu_los_hours",
    "avg_heart_rate", "avg_spo2", "total_input_ml", "total_output_ml",
    "num_procedures", "num_medications", "icu_mortality"
)
fact.write.format("delta").mode("ignore").save("s3a://mimic-gold/IcuStayMonitoring/fact_icu_monitoring")

spark.stop()