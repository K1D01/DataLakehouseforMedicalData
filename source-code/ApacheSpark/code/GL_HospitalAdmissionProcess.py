from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, month, year, date_format, to_date, row_number, datediff, lead, when
from pyspark.sql.types import DateType
from functools import reduce
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("MIMIC-IV Silver to Gold Hospital Admission Process") \
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


#HospitalAdmissionProcess
#Dim date 
admissions = spark.read.format("delta").load("s3a://mimic-silver/PatientTracking/admissions")
transfers = spark.read.format("delta").load("s3a://mimic-silver/PatientTracking/transfers")
services = spark.read.format("delta").load("s3a://mimic-silver/Administration/services")

tables_and_columns = [
    (admissions, ["admittime", "dischtime"]),
    (transfers, ["intime", "outtime"]),
    (services, ["transfertime"])
]

date_cols = []
for df, columns in tables_and_columns:
    for col_name in columns:
        df_dates = df.select(to_date(col(col_name)).alias("date")).dropna().distinct()
        date_cols.append(df_dates)

all_dates_df = reduce(lambda df1, df2: df1.union(df2), date_cols).distinct()

dim_date = all_dates_df.withColumn("date_sk", date_format("date", "yyyyMMdd").cast("int")) \
    .withColumn("day", dayofmonth("date")) \
    .withColumn("month", month("date")) \
    .withColumn("year", year("date")) \
    .withColumn("weekday", date_format("date", "EEEE"))

dim_date.write.format("delta") \
    .mode("ignore") \
    .save("s3a://mimic-gold/HospitalAdmissionProcess/dim_date")

#Dim patient
patients = spark.read.format("delta").load("s3a://mimic-silver/PatientTracking/patients")

w = Window.orderBy("subject_id")  
dim_patient = patients.select(
    "subject_id", "gender",  "dod", "anchor_age", "anchor_year_group"
).withColumn(
    "patient_sk", row_number().over(w)
)

dim_patient = dim_patient.select(
    "patient_sk", "subject_id", "gender", "dod", "anchor_age", "anchor_year_group"
)

dim_patient.write.format("delta") \
    .mode("ignore") \
    .save("s3a://mimic-gold/HospitalAdmissionProcess/dim_patient")

#Dim service
services = spark.read.format("delta").load("s3a://mimic-silver/Administration/services")

unique_services = services.select("curr_service").dropna().distinct()

w = Window.orderBy("curr_service")
dim_service = unique_services.withColumn("service_sk", row_number().over(w))

dim_service = dim_service.select("service_sk", col("curr_service").alias("service_id"))

dim_service.write.format("delta") \
    .mode("ignore") \
    .save("s3a://mimic-gold/HospitalAdmissionProcess/dim_service")

#Dim diagnosis
diag = spark.read.format("delta").load("s3a://mimic-silver/Billing/diagnoses_icd")
diag_desc = spark.read.format("delta").load("s3a://mimic-silver/Billing/d_icd_diagnoses")

icd_joined = diag.select("icd_code").distinct() \
    .join(diag_desc.select("icd_code", "long_title"), on="icd_code", how="left")

w = Window.orderBy("icd_code")
dim_diagnosis = icd_joined.withColumn("diagnosis_sk", row_number().over(w))

dim_diagnosis = dim_diagnosis.select(
    "diagnosis_sk", "icd_code", col("long_title").alias("icd_description")
)

dim_diagnosis.write.format("delta") \
    .mode("ignore") \
    .save("s3a://mimic-gold/HospitalAdmissionProcess/dim_diagnosis")


#fact admissions
admissions = spark.read.format("delta").load("s3a://mimic-silver/PatientTracking/admissions")
patients = spark.read.format("delta").load("s3a://mimic-silver/PatientTracking/patients")
diagnoses = spark.read.format("delta").load("s3a://mimic-silver/Billing/diagnoses_icd")
services = spark.read.format("delta").load("s3a://mimic-silver/Administration/services")

dim_patient = spark.read.format("delta").load("s3a://mimic-gold/HospitalAdmissionProcess/dim_patient")
dim_service = spark.read.format("delta").load("s3a://mimic-gold/HospitalAdmissionProcess/dim_service")
dim_diagnosis = spark.read.format("delta").load("s3a://mimic-gold/HospitalAdmissionProcess/dim_diagnosis")
dim_date = spark.read.format("delta").load("s3a://mimic-gold/HospitalAdmissionProcess/dim_date")

w = Window.partitionBy("subject_id").orderBy("admittime")
adm_with_lead = admissions.withColumn("next_admit", lead("admittime").over(w)) \
    .withColumn("days_to_next", datediff("next_admit", "dischtime")) \
    .withColumn("readmitted_flag", when(col("days_to_next") <= 30, 1).otherwise(0))

svc_first = services.groupBy("hadm_id").agg({"transfertime": "min"}) \
    .withColumnRenamed("min(transfertime)", "first_transfer_time")

svc_join = services.join(svc_first, on="hadm_id", how="inner") \
    .filter(col("transfertime") == col("first_transfer_time")) \
    .select("hadm_id", "curr_service")

diag_first = diagnoses.groupBy("hadm_id").agg({"icd_code": "first"}).withColumnRenamed("first(icd_code)", "icd_code")

fact_df = adm_with_lead \
    .join(dim_patient, on="subject_id") \
    .join(svc_join, on="hadm_id", how="left") \
    .join(dim_service, dim_service.service_id == svc_join.curr_service, how="left") \
    .join(diag_first, on="hadm_id", how="left") \
    .join(dim_diagnosis, on="icd_code", how="left") \
    .withColumn("length_of_stay", datediff("dischtime", "admittime")) \
    .withColumn("admit_date_sk", date_format(to_date("admittime"), "yyyyMMdd").cast("int")) \
    .withColumn("discharge_date_sk", date_format(to_date("dischtime"), "yyyyMMdd").cast("int"))

w2 = Window.orderBy("hadm_id")
fact_admissions = fact_df.withColumn("fact_admission_id", row_number().over(w2))

fact_admissions = fact_admissions.select(
    "fact_admission_id",
    "hadm_id", col("patient_sk"),
    "service_sk", "diagnosis_sk",
    "admit_date_sk", "discharge_date_sk",
    "admission_type", "hospital_expire_flag",
    "readmitted_flag", "length_of_stay"
).withColumnRenamed("hadm_id", "admission_id")

fact_admissions.write.format("delta") \
    .mode("ignore") \
    .save("s3a://mimic-gold/HospitalAdmissionProcess/fact_admissions")

#fact transfers
transfers = spark.read.format("delta").load("s3a://mimic-silver/PatientTracking/transfers")
dim_patient = spark.read.format("delta").load("s3a://mimic-gold/HospitalAdmissionProcess/dim_patient")

fact_df = transfers.join(dim_patient, on="subject_id", how="left") \
    .withColumn("transfer_date_sk", date_format(to_date("intime"), "yyyyMMdd").cast("int"))

w = Window.orderBy("subject_id", "hadm_id", "intime")
fact_transfers = fact_df.withColumn("fact_transfer_id", row_number().over(w))

fact_transfers = fact_transfers.select(
    "fact_transfer_id",
    col("hadm_id").alias("admission_id"),
    "patient_sk",
    col("careunit").alias("from_location"),
    "transfer_date_sk"
)

fact_transfers.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://mimic-gold/HospitalAdmissionProcess/fact_transfers")
