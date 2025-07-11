from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, dayofmonth, month, year, date_format, when, dayofweek

spark = SparkSession.builder \
    .appName("MIMIC-IV Silver to Gold Diagnosis & Procedure Analytics") \
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

d_icd_diag = spark.read.format("delta").load("s3a://mimic-silver/Billing/d_icd_diagnoses")

dim_diagnosis = d_icd_diag \
    .dropDuplicates(["icd_code", "icd_version"]) \
    .withColumn("diagnosis_sk", monotonically_increasing_id()) \
    .select("diagnosis_sk", "icd_code", "icd_version", "long_title")

dim_diagnosis.write.format("delta") \
    .mode("ignore") \
    .save("s3a://mimic-gold/diagnosis_process/dim_diagnosis")

d_icd_proc = spark.read.format("delta").load("s3a://mimic-silver/Billing/d_icd_procedures")

dim_procedure = d_icd_proc \
    .dropDuplicates(["icd_code", "icd_version"]) \
    .withColumn("procedure_sk", monotonically_increasing_id()) \
    .select("procedure_sk", "icd_code", "icd_version", "long_title")

dim_procedure.write.format("delta").mode("ignore") \
    .save("s3a://mimic-gold/diagnosis_process/dim_procedure")

patients = spark.read.format("delta").load("s3a://mimic-silver/PatientTracking/patients")

dim_diagproc_patient = patients \
    .dropDuplicates(["subject_id"]) \
    .withColumn("patient_sk", monotonically_increasing_id()) \
    .select("patient_sk", "subject_id", "gender", "anchor_age", "anchor_year", "dod")

dim_diagproc_patient.write.format("delta").mode("ignore") \
    .save("s3a://mimic-gold/diagnosis_process/dim_diagproc_patient")


admissions = spark.read.format("delta").load("s3a://mimic-silver/PatientTracking/admissions")

dim_diagproc_admission = admissions \
    .dropDuplicates(["hadm_id"]) \
    .withColumn("admission_sk", monotonically_increasing_id()) \
    .select("admission_sk", "hadm_id", "admittime", "dischtime",
            "admission_type", "insurance", "language", 
            "marital_status", "race",  
            "admission_location", "discharge_location", 
            "admit_provider_id", "hospital_expire_flag")

dim_diagproc_admission.write.format("delta").mode("ignore") \
    .save("s3a://mimic-gold/diagnosis_process/dim_diagproc_admission")




proc = spark.read.format("delta").load("s3a://mimic-silver/Billing/procedures_icd")

dates = proc.select(to_date("chartdate").alias("date")) \
            .dropna().dropDuplicates()

dim_diagproc_date = dates \
    .withColumn("date_sk", monotonically_increasing_id()) \
    .withColumn("day", dayofmonth("date")) \
    .withColumn("month", month("date")) \
    .withColumn("year", year("date")) \
    .withColumn("weekday", date_format("date", "E")) \
    .withColumn("is_weekend", when(dayofweek("date").isin(1, 7), True).otherwise(False))

dim_diagproc_date.write.format("delta").mode("ignore") \
    .save("s3a://mimic-gold/diagnosis_process/dim_diagproc_date")

# Load sources
diagnoses_icd = spark.read.format("delta").load("s3a://mimic-silver/Billing/diagnoses_icd")
dim_diagnosis = spark.read.format("delta").load("s3a://mimic-gold/diagnosis_process/dim_diagnosis")
dim_patient = spark.read.format("delta").load("s3a://mimic-gold/diagnosis_process/dim_diagproc_patient")
dim_admission = spark.read.format("delta").load("s3a://mimic-gold/diagnosis_process/dim_diagproc_admission")

# Build fact_diagnosis
fact_diagnosis = diagnoses_icd \
    .join(dim_diagnosis, ["icd_code", "icd_version"], "left") \
    .join(dim_patient, "subject_id", "left") \
    .join(dim_admission, "hadm_id", "left") \
    .dropna(subset=["diagnosis_sk", "patient_sk", "admission_sk"]) \
    .withColumn("diagnosis_fact_sk", monotonically_increasing_id()) \
    .select("diagnosis_fact_sk", "diagnosis_sk", "patient_sk", "admission_sk", "seq_num")

fact_diagnosis.write.format("delta").mode("ignore") \
    .save("s3a://mimic-gold/diagnosis_process/fact_diagnosis")


# Load sources
procedures_icd = spark.read.format("delta").load("s3a://mimic-silver/Billing/procedures_icd") \
                            .withColumn("chartdate", to_date("chartdate"))
dim_procedure = spark.read.format("delta").load("s3a://mimic-gold/diagnosis_process/dim_procedure")
dim_date = spark.read.format("delta").load("s3a://mimic-gold/diagnosis_process/dim_diagproc_date")

# Build fact_procedure
fact_procedure = procedures_icd \
    .join(dim_procedure, ["icd_code", "icd_version"], "left") \
    .join(dim_patient, "subject_id", "left") \
    .join(dim_admission, "hadm_id", "left") \
    .join(dim_date, procedures_icd["chartdate"] == dim_date["date"], "left") \
    .dropna(subset=["procedure_sk", "patient_sk", "admission_sk", "date_sk"]) \
    .withColumn("procedure_fact_sk", monotonically_increasing_id()) \
    .select("procedure_fact_sk", "procedure_sk", "patient_sk", "admission_sk", "date_sk", "seq_num")

fact_procedure.write.format("delta").mode("ignore") \
    .save("s3a://mimic-gold/diagnosis_process/fact_procedure")

