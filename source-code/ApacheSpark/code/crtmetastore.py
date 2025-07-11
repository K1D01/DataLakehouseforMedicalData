from pyspark.sql import SparkSession
import os

# Init SparkSession có Hive support
spark = SparkSession.builder \
    .appName("Register Gold Delta Tables") \
    .master("spark://spark-master:7077")\
    .config("spark.executor.memory", "3g")\
    .config("spark.executor.cores", "2")\
    .config("spark.executor.instances", "1")\
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

print("✅ Spark session created successfully!")

# Tạo database Hive nếu chưa có
spark.sql("CREATE DATABASE IF NOT EXISTS hospitaladmissionprocess")

spark.sql("""
    CREATE TABLE IF NOT EXISTS hospitaladmissionprocess.dim_date
    USING DELTA
    LOCATION 's3a://mimic-gold/HospitalAdmissionProcess/dim_date'
""")


spark.sql("""
    CREATE TABLE IF NOT EXISTS hospitaladmissionprocess.dim_diagnosis
    USING DELTA
    LOCATION 's3a://mimic-gold/HospitalAdmissionProcess/dim_diagnosis'
""")


spark.sql("""
    CREATE TABLE IF NOT EXISTS hospitaladmissionprocess.dim_patient
    USING DELTA
    LOCATION 's3a://mimic-gold/HospitalAdmissionProcess/dim_patient'
""")


spark.sql("""
    CREATE TABLE IF NOT EXISTS hospitaladmissionprocess.dim_service
    USING DELTA
    LOCATION 's3a://mimic-gold/HospitalAdmissionProcess/dim_service'
""")


spark.sql("""
    CREATE TABLE IF NOT EXISTS hospitaladmissionprocess.fact_admissions
    USING DELTA
    LOCATION 's3a://mimic-gold/HospitalAdmissionProcess/fact_admissions'
""")


spark.sql("""
    CREATE TABLE IF NOT EXISTS hospitaladmissionprocess.fact_transfers
    USING DELTA
    LOCATION 's3a://mimic-gold/HospitalAdmissionProcess/fact_transfers'
""")

# Tạo database Hive nếu chưa có
spark.sql("CREATE DATABASE IF NOT EXISTS icustaymonitoring")

spark.sql("""
    CREATE TABLE IF NOT EXISTS icustaymonitoring.dim_date
    USING DELTA
    LOCATION 's3a://mimic-gold/IcuStayMonitoring/dim_date'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS icustaymonitoring.dim_patient
    USING DELTA
    LOCATION 's3a://mimic-gold/IcuStayMonitoring/dim_patient'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS icustaymonitoring.dim_service
    USING DELTA
    LOCATION 's3a://mimic-gold/IcuStayMonitoring/dim_service'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS icustaymonitoring.fact_icu_monitoring
    USING DELTA
    LOCATION 's3a://mimic-gold/IcuStayMonitoring/fact_icu_monitoring'
""")

##
spark.sql("CREATE DATABASE IF NOT EXISTS DiagnosisProcedureAnalytics")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DiagnosisProcedureAnalytics.dim_diagnosis
    USING DELTA
    LOCATION 's3a://mimic-gold/diagnosis_process/dim_diagnosis'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DiagnosisProcedureAnalytics.dim_diagproc_admission
    USING DELTA
    LOCATION 's3a://mimic-gold/diagnosis_process/dim_diagproc_admission'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DiagnosisProcedureAnalytics.dim_diagproc_date
    USING DELTA
    LOCATION 's3a://mimic-gold/diagnosis_process/dim_diagproc_date'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DiagnosisProcedureAnalytics.dim_diagproc_patient
    USING DELTA
    LOCATION 's3a://mimic-gold/diagnosis_process/dim_diagproc_patient'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DiagnosisProcedureAnalytics.dim_procedure
    USING DELTA
    LOCATION 's3a://mimic-gold/diagnosis_process/dim_procedure'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DiagnosisProcedureAnalytics.fact_diagnosis
    USING DELTA
    LOCATION 's3a://mimic-gold/diagnosis_process/fact_diagnosis'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DiagnosisProcedureAnalytics.fact_procedure
    USING DELTA
    LOCATION 's3a://mimic-gold/diagnosis_process/fact_procedure'
""")

##
spark.sql("CREATE DATABASE IF NOT EXISTS SilverLayer")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.patients
    USING DELTA
    LOCATION 's3a://mimic-silver/PatientTracking/patients'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.admissions
    USING DELTA
    LOCATION 's3a://mimic-silver/PatientTracking/admissions'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.transfers
    USING DELTA
    LOCATION 's3a://mimic-silver/PatientTracking/transfers'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.poe_detail
    USING DELTA
    LOCATION 's3a://mimic-silver/Administration/poe_detail'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.poe
    USING DELTA
    LOCATION 's3a://mimic-silver/Administration/poe'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.services
    USING DELTA
    LOCATION 's3a://mimic-silver/Administration/services'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.d_hcpcs
    USING DELTA
    LOCATION 's3a://mimic-silver/Billing/d_hcpcs'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.d_icd_diagnoses
    USING DELTA
    LOCATION 's3a://mimic-silver/Billing/d_icd_diagnoses'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.d_icd_procedures
    USING DELTA
    LOCATION 's3a://mimic-silver/Billing/d_icd_procedures'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.diagnoses_icd
    USING DELTA
    LOCATION 's3a://mimic-silver/Billing/diagnoses_icd'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.drgcodes
    USING DELTA
    LOCATION 's3a://mimic-silver/Billing/drgcodes'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.hcpcsevents
    USING DELTA
    LOCATION 's3a://mimic-silver/Billing/hcpcsevents'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.procedures_icd
    USING DELTA
    LOCATION 's3a://mimic-silver/Billing/procedures_icd'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.d_labitems
    USING DELTA
    LOCATION 's3a://mimic-silver/Measurement/d_labitems'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.labevents
    USING DELTA
    LOCATION 's3a://mimic-silver/Measurement/labevents'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.microbiologyevents
    USING DELTA
    LOCATION 's3a://mimic-silver/Measurement/microbiologyevents'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.omr
    USING DELTA
    LOCATION 's3a://mimic-silver/Measurement/omr'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.emar_detail
    USING DELTA
    LOCATION 's3a://mimic-silver/Medication/emar_detail'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.emar
    USING DELTA
    LOCATION 's3a://mimic-silver/Medication/emar'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.pharmacy
    USING DELTA
    LOCATION 's3a://mimic-silver/Medication/pharmacy'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.prescriptions
    USING DELTA
    LOCATION 's3a://mimic-silver/Medication/prescriptions'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.provider
    USING DELTA
    LOCATION 's3a://mimic-silver/Provider/provider'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.caregiver
    USING DELTA
    LOCATION 's3a://mimic-silver/ICU-Caregiver/caregiver'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.chartevents
    USING DELTA
    LOCATION 's3a://mimic-silver/ICU-Measurement/chartevents'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.d_items
    USING DELTA
    LOCATION 's3a://mimic-silver/ICU-Measurement/d_items'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.datetimeevents
    USING DELTA
    LOCATION 's3a://mimic-silver/ICU-Measurement/datetimeevents'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.ingredientevents
    USING DELTA
    LOCATION 's3a://mimic-silver/ICU-Measurement/ingredientevents'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.inputevents
    USING DELTA
    LOCATION 's3a://mimic-silver/ICU-Measurement/inputevents'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.outputevents
    USING DELTA
    LOCATION 's3a://mimic-silver/ICU-Measurement/outputevents'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.procedureevents
    USING DELTA
    LOCATION 's3a://mimic-silver/ICU-Measurement/procedureevents'
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS SilverLayer.icustays
    USING DELTA
    LOCATION 's3a://mimic-silver/ICU-PatientTracking/icustays'
""")

spark.stop()
