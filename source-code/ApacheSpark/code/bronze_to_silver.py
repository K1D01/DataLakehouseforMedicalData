from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import when, to_date, to_timestamp, col, substring

spark = SparkSession.builder \
    .appName("MIMIC-IV Bronze to Silver") \
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


print("🔄 Processing patients")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/patients")
df_silver = df.withColumn("gender", when(col("gender") == "M", "Male").when(col("gender") == "F", "Female")) \
              .withColumn("dod", to_date("dod", "yyyy-MM-dd"))\
              .withColumn("anchor_year", col("anchor_year").cast("int")) \
              .withColumn("anchor_age", col("anchor_age").cast("int"))\
              .withColumn("subject_id", col("subject_id").cast("int"))
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/PatientTracking/patients")
print("✅ Done patients")

print("🔄 Processing admissions")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/admissions")
df_silver = df.withColumn("subject_id", col("subject_id").cast("int"))\
              .withColumn("hadm_id", col("hadm_id").cast("int"))\
              .withColumn("hospital_expire_flag", col("hospital_expire_flag").cast("int"))\
              .withColumn("admittime", to_date(col("admittime"))) \
              .withColumn("dischtime", to_date(col("dischtime"))) \
              .withColumn("edregtime", to_date(col("edregtime"))) \
              .withColumn("edouttime", to_date(col("edouttime"))) \
              .withColumn("deathtime", to_date(col("deathtime")))     
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/PatientTracking/admissions")
print("✅ Done admissions")

print("🔄 Processing transfers")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/transfers")
df_silver = df.withColumn("subject_id", col("subject_id").cast("int"))\
              .withColumn("hadm_id", col("hadm_id").cast("int"))\
              .withColumn("transfer_id", col("transfer_id").cast("int"))\
              .withColumn("intime", to_date(col("intime"))) \
              .withColumn("outtime", to_date(col("outtime")))    
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/PatientTracking/transfers")
print("✅ Done transfers")

print("🔄 Processing services")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/services")
df_silver = df.withColumn("subject_id", col("subject_id").cast("int"))\
              .withColumn("hadm_id", col("hadm_id").cast("int"))\
              .withColumn("transfertime", to_date(col("transfertime")))    
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/Administration/services")
print("✅ Done services")

print("🔄 Processing poe")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/poe")
df_silver = df.withColumn("subject_id", col("subject_id").cast("int"))\
              .withColumn("poe_id", substring(col("poe_id"), 1, 8).cast("int"))\
              .withColumn("poe_seq", col("poe_seq").cast("int"))\
              .withColumn("hadm_id", col("hadm_id").cast("int"))\
              .withColumn("ordertime", to_date(col("ordertime"))) 
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/Administration/poe")
print("✅ Done poe")

print("🔄 Processing poe_detail")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/poe_detail")
df_silver = df.withColumn("subject_id", col("subject_id").cast("int"))\
              .withColumn("poe_id", substring(col("poe_id"), 1, 8).cast("int"))\
              .withColumn("poe_seq", col("poe_seq").cast("int"))
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/Administration/poe_detail")
print("✅ Done poe_detail")

print("🔄 Processing diagnoses_icd")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/diagnoses_icd")
df_silver = df.withColumn("subject_id", col("subject_id").cast("int"))\
              .withColumn("hadm_id", col("hadm_id").cast("int"))\
              .withColumn("icd_version", col("icd_version").cast("int"))\
              .withColumn("seq_num", col("seq_num").cast("int"))
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/Billing/diagnoses_icd")
print("✅ Done diagnoses_icd")

print("🔄 Processing d_icd_diagnoses")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/d_icd_diagnoses")
df_silver = df.withColumn("icd_version", col("icd_version").cast("int"))
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/Billing/d_icd_diagnoses")
print("✅ Done d_icd_diagnoses")

print("🔄 Processing procedures_icd")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/procedures_icd")
df_silver = df.withColumn("icd_version", col("icd_version").cast("int"))\
              .withColumn("subject_id", col("subject_id").cast("int"))\
              .withColumn("hadm_id", col("hadm_id").cast("int"))\
              .withColumn("seq_num", col("seq_num").cast("int"))\
              .withColumn("chartdate", to_date(col("chartdate"))) 
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/Billing/procedures_icd")
print("✅ Done procedures_icd")

print("🔄 Processing d_icd_procedures")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/d_icd_procedures")
df_silver = df.withColumn("icd_version", col("icd_version").cast("int"))
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/Billing/d_icd_procedures")
print("✅ Done d_icd_procedures")

print("🔄 Processing drgcodes")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/drgcodes")
df_silver = df.withColumn("subject_id", col("subject_id").cast("int"))\
              .withColumn("hadm_id", col("hadm_id").cast("int"))\
              .withColumn("drg_code", col("drg_code").cast("int"))\
              .withColumn("drg_severity", col("drg_severity").cast("int"))\
              .withColumn("drg_mortality", col("drg_mortality").cast("int"))
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/Billing/drgcodes")
print("✅ Done drgcodes")

print("🔄 Processing hcpcsevents")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/hcpcsevents")
df_silver = df.withColumn("subject_id", col("subject_id").cast("int"))\
              .withColumn("hadm_id", col("hadm_id").cast("int"))\
              .withColumn("seq_num", col("seq_num").cast("int"))\
              .withColumn("chartdate", to_date(col("chartdate"))) 
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/Billing/hcpcsevents")
print("✅ Done hcpcsevents")

print("🔄 Processing d_hcpcs")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/d_hcpcs")
df_silver = df.withColumn("category", col("category").cast("int"))
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/Billing/d_hcpcs")
print("✅ Done d_hcpcs")

print("🔄 Processing pharmacy")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/pharmacy")
df_silver = df.withColumn("subject_id", col("subject_id").cast("int"))\
                .withColumn("hadm_id", col("hadm_id").cast("int"))\
                .withColumn("pharmacy_id", col("pharmacy_id").cast("int"))\
                .withColumn("starttime", to_date(col("starttime")))\
                .withColumn("stoptime", to_date(col("stoptime")))\
                .withColumn("entertime", to_date(col("entertime")))\
                .withColumn("verifiedtime", to_date(col("verifiedtime")))\
                .withColumn("expirationdate", to_date(col("expirationdate")))
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/Medication/pharmacy")
print("✅ Done pharmacy")

print("🔄 Processing prescriptions")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/prescriptions")
df_silver = df.withColumn("subject_id", col("subject_id").cast("int"))\
                .withColumn("hadm_id", col("hadm_id").cast("int"))\
                .withColumn("pharmacy_id", col("pharmacy_id").cast("int"))\
                .withColumn("poe_id", substring("poe_id", 1, 8).cast("int"))\
                .withColumn("poe_seq", col("poe_seq").cast("int"))\
                .withColumn("starttime", to_date(col("starttime")))\
                .withColumn("stoptime", to_date(col("stoptime")))
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/Medication/prescriptions")
print("✅ Done prescriptions")

print("🔄 Processing emar")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/emar")
df_silver = df.withColumn("subject_id", col("subject_id").cast("int"))\
                .withColumn("hadm_id", col("hadm_id").cast("int"))\
                .withColumn("emar_seq", col("emar_seq").cast("int"))\
                .withColumn("emar_id", substring("emar_id", 1, 8).cast("int"))\
                .withColumn("pharmacy_id", col("pharmacy_id").cast("int"))\
                .withColumn("charttime", to_date(col("charttime")))\
                .withColumn("scheduletime", to_date(col("scheduletime")))\
                .withColumn("storetime", to_date(col("storetime")))
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/Medication/emar")
print("✅ Done emar")

print("🔄 Processing emar_detail")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/emar_detail")
df_silver = df.withColumn("subject_id", col("subject_id").cast("int"))\
                .withColumn("emar_id", substring("emar_id", 1, 8).cast("int"))\
                .withColumn("emar_seq", col("emar_seq").cast("int"))\
                .withColumn("pharmacy_id", col("pharmacy_id").cast("int"))
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/Medication/emar_detail")
print("✅ Done emar_detail")

print("🔄 Processing labevents")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/labevents")
df_silver = df.withColumn("charttime", to_date("charttime")) \
              .withColumn("storetime", to_date("storetime"))\
              .withColumn("subject_id", col("subject_id").cast("int")) \
              .withColumn("hadm_id", col("hadm_id").cast("int")) \
              .withColumn("specimen_id", col("specimen_id").cast("int")) \
              .withColumn("itemid", col("itemid").cast("int")) \
              .withColumn("labevent_id", col("labevent_id").cast("int"))\
              .withColumn("valuenum", col("valuenum").cast("double")) \
              .withColumn("ref_range_lower", col("ref_range_lower").cast("double")) \
              .withColumn("ref_range_upper", col("ref_range_upper").cast("double"))
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/Measurement/labevents")
print("✅ Done labevents")

print("🔄 Processing microbiologyevents")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/microbiologyevents")
df_silver = df.withColumn("chartdate", to_date("chartdate")) \
                .withColumn("storedate", to_date("storedate")) \
                .withColumn("microevent_id", col("microevent_id").cast("int")) \
                .withColumn("subject_id", col("subject_id").cast("int")) \
                .withColumn("hadm_id", col("hadm_id").cast("int")) \
                .withColumn("micro_specimen_id", col("micro_specimen_id").cast("int")) \
                .withColumn("spec_itemid", col("spec_itemid").cast("int")) \
                .withColumn("org_itemid", col("org_itemid").cast("int")) \
                .withColumn("test_itemid", col("test_itemid").cast("int")) \
                .withColumn("test_seq", col("test_seq").cast("int")) \
                .withColumn("ab_itemid", col("ab_itemid").cast("int"))            
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/Measurement/microbiologyevents")
print("✅ Done microbiologyevents")

print("🔄 Processing omr")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/omr")
df_silver = df.withColumn("chartdate", to_date("chartdate"))\
                .withColumn("subject_id", col("subject_id").cast("int")) \
                .withColumn("seq_num", col("seq_num").cast("int"))          
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/Measurement/omr")
print("✅ Done omr")

print("🔄 Processing d_labitems")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/d_labitems")
df_silver = df.withColumn("itemid", col("itemid").cast("int"))   
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/Measurement/d_labitems")
print("✅ Done d_labitems")

print("🔄 Processing provider")
df = spark.read.format("delta").load("s3a://mimic-bronze/hosp/provider")
df_silver = df
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/Provider/provider")
print("✅ Done provider")

print("🔄 Processing icustays")
df = spark.read.format("delta").load("s3a://mimic-bronze/icu/icustays")
df_silver = df.withColumn("subject_id", col("subject_id").cast("int")) \
                   .withColumn("hadm_id", col("hadm_id").cast("int")) \
                   .withColumn("stay_id", col("stay_id").cast("int")) \
                   .withColumn("intime", to_date("intime")) \
                   .withColumn("outtime", to_date("outtime"))
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/ICU-PatientTracking/icustays")
print("✅ Done icustays")

print("🔄 Processing chartevents")
df = spark.read.format("delta").load("s3a://mimic-bronze/icu/chartevents")
df_silver = df.withColumn("subject_id", col("subject_id").cast("int")) \
              .withColumn("hadm_id", col("hadm_id").cast("int")) \
              .withColumn("stay_id", col("stay_id").cast("int")) \
              .withColumn("caregiver_id", col("caregiver_id").cast("int")) \
              .withColumn("itemid", col("itemid").cast("int")) \
              .withColumn("charttime", to_date("charttime")) \
              .withColumn("storetime", to_date("storetime")) \
              .withColumn("valuenum", col("valuenum").cast("double")) \
              .withColumn("valueuom", col("valueuom").cast("string")) \
              .withColumn("value", col("value").cast("string")) \
              .withColumn("warning", col("warning").cast("int"))
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/ICU-Measurement/chartevents")
print("✅ Done chartevents")

print("🔄 Processing datetimeevents")
df = spark.read.format("delta").load("s3a://mimic-bronze/icu/datetimeevents")
df_silver = df.withColumn("subject_id", col("subject_id").cast("int")) \
              .withColumn("hadm_id", col("hadm_id").cast("int")) \
              .withColumn("stay_id", col("stay_id").cast("int")) \
              .withColumn("caregiver_id", col("caregiver_id").cast("int")) \
              .withColumn("itemid", col("itemid").cast("int")) \
              .withColumn("charttime", to_date("charttime")) \
              .withColumn("storetime", to_date("storetime")) \
              .withColumn("value", to_date("value")) 
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/ICU-Measurement/datetimeevents")
print("✅ Done datetimeevents")

print("🔄 Processing ingredientevents")
df = spark.read.format("delta").load("s3a://mimic-bronze/icu/ingredientevents")
df_silver = df.withColumn("subject_id", col("subject_id").cast("int")) \
              .withColumn("hadm_id", col("hadm_id").cast("int")) \
              .withColumn("stay_id", col("stay_id").cast("int")) \
              .withColumn("caregiver_id", col("caregiver_id").cast("int")) \
              .withColumn("itemid", col("itemid").cast("int")) \
              .withColumn("orderid", col("orderid").cast("int")) \
              .withColumn("linkorderid", col("linkorderid").cast("int")) \
              .withColumn("amount", col("amount").cast("double")) \
              .withColumn("rate", col("rate").cast("double")) \
              .withColumn("originalamount", col("originalamount").cast("double")) \
              .withColumn("originalrate", col("originalrate").cast("double")) \
              .withColumn("starttime", to_date("starttime")) \
              .withColumn("endtime", to_date("endtime")) \
              .withColumn("storetime", to_date("storetime")) 
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/ICU-Measurement/ingredientevents")
print("✅ Done ingredientevents")

print("🔄 Processing inputevents")
df = spark.read.format("delta").load("s3a://mimic-bronze/icu/inputevents")
df_silver = df.withColumn("subject_id", col("subject_id").cast("int")) \
                         .withColumn("hadm_id", col("hadm_id").cast("int")) \
                         .withColumn("stay_id", col("stay_id").cast("int")) \
                         .withColumn("caregiver_id", col("caregiver_id").cast("int")) \
                         .withColumn("itemid", col("itemid").cast("int")) \
                         .withColumn("orderid", col("orderid").cast("int")) \
                         .withColumn("linkorderid", col("linkorderid").cast("int")) \
                         .withColumn("amount", col("amount").cast("double")) \
                         .withColumn("rate", col("rate").cast("double")) \
                         .withColumn("originalamount", col("originalamount").cast("double")) \
                         .withColumn("originalrate", col("originalrate").cast("double")) \
                         .withColumn("patientweight", col("patientweight").cast("double")) \
                         .withColumn("totalamount", col("totalamount").cast("double")) \
                         .withColumn("isopenbag", col("isopenbag").cast("int")) \
                         .withColumn("continueinnextdept", col("continueinnextdept").cast("int")) \
                         .withColumn("starttime",to_date("starttime")) \
                         .withColumn("endtime", to_date("endtime")) \
                         .withColumn("storetime", to_date("storetime")) 
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/ICU-Measurement/inputevents")
print("✅ Done inputevents")

print("🔄 Processing outputevents")
df = spark.read.format("delta").load("s3a://mimic-bronze/icu/outputevents")
df_silver = df.withColumn("subject_id", col("subject_id").cast("int")) \
              .withColumn("hadm_id", col("hadm_id").cast("int")) \
              .withColumn("stay_id", col("stay_id").cast("int")) \
              .withColumn("caregiver_id", col("caregiver_id").cast("int")) \
              .withColumn("itemid", col("itemid").cast("int")) \
              .withColumn("value", col("value").cast("int")) \
              .withColumn("charttime", to_date("charttime")) \
              .withColumn("storetime", to_date("storetime"))
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/ICU-Measurement/outputevents")
print("✅ Done outputevents")

print("🔄 Processing procedureevents")
df = spark.read.format("delta").load("s3a://mimic-bronze/icu/procedureevents")
df_silver = df.withColumn("subject_id", col("subject_id").cast("int")) \
    .withColumn("hadm_id", col("hadm_id").cast("int")) \
    .withColumn("stay_id", col("stay_id").cast("int")) \
    .withColumn("caregiver_id", col("caregiver_id").cast("int")) \
    .withColumn("itemid", col("itemid").cast("int")) \
    .withColumn("orderid", col("orderid").cast("int")) \
    .withColumn("linkorderid", col("linkorderid").cast("int")) \
    .withColumn("value", col("value").cast("double")) \
    .withColumn("patientweight", col("patientweight").cast("double")) \
    .withColumn("isopenbag", col("isopenbag").cast("int")) \
    .withColumn("continueinnextdept", col("continueinnextdept").cast("int")) \
    .withColumn("originalamount", col("originalamount").cast("double")) \
    .withColumn("originalrate", col("originalrate").cast("double")) \
    .withColumn("starttime", to_date("starttime")) \
    .withColumn("endtime", to_date("endtime")) \
    .withColumn("storetime", to_date("storetime"))
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/ICU-Measurement/procedureevents")
print("✅ Done procedureevents")

print("🔄 Processing d_items")
df = spark.read.format("delta").load("s3a://mimic-bronze/icu/d_items")
df_silver = df.withColumn("itemid", col("itemid").cast("int")) \
              .withColumn("lownormalvalue", col("lownormalvalue").cast("int")) \
              .withColumn("highnormalvalue", col("highnormalvalue").cast("int"))
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/ICU-Measurement/d_items")
print("✅ Done d_items")

print("🔄 Processing caregiver")
df = spark.read.format("delta").load("s3a://mimic-bronze/icu/caregiver")
df_silver = df.withColumn("caregiver_id", col("caregiver_id").cast("int"))
df_silver.write.format("delta").mode("ignore").save("s3a://mimic-silver/ICU-Caregiver/caregiver")
print("✅ Done caregiver")

print("✅ Silver successfully!")
spark.stop()