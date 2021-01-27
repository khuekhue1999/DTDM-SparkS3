# Databricks notebook source
import urllib
ACCESS_KEY = "AKIAZ6VV26XQQ5ERDOOT"
SECRET_KEY = "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"
ENCODED_SECRET_KEY = urllib.parse.quote(SECRET_KEY, "")
AWS_BUCKET_NAME = "dtdm" #ten bucket
MOUNT_NAME = "data_DTDM" #Tên thư mục đặt tên bất kỳ
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

display(dbutils.fs.ls("mnt/data_DTDM/"))

# COMMAND ----------

file_location = "dbfs:/mnt/data_DTDM/train.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

df.printSchema


# COMMAND ----------

df.count

# COMMAND ----------

df. repartition(1).write. mode('overwrite'). parquet('mnt/data/DTDM_data')

# COMMAND ----------

display(dbutils.fs.ls("mnt/data/DTDM_data"))

# COMMAND ----------

parquetFile = spark.read.parquet("dbfs:/mnt/data/DTDM_data/part-00000-tid-3184447195556126130-c248ef55-28c9-41f4-a9e0-811e1400dd5b-255-1-c000.snappy.parquet")

# COMMAND ----------

parquetFile.show()

# COMMAND ----------

type(parquetFile)

# COMMAND ----------


