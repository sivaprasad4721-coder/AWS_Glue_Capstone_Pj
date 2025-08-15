import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import sum , count, explode, col, first, year

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

customer_df = spark.read.parquet("s3://custdatatrans/target/staging/part-00000-694aa1d4-f12e-47ef-b121-b05c7e3e15dd-c000.snappy.parquet")

geolocation_df = glueContext.create_dynamic_frame.from_catalog(
    database="customer_db",
    table_name="geolocation_dataset_json"
)
 
df_geolocation = geolocation_df.toDF()

transaction_df = spark.sql(''' select * from customer_db.transactions_csv ''')

df_joined = (
    customer_df.join(df_geolocation, on="zip_code", how="inner")
       .join(transaction_df, on="customer_id", how="inner")
)

df_agg = (
    df_joined.groupBy("customer_id")
      .agg(
          sum("transaction_amount").alias("total_transaction_amount"),
          count("transaction_amount").alias("transaction_count"),
           first("city").alias("city"), 
           first("state").alias("state"),
           first(year(col("created_at"))).alias("year")          
      )
)

df_agg.show(10,0)

output_path = "s3://custdatatrans/target/final/"
 
df_agg.write.mode("overwrite") \
    .partitionBy("state","year") \
    .option("compression", "snappy") \
    .parquet(output_path)
    
job.commit()