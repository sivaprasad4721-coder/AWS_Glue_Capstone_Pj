import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


customer_df=spark.sql(''' select * from customer_db.customers_dataset_csv  ''')

customer_df.show(10,0)

drop_customer_dup = customer_df.dropDuplicates(["customer_id"])

drop_null = drop_customer_dup.dropna(subset=["email", "zip_code"])

output_path = "s3://custdatatrans/target/staging/"

drop_null.write.mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(output_path)

job.commit()
