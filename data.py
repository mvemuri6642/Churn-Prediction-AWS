 
import datetime
import json
import random
import boto3
import pyspark
import os
import sagemaker_pyspark
from pyspark.sql import SparkSession
role = get_execution_role()
# Configure Spark to use the SageMaker Spark dependency jars
jars = sagemaker_pyspark.classpath_jars()

classpath = ":".join(sagemaker_pyspark.classpath_jars())
STREAM_NAME = "ExampleInputStream"
my_session = boto3.session.Session()
my_region = my_session.region_name



def get_data():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell'

    s1 = (
    SparkSession.builder.config("spark.driver.extraClassPath", classpath)
    .master("local[*]")
    .getOrCreate()
    )

    hadoopConf=s1._jsc.hadoopConfiguration()
    hadoopConf.set('fs.s3a.access.key','AKIA5BL67OKLIAAI6ENY')
    hadoopConf.set('fs.s3a.secret.key','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')
    hadoopConf.set('spark.haddop.fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

    testingData = (
    s1.read.format("libsvm")
    .option("header", "True")  # If your CSV has a header row
    .option("inferSchema", "False")  # Infers the data types of columns
    .option("numFeatures", "15")
    .load("s3a://sagemaker-us-east-1-896303854230/lp-notebooks-datasets/ecom/text-csv/test/test")
    )
    return testingData




def generate(stream_name, kinesis_client):
    while True:
        data = get_data()
        print(data)
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey="partitionkey")


if __name__ == '__main__':
    generate(STREAM_NAME, boto3.client('kinesis',region_name=my_region))
