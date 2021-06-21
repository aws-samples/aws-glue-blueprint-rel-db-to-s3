# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


def log_error_and_exit(logger, error_msg):
    logger.error(error_msg)
    raise Exception(error_msg)


# @params: [JOB_NAME]
# Get the parameters passed to the Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'nature_of_data', 'source_table_name', 'source_schema_name', 'target_db_name', 'target_s3_location', 'target_partition_columns', 'source_s3_location', 'type_of_pull'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()
# Parameter validation
nature_of_data = args['nature_of_data']
logger.info("Nature of Data: " + nature_of_data)
if nature_of_data != 'mutable' and nature_of_data != 'immutable':
    error_msg = "nature_of_data can either be mutable or immutable"
    log_error_and_exit(logger, error_msg)

source_table_name = args['source_table_name']
logger.info("Table Name: " + source_table_name)
if not(source_table_name and source_table_name.strip()):
    error_msg = "source_table_name cannot be an empty string or spaces"
    log_error_and_exit(logger, error_msg)


type_of_pull = args['type_of_pull']
logger.info("Type of Pull: " + type_of_pull)
if type_of_pull != 'full' and type_of_pull != 'incremental':
    error_msg = "type_of_pull can either be full or incremental"
    log_error_and_exit(logger, error_msg)

source_schema_name = args['source_schema_name']
logger.info("Schema Name: " + source_schema_name)
if not(source_schema_name and source_schema_name.strip()):
    error_msg = "source_schema_name cannot be an empty string or spaces"
    log_error_and_exit(logger, error_msg)

target_db_name = args['target_db_name']
logger.info("Target DB Name: " + target_db_name)
if not(target_db_name and target_db_name.strip()):
    error_msg = "target_db_name cannot be an empty string or spaces"
    log_error_and_exit(logger, error_msg)

target_s3_location = args['target_s3_location']
logger.info("Target S3 Location: " + target_s3_location)
if not(target_s3_location and target_s3_location.strip()):
    error_msg = "target_s3_location cannot be an empty string or spaces"
    log_error_and_exit(logger, error_msg)


target_partition_columns = args['target_partition_columns']
logger.info("Target Partition Columns: " + target_partition_columns)
if not(target_partition_columns and target_partition_columns.strip()):
    error_msg = "target_partition_columns cannot be an empty string or spaces"
    log_error_and_exit(logger, error_msg)
target_partition_columns = target_partition_columns.replace(" ", "")
partition_column_list = target_partition_columns.split(",")

source_s3_location = args['source_s3_location']
logger.info("Source S3 Location: " + source_s3_location)
if not(source_s3_location and source_s3_location.strip()):
    error_msg = "source_s3_location cannot be an empty string or spaces"
    log_error_and_exit(logger, error_msg)

# Check if the target table exists. If it does not exist, this is the first run of the job.
glue = boto3.client('glue')
try:
    response = glue.get_table(
        DatabaseName=target_db_name,
        Name=source_table_name
    )
    first_run = False
except glue.exceptions.EntityNotFoundException:
    first_run = True

source_s3_path = source_s3_location + source_schema_name + "/" + source_table_name + "/"
target_s3_path = target_s3_location + source_schema_name + "/" + source_table_name + "/"
logger.info("Source S3 Path: " + source_s3_path)
logger.info("Target S3 Path:" + target_s3_path)

input_DyF = glueContext.create_dynamic_frame.from_options(connection_type="s3", format="parquet", connection_options={"paths": [source_s3_path]}, transformation_ctx="DataSource0")
input_record_count = input_DyF.count()
if input_record_count > 0:
    if first_run:
        if target_partition_columns != 'None':
            DataSink0 = glueContext.getSink(path=target_s3_path, connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=partition_column_list, compression="snappy", enableUpdateCatalog=True, transformation_ctx="DataSink0")
        else:
            DataSink0 = glueContext.getSink(path=target_s3_path, connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", compression="snappy", enableUpdateCatalog=True, transformation_ctx="DataSink0")
        DataSink0.setCatalogInfo(catalogDatabase=target_db_name, catalogTableName=source_table_name)
        DataSink0.setFormat("glueparquet")
        DataSink0.writeFrame(input_DyF)
    elif nature_of_data == 'immutable':
        if type_of_pull == 'full':
            D0 = glueContext.purge_table(target_db_name, source_table_name, options={"retentionPeriod": 0})
        if target_partition_columns != 'None':
            DataSink0 = glueContext.getSink(path=target_s3_path, connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=partition_column_list, compression="snappy", enableUpdateCatalog=True, transformation_ctx="DataSink0")
        else:
            DataSink0 = glueContext.getSink(path=target_s3_path, connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", compression="snappy", enableUpdateCatalog=True, transformation_ctx="DataSink0")
        DataSink0.setCatalogInfo(catalogDatabase=target_db_name, catalogTableName=source_table_name)
        DataSink0.setFormat("glueparquet")
        DataSink0.writeFrame(input_DyF)

    elif type_of_pull == 'incremental':
        args = getResolvedOptions(sys.argv, ['primary_key'])
        primary_key = args['primary_key']
        logger.info("Primary Keys: " + primary_key)
        if not(primary_key and primary_key.strip()):
            error_msg = "primary_key cannot be an empty string or spaces when type_of_pull is incremental and nature_of_data is mutable"
            log_error_and_exit(logger, error_msg)
        primary_key = primary_key.replace(" ", "")
        primary_key_list = primary_key.split(",")

        if target_partition_columns != 'None':
            columns_to_keep = primary_key + "," + target_partition_columns
            columns_to_keep_list = columns_to_keep.split(",")
            target_DF = spark.sql("select " + primary_key + ", " + target_partition_columns + " from `" + target_db_name + "`.`" + source_table_name + "`")
            target_DyF = DynamicFrame.fromDF(target_DF, glueContext, "target_DyF")
            input_DyF1 = input_DyF.select_fields(columns_to_keep_list)
            join_DyF = input_DyF1.join(primary_key_list, primary_key_list, target_DyF)
            join_DF = join_DyF.select_fields(partition_column_list).toDF()
            input_DF = input_DyF.toDF()
            full_DF = input_DF.select(partition_column_list).union(join_DF)
            distinct_partitions = full_DF.select(partition_column_list).distinct().collect()

            where_predicate = ""
            cnt_outer = 1
            for x in distinct_partitions:
                cnt_inner = 1
                where_predicate = where_predicate + "("
                for y in partition_column_list:
                    where_predicate = where_predicate + y + "=='" + str(x[y]) + "' "
                    if cnt_inner < len(partition_column_list):
                        where_predicate = where_predicate + " and "
                    else:
                        where_predicate = where_predicate + " ) "
                    cnt_inner = cnt_inner + 1
                if cnt_outer < len(distinct_partitions):
                    where_predicate = where_predicate + " or "
                cnt_outer = cnt_outer + 1
            target_DF = spark.sql("select * from `" + target_db_name + "`.`" + source_table_name + "` where " + where_predicate)
        else:
            target_DF = spark.sql("select * from `" + target_db_name + "`.`" + source_table_name + "`")
            where_predicate = '(1==1)'
        target_DyF = DynamicFrame.fromDF(target_DF, glueContext, "target_DyF")
        merged_DyF = target_DyF.mergeDynamicFrame(input_DyF, primary_key_list)
        merged_DF = merged_DyF.toDF()
        merged_DF.persist()
        merged_DF.count()
        D0 = glueContext.purge_table(target_db_name, source_table_name, options={"partitionPredicate": where_predicate, "retentionPeriod": 0})
        merged_DyF = DynamicFrame.fromDF(merged_DF, glueContext, "merged_DyF")
        if target_partition_columns != 'None':
            DataSink0 = glueContext.getSink(path=target_s3_path, connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=partition_column_list, compression="snappy", enableUpdateCatalog=True, transformation_ctx="DataSink0")
        else:
            DataSink0 = glueContext.getSink(path=target_s3_path, connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", compression="snappy", enableUpdateCatalog=True, transformation_ctx="DataSink0")
        DataSink0.setCatalogInfo(catalogDatabase=target_db_name, catalogTableName=source_table_name)
        DataSink0.setFormat("glueparquet")
        DataSink0.writeFrame(merged_DyF)
    else:
        D0 = glueContext.purge_table(target_db_name, source_table_name, options={"retentionPeriod": 0})
        if target_partition_columns != 'None':
            DataSink0 = glueContext.getSink(path=target_s3_path, connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=target_partition_columns, compression="snappy", enableUpdateCatalog=True, transformation_ctx="DataSink0")
        else:
            DataSink0 = glueContext.getSink(path=target_s3_path, connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", compression="snappy", enableUpdateCatalog=True, transformation_ctx="DataSink0")
        DataSink0.setCatalogInfo(catalogDatabase=target_db_name, catalogTableName=source_table_name)
        DataSink0.setFormat("glueparquet")
        DataSink0.writeFrame(input_DyF)
    logger.info("Processed " + str(input_record_count) + " records from source")
else:
    logger.info("No records received from source. Nothing processed")
logger.info("Job Ended Successfully")
job.commit()
