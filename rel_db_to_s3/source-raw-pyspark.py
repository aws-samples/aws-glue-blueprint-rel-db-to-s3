# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import boto3


def log_error_and_exit(logger, error_msg):
    logger.error(error_msg)
    raise Exception(error_msg)

# @params: [JOB_NAME]
# Get the parameters passed to the Glue job
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME', 'parallel_pull', 'glue_catalog_db_name', 'type_of_pull', 'source_table_name', 'source_schema_name', 'glue_catalog_table_name', 'target_s3_location'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

# Parameter validation
parallel_pull = args['parallel_pull']
logger.info("Parallel Pull: " + parallel_pull)
if parallel_pull != 'false' and parallel_pull != 'true':
    error_msg = "parallel_pull can either be true or false"
    log_error_and_exit(logger, error_msg)

source_table_name = args['source_table_name']
logger.info("Table Name: " + source_table_name)
if not(source_table_name and source_table_name.strip()):
    error_msg = "source_table_name cannot be an empty string or spaces"
    log_error_and_exit(logger, error_msg)

source_schema_name = args['source_schema_name']
logger.info("Schema Name: " + source_schema_name)
if not(source_schema_name and source_schema_name.strip()):
    error_msg = "source_schema_name cannot be an empty string or spaces"
    log_error_and_exit(logger, error_msg)

glue_catalog_db_name = args['glue_catalog_db_name']
logger.info("Glue catalog DB Name: " + glue_catalog_db_name)
if not(glue_catalog_db_name and glue_catalog_db_name.strip()):
    error_msg = "glue_catalog_db_name cannot be an empty string or spaces"
    log_error_and_exit(logger, error_msg)

glue_catalog_table_name = args['glue_catalog_table_name']
logger.info("Glue Catalog Table Name: " + glue_catalog_table_name)
if not(glue_catalog_table_name and glue_catalog_table_name.strip()):
    error_msg = "glue_catalog_table_name cannot be an empty string or spaces"
    log_error_and_exit(logger, error_msg)

type_of_pull = args['type_of_pull']
logger.info("Type of Pull: " + type_of_pull)
if type_of_pull != 'full' and type_of_pull != 'incremental':
    error_msg = "type_of_pull can either be full or incremental"
    log_error_and_exit(logger, error_msg)

target_s3_location = args['target_s3_location']
logger.info("Target S3 Location: " + target_s3_location)
if not(target_s3_location and target_s3_location.strip()):
    error_msg = "target_s3_location cannot be an empty string or spaces"
    log_error_and_exit(logger, error_msg)

target_s3_location = target_s3_location + source_schema_name + "/" + source_table_name + "/"
if parallel_pull == 'true':
    args = getResolvedOptions(sys.argv, ['hashfield', 'hashpartitions'])
    # Parameter validation
    hashfield = args['hashfield']
    logger.info("Hashfield: " + hashfield)
    if not(hashfield and hashfield.strip()):
        error_msg = "hashfield cannot be an empty string or spaces when parallel pull is true"
        log_error_and_exit(logger, error_msg)

    hashpartitions = args['hashpartitions']
    logger.info("Hashpartitions: " + hashpartitions)
    if not(hashpartitions and hashpartitions.strip()):
        error_msg = "hashpartitions cannot be an empty string or spaces when parallel pull is true"
        log_error_and_exit(logger, error_msg)

    try:
        numhashpartitions = int(hashpartitions)
        if numhashpartitions <= 0:
            error_msg = "hashpartitions needs to be a positive integer"
            log_error_and_exit(logger, error_msg)
    except:
        error_msg = "hashpartitions needs to be a positive integer"
        log_error_and_exit(logger, error_msg)

if type_of_pull == 'incremental':
    args = getResolvedOptions(sys.argv, ['bookmark_key', 'bookmark_key_order'])
    bookmark_key = args['bookmark_key']
    logger.info("Bookmark Key: " + bookmark_key)
    if not(bookmark_key and bookmark_key.strip()):
        error_msg = "bookmark_key cannot be an empty string or spaces when type_of_pull is incremental"
        log_error_and_exit(logger, error_msg)
    bookmark_key = bookmark_key.replace(" ", "")

    bookmark_key_order = args['bookmark_key_order']
    logger.info("Bookmark Key Order: " + bookmark_key_order)
    if bookmark_key_order != 'asc' and bookmark_key_order != 'desc':
        error_msg = "bookmark_key_order can either be asc or desc"
        log_error_and_exit(logger, error_msg)

    if parallel_pull == 'true':
        source_DyF = glueContext.create_dynamic_frame_from_catalog(glue_catalog_db_name, glue_catalog_table_name, transformation_ctx="DataSource0",
                                                                   additional_options={"jobBookmarkKeys": [bookmark_key], "jobBookmarkKeysSortOrder": bookmark_key_order, "hashfield": hashfield, "hashpartitions": hashpartitions})
    else:
        source_DyF = glueContext.create_dynamic_frame_from_catalog(glue_catalog_db_name, glue_catalog_table_name, transformation_ctx="DataSource0",
                                                                   additional_options={"jobBookmarkKeys": [bookmark_key], "jobBookmarkKeysSortOrder": bookmark_key_order})
else:
    if parallel_pull == 'true':
        source_DyF = glueContext.create_dynamic_frame_from_catalog(glue_catalog_db_name, glue_catalog_table_name, additional_options={"hashfield": hashfield, "hashpartitions": hashpartitions})
    else:
        source_DyF = glueContext.create_dynamic_frame_from_catalog(glue_catalog_db_name, glue_catalog_table_name)
rec_count = source_DyF.count()
Datasink0 = glueContext.write_dynamic_frame_from_options(frame=source_DyF, connection_type="s3", connection_options={"path": target_s3_location}, format="parquet")
logger.info("Job Ended Successfully. Pulled " + str(rec_count) + " records from source")
job.commit()
