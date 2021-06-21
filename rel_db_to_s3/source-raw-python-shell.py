# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
from awsglue.utils import getResolvedOptions
import awswrangler as wr
import pandas
import boto3
from datetime import datetime
import logging
from logging import getLogger, StreamHandler, INFO


def log_error_and_exit(logger, error_msg):
    logger.error(error_msg)
    raise Exception(error_msg)


# Set Logger properties
logger = getLogger(__name__)
handler = StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logger.propagate = False

now = datetime.now()
dt_string = now.strftime("%Y%m%d%H%M%S")
# Get the parameters passed to the Glue job
args = getResolvedOptions(sys.argv,
                          ['source_db_type', 'type_of_pull', 'source_table_name', 'source_schema_name', 'connection_name', 'target_s3_location'])
# Parameter validation
source_db_type = args['source_db_type']
logger.info("Source DB Type: " + source_db_type)
if source_db_type != 'mysql' and source_db_type != 'postgres':
    error_msg = "source_db_type can either be mysql or postgres"
    log_error_and_exit(logger, error_msg)

type_of_pull = args['type_of_pull']
logger.info("Type of Pull: " + type_of_pull)
if type_of_pull != 'full' and type_of_pull != 'incremental':
    error_msg = "type_of_pull can either be full or incremental"
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

target_s3_location = args['target_s3_location']
logger.info("Target S3 Location: " + target_s3_location)
if not(target_s3_location and target_s3_location.strip()):
    error_msg = "target_s3_location cannot be an empty string or spaces"
    log_error_and_exit(logger, error_msg)

connection_name = args['connection_name']
logger.info("Connection Name: " + connection_name)
if not(connection_name and connection_name.strip()):
    error_msg = "connection_name cannot be an empty string or spaces"
    log_error_and_exit(logger, error_msg)


if type_of_pull == 'incremental':
    logger.info("Incremental Table pull")
    # Get additional Parameter values
    args = getResolvedOptions(sys.argv, ['workflow_name', 'bookmark_key', 'bookmark_key_order', 'dynamodb_control_table_name'])
    # Parameter validation

    workflow_name = args['workflow_name']
    logger.info("Workflow name: " + workflow_name)
    if not(workflow_name and workflow_name.strip()):
        error_msg = "workflow_name cannot be an empty string or spaces for incremental loads"
        log_error_and_exit(logger, error_msg)

    bookmark_key = args['bookmark_key']
    logger.info("Bookmark key: " + bookmark_key)
    if not(bookmark_key and bookmark_key.strip()):
        error_msg = "bookmark_key cannot be an empty string or spaces for incremental loads"
        log_error_and_exit(logger, error_msg)
    bookmark_key = bookmark_key.replace(" ", "")
    bookmark_key_list = bookmark_key.split(",")

    bookmark_key_order = args['bookmark_key_order']
    logger.info("Bookmark Key Order: " + bookmark_key_order)
    if bookmark_key_order != 'asc' and bookmark_key_order != 'desc':
        error_msg = "bookmark_key_order can either be asc or desc"
        log_error_and_exit(logger, error_msg)

    dynamodb_control_table_name = args['dynamodb_control_table_name']
    logger.info("DynamoDB Control Table Name: " + dynamodb_control_table_name)
    if not(dynamodb_control_table_name and dynamodb_control_table_name.strip()):
        error_msg = "dynamodb_control_table_name cannot be an empty string or spaces for incremental loads"
        log_error_and_exit(logger, error_msg)

    dynamodb_key = workflow_name + "|" + source_schema_name + "|" + source_table_name
    try:
        ddb_client = boto3.client('dynamodb')
        response = ddb_client.get_item(
            Key={
                'identifier': {
                    'S': dynamodb_key,
                }
            },
            ProjectionExpression="bookmarked_value",
            TableName=dynamodb_control_table_name
        )
        if 'Item' in response:
            bookmarked_value = response['Item']['bookmarked_value']['S']
            logger.info("bookmarked_value : " + bookmarked_value)
            incremental_pull = True
        else:
            incremental_pull = False
            logger.info("First incremental pull for this table. Hence, this will be a full pull")
    except:
            error_msg = "Error reading DynamoDB control Table"
            log_error_and_exit(logger, error_msg)

    if incremental_pull:
        if bookmark_key_order == 'asc':
            querytxt = "select * from " + source_schema_name + "." + source_table_name + " where (" + bookmark_key + ") > (" + bookmarked_value + ")"
        else:
            querytxt = "select * from " + source_schema_name + "." + source_table_name + " where (" + bookmark_key + ") < (" + bookmarked_value + ")"
    else:
        querytxt = "select * from " + source_schema_name + "." + source_table_name
else:
    logger.info("Full table pull")
    querytxt = "select * from " + source_schema_name + "." + source_table_name
logger.info("Query on Source Database : " + querytxt)
try:
    if source_db_type == 'mysql':
        con = wr.mysql.connect(connection_name)
        df = wr.mysql.read_sql_query(sql=querytxt, con=con)
    elif source_db_type == 'postgres':
        con = wr.postgresql.connect(connection_name)
        df = wr.postgresql.read_sql_query(sql=querytxt, con=con)
    else:
        error_msg = "Unsupported Source Database Type"
        log_error_and_exit(logger, error_msg)

    logger.info("Number of records pulled from the source :" + str(len(df.index)))
    con.close()
except:
    error_msg = "Unable to run the query on the source database"
    log_error_and_exit(logger, error_msg)

if len(df.index) > 0:
    target_s3_location = target_s3_location + source_schema_name + "/" + source_table_name + "/data_" + dt_string + ".parquet"
    logger.info("Writing data to the following S3 location : " + target_s3_location)
    try:
        wr.s3.to_parquet(df, target_s3_location, index=False, compression="snappy")
    except:
        error_msg = "Error writing to S3 - Please check if the S3 bucket exists and the IAM role for the job has permissions to write to the S3 bucket"
        log_error_and_exit(logger, error_msg)
    if type_of_pull == 'incremental':
        df_bookmark_column = df[bookmark_key_list]
        df_bookmark_column = df_bookmark_column.astype(str)
        df_bookmark_column.update(df_bookmark_column[bookmark_key_list].applymap('\'{}\''.format))
        df_bookmark_column['concat_val'] = df_bookmark_column.apply(lambda row: row.dropna().tolist(), axis=1)

        if bookmark_key_order == 'asc':
            min_max_values = max(df_bookmark_column['concat_val'])
        else:
            min_max_values = min(df_bookmark_column['concat_val'])
        updated_bookmark_value = ','.join(map(str, min_max_values))
        logger.info("Updated Bookmark Value: " + updated_bookmark_value)
        try:
            response = ddb_client.put_item(TableName=dynamodb_control_table_name, Item={'identifier': {'S': dynamodb_key}, 'bookmarked_value': {'S': updated_bookmark_value}})
        except:
            error_msg = "Error updating the bookmark in the DynamoDB Table. Data to S3 is however written. Please clean the data in S3 manually"
            log_error_and_exit(logger, error_msg)

else:
    logger.info("No incremental data to pull from the source table")
logger.info("Job Ended Successfully")
