# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from awsglue.blueprint.workflow import Workflow, Entities
from awsglue.blueprint.job import Job
from awsglue.blueprint.crawler import *
import boto3
from botocore.client import ClientError
import datetime
import json
import sys
import logging
from logging import getLogger, StreamHandler, INFO


def create_s3_bucket_if_needed(s3_client, bucket_name, region, logger):
    location = {'LocationConstraint': region}
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"S3 Bucket already exists: {bucket_name}")
    except ClientError as ce1:
        if ce1.response['Error']['Code'] == "404":  # bucket not found
            logger.info(f"Creating S3 bucket: {bucket_name}")
            try:
                if system_params['region'] == "us-east-1":
                    bucket = s3_client.create_bucket(Bucket=bucket_name)
                else:
                    bucket = s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
                logger.info(f"Created S3 bucket: {bucket_name}")
            except ClientError as ce2:
                logger.error(f"Unexpected error occurred when creating S3 bucket: {bucket_name}, exception: {ce2}")
                raise
            except:
                logger.error(f"Unexpected error occurred when creating S3 bucket: {bucket_name}")
                raise
        else:
            logger.error(f"Unexpected error occurred when heading S3 bucket: {bucket_name}, exception: {ce1}")
            raise
    except:
        logger.error(f"Unexpected error occurred when heading S3 bucket: {bucket_name}")
        raise


def log_error_and_exit(logger, error_msg):
    logger.error(error_msg)
    raise Exception(error_msg)


def generate_schedule(type):
    now = datetime.datetime.utcnow()
    year = now.year
    number_of_month = now.month
    days = now.day
    hours = now.hour
    minutes = now.minute
    days_of_week = now.weekday()

    if type == 'Hourly':
        return generate_cron_expression(minutes, "0/1", "*", "*", "?", "*")
    elif type == 'Daily':
        return generate_cron_expression(minutes, hours, "*", "*", "?", "*")
    elif type == 'Weekly':
        return generate_cron_expression(minutes, hours, "?", "*", days_of_week, "*")
    elif type == 'Monthly':
        return generate_cron_expression(minutes, hours, days, "*", "?", "*")
    else:
        return generate_cron_expression(minutes, hours, days, number_of_month, "?", year)


def generate_cron_expression(minutes, hours, days, number_of_month, days_of_week, year):
    return "cron({0} {1} {2} {3} {4} {5})".format(minutes, hours, days, number_of_month, days_of_week, year)


def read_validate_parameter(parameter_key, input_dict, error_string, result, valid_values, parameter_type, parameter_data_type):
    try:
        parameter_value = input_dict[parameter_key]
    except:
        if parameter_data_type == 'numeric':
            parameter_value = '0'
        else:
            parameter_value = ''
    if valid_values:
        if parameter_value in valid_values:
            error_val = 0
            error_string = error_string
        else:
            error_val = 1
            error_string = error_string + parameter_key + ' value is not valid and should be one of ' + str(valid_values) + ', '
    else:
        if parameter_data_type == 'numeric':
            try:
                parameter_value = float(parameter_value)
                if parameter_value <= 0.0:
                    error_val = 1
                    error_string = error_string + parameter_key + ' should be numeric. Value should be 0.0625 or a positive integer for python-shell components and an integer greater than 2 for pyspark components, '
                else:
                    error_val = 0
                    error_string = error_string
            except:
                error_val = 1
                error_string = error_string + parameter_key + ' should be numeric. Value should be 0.0625 or a positive integer for python-shell components and an integer greater than 2 for pyspark components, '
        else:
            if parameter_type == 'mandatory':
                if parameter_value.strip():
                    error_val = 0
                    error_string = error_string
                else:
                    error_val = 1
                    error_string = error_string + parameter_key + ' is blanks or null, '
            else:
                error_val = 0
                error_string = error_string
        if result == 1:
            error_val = 1
    return(parameter_value, error_val, error_string)


def generate_layout(user_params, system_params):
    # Set Logger properties
    logger = getLogger(__name__)
    handler = StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False

    # Validate Blueprint level parameters
    val_result = 0
    error_string = ''
    validation_results_s3_location, val_result, error_string = read_validate_parameter('ValidationResultsS3Location', user_params, error_string, val_result, [], 'mandatory', 'string')
    if val_result == 1:
        log_error_and_exit(logger, error_string)
    len_val_location = len(validation_results_s3_location)
    val_bucket_name = validation_results_s3_location[5:validation_results_s3_location.find("/", 5)]
    val_key_name = validation_results_s3_location[validation_results_s3_location.find("/", 5) + 1:]
    f = open("Blueprint_parameter_validation.csv", "w")
    f.write("source_schema_name|source_table_name|Errors\n")
    session = boto3.Session(region_name=system_params['region'])
    s3_client = session.client('s3')
    workflow_name, val_result, error_string = read_validate_parameter('WorkflowName', user_params, error_string, val_result, [], 'mandatory', 'string')
    dynamodb_control_table_name = 'rel_db_to_s3_control_table'
    iam_role, val_result, error_string = read_validate_parameter('GlueExecutionRole', user_params, error_string, val_result, [], 'mandatory', 'string')
    connection_name, val_result, error_string = read_validate_parameter('GlueConnectionName', user_params, error_string, val_result, [], 'mandatory', 'string')
    raw_s3_location, val_result, error_string = read_validate_parameter('RawS3Location', user_params, error_string, val_result, [], 'mandatory', 'string')
    curated_s3_location, val_result, error_string = read_validate_parameter('CuratedS3Location', user_params, error_string, val_result, [], 'mandatory', 'string')
    parameter_file_location, val_result, error_string = read_validate_parameter('ParameterFileLocation', user_params, error_string, val_result, [], 'mandatory', 'string')
    aws_wrangler_whl_file_location, val_result, error_string = read_validate_parameter('AWSWranglerWheelFileLocation', user_params, error_string, val_result, [], 'optional', 'string')
    # blueprint_val_result value is set to 1 if there is an error in any of the Blueprint parameters or any of the parameters
    # in the JSON file
    if val_result == 1:
        f.write("NA|NA|" + error_string + "\n")
        blueprint_val_result = 1
    else:
        blueprint_val_result = 0
    if not(parameter_file_location and parameter_file_location.strip()):
        # If there is no valid parameter file location, there is nothing more to validate. So, upload the validation file to S3 and exit
        s3_client.upload_file("Blueprint_parameter_validation.csv", val_bucket_name, val_key_name + "Blueprint_parameter_validation.csv")
        log_error_and_exit(logger, "Invalid Parameter file location")
    len_parameter_file_location = len(parameter_file_location)
    bucket_name = parameter_file_location[5:parameter_file_location.find("/", 5)]
    key_name = parameter_file_location[parameter_file_location.find("/", 5) + 1:]
    with open('parameters.json', 'wb') as data:
        s3_client.download_fileobj(bucket_name, key_name, data)
    # Now validate the parameter file
    f1 = open('parameters.json',)
    data = json.load(f1)
    for parameters in data['job_parameters']:
        error_string = ''
        # val_result is set to 0 for each row initially and then set to 1 if there is an issue. If the val_result value
        # is 1 after parameter validation for a particular row, blueprint_val_result is set to 1 at the end.
        val_result = 0
        source_raw_job_type, val_result, error_string = read_validate_parameter('source_raw_job_type', parameters, error_string, val_result, ['python-shell', 'pyspark'], 'mandatory', 'string')
        type_of_pull, val_result, error_string = read_validate_parameter('type_of_pull', parameters, error_string, val_result, ['full', 'incremental'], 'mandatory', 'string')
        source_schema_name, val_result, error_string = read_validate_parameter('source_schema_name', parameters, error_string, val_result, [], 'mandatory', 'string')
        source_table_name, val_result, error_string = read_validate_parameter('source_table_name', parameters, error_string, val_result, [], 'mandatory', 'string')
        nature_of_data, val_result, error_string = read_validate_parameter('nature_of_data', parameters, error_string, val_result, ['mutable', 'immutable'], 'mandatory', 'string')
        target_db_name, val_result, error_string = read_validate_parameter('target_db_name', parameters, error_string, val_result, [], 'mandatory', 'string')
        if target_db_name and target_db_name.strip():
            glue_client = boto3.client('glue')
            try:
                glue_response = glue_client.get_database(Name=target_db_name)
            except glue_client.exceptions.EntityNotFoundException:
                logger.info("Curated DB in the Glue catalog does not exist. Creating one.")
                try:
                    glue_response = glue_client.create_database(DatabaseInput={'Name': target_db_name})
                except:
                    logger.info("Error creating Curated DB in Glue catalog")
                    log_error_and_exit(logger, "Error creating Curated DB in Glue catalog")
            except:
                logger.info("Error checking if Curated DB exists in Glue catalog")
                log_error_and_exit(logger, "Error checking if Curated DB exists in Glue catalog")
        target_partition_columns, val_result, error_string = read_validate_parameter('target_partition_columns', parameters, error_string, val_result, [], 'mandatory', 'string')
        compute_capacity_source_raw, val_result, error_string = read_validate_parameter('compute_capacity_source_raw', parameters, error_string, val_result, [], 'mandatory', 'numeric')
        compute_capacity_raw_curated, val_result, error_string = read_validate_parameter('compute_capacity_raw_curated', parameters, error_string, val_result, [], 'mandatory', 'numeric')
        raw_curated_worker_type, val_result, error_string = read_validate_parameter('raw_curated_worker_type', parameters, error_string, val_result, ['Standard', 'G.1X', 'G.2X'], 'mandatory', 'string')
        # Now the optional parameters
        bookmark_key, val_result, error_string = read_validate_parameter('bookmark_key', parameters, error_string, val_result, [], 'optional', 'string')
        source_db_type, val_result, error_string = read_validate_parameter('source_db_type', parameters, error_string, val_result, [], 'optional', 'string')
        parallel_pull, val_result, error_string = read_validate_parameter('parallel_pull', parameters, error_string, val_result, [], 'optional', 'string')
        glue_catalog_db_name, val_result, error_string = read_validate_parameter('glue_catalog_db_name', parameters, error_string, val_result, [], 'optional', 'string')
        glue_catalog_table_name, val_result, error_string = read_validate_parameter('glue_catalog_table_name', parameters, error_string, val_result, [], 'optional', 'string')
        hashfield, val_result, error_string = read_validate_parameter('hashfield', parameters, error_string, val_result, [], 'optional', 'string')
        hashpartitions, val_result, error_string = read_validate_parameter('hashpartitions', parameters, error_string, val_result, [], 'optional', 'string')
        bookmark_key_order, val_result, error_string = read_validate_parameter('bookmark_key_order', parameters, error_string, val_result, [], 'optional', 'string')
        primary_key, val_result, error_string = read_validate_parameter('primary_key', parameters, error_string, val_result, [], 'optional', 'string')
        source_raw_worker_type, val_result, error_string = read_validate_parameter('source_raw_worker_type', parameters, error_string, val_result, [], 'optional', 'string')

        if source_raw_job_type == 'python-shell' and not aws_wrangler_whl_file_location.strip():
            val_result = 1
            error_string = error_string + 'AWS Wrangler whl file location needs to be specified when spurce_raw_job_type is python_shell, '
        if source_raw_job_type == 'python-shell' and source_db_type != 'mysql' and source_db_type != 'postgres':
            val_result = 1
            error_string = error_string + 'source_db_type should either be mysql or postgres when source_raw_job_type is python-shell, '

        if source_raw_job_type == 'pyspark' and parallel_pull != 'true' and parallel_pull != 'false':
            val_result = 1
            error_string = error_string + 'parallel_pull needs to be set to true or false when source_raw_job_type is pyspark, '

        if source_raw_job_type == 'pyspark' and not glue_catalog_db_name.strip():
            val_result = 1
            error_string = error_string + 'glue_catalog_db_name needs to be set when source_raw_job_type is pyspark, '

        if source_raw_job_type == 'pyspark' and not glue_catalog_table_name.strip():
            val_result = 1
            error_string = error_string + 'glue_catalog_table_name needs to be set when source_raw_job_type is pyspark, '

        if source_raw_job_type == 'pyspark' and parallel_pull == 'true' and not(hashfield.strip() and hashpartitions.strip()):
            val_result = 1
            error_string = error_string + 'hashfield and  hashpartitions needs to be set when source_raw_job_type is pyspark and parallel_pull is true, '

        if type_of_pull == 'incremental' and not(bookmark_key.strip() and (bookmark_key_order == 'asc' or bookmark_key_order == 'desc')):
            val_result = 1
            error_string = error_string + 'bookmark_key needs to be set and bookmark_key_order needs to be asc or desc when type_of_pull is incremental, '

        if nature_of_data == 'mutable' and type_of_pull == 'incremental' and not primary_key.strip():
            val_result = 1
            error_string = error_string + 'primary_key need to be provided when nature_of_data is mutable and type_of_pull is incremental, '
        if val_result == 1:
            f.write(source_schema_name + '|' + source_table_name + '|' + error_string + '\n')
            blueprint_val_result = 1
    f.close()
    # if blueprint_val_result is 1, it means there is an issue with one or more of the blueprint or JSON parameters
    # In that case, upload the validation file to S3.
    if blueprint_val_result == 1:
        s3_client.upload_file("Blueprint_parameter_validation.csv", val_bucket_name, val_key_name + "Blueprint_parameter_validation.csv")
        log_error_and_exit(logger, "Issues with provided parameter values. Please check the Blueprint_parameter_validation.csv file in S3 bucket")
    # Validation ends here
    # Check if DynamoDB Table needs to be created and create one if missing
    ddb_client = boto3.client('dynamodb')
    ddb_table_exists = True
    try:
        response = ddb_client.describe_table(TableName=dynamodb_control_table_name)
    except ddb_client.exceptions.ResourceNotFoundException:
        ddb_table_exists = False
    except:
        log_error_and_exit(logger, "Issues checking if DynamoDB Table exists.")
    f = open('parameters.json',)
    par_values = f.read()
    if par_values.find("\"python-shell\"", 0) > 0:
        python_shell_job_exists = True
    else:
        python_shell_job_exists = False

    if python_shell_job_exists and not ddb_table_exists:
        logger.info("Creating DynamoDB Table.")
        try:
            response = ddb_client.create_table(
                TableName=dynamodb_control_table_name,
                KeySchema=[
                    {
                        'AttributeName': 'identifier',
                        'KeyType': 'HASH'  # Partition key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'identifier',
                        'AttributeType': 'S'
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
        except:

            log_error_and_exit(logger, "Error Creating DynamoDB Table")

    f.close()
    
    # Creating script bucket
    the_script_bucket = f"aws-glue-scripts-{system_params['accountId']}-{system_params['region']}"
    create_s3_bucket_if_needed(s3_client, the_script_bucket, system_params['region'], logger)

    # Creating temp bucket
    the_temp_bucket = f"aws-glue-temporary-{system_params['accountId']}-{system_params['region']}"
    create_s3_bucket_if_needed(s3_client, the_temp_bucket, system_params['region'], logger)
    the_temp_prefix = f"{workflow_name}/"
    the_temp_location = f"s3://{the_temp_bucket}/{the_temp_prefix}"

    # Upload job scripts to script bucket

    file_name = "raw-curated-pyspark.py"
    the_script_key = f"{workflow_name}/{file_name}"
    the_script_location = f"s3://{the_script_bucket}/{the_script_key}"
    with open("rel_db_to_s3/" + file_name, "rb") as f:
        s3_client.upload_fileobj(f, the_script_bucket, the_script_key)

    file_name = "source-raw-pyspark.py"
    the_script_key = f"{workflow_name}/{file_name}"
    the_script_location = f"s3://{the_script_bucket}/{the_script_key}"
    with open("rel_db_to_s3/" + file_name, "rb") as f:
        s3_client.upload_fileobj(f, the_script_bucket, the_script_key)

    file_name = "source-raw-python-shell.py"
    the_script_key = f"{workflow_name}/{file_name}"
    the_script_location = f"s3://{the_script_bucket}/{the_script_key}"
    with open("rel_db_to_s3/" + file_name, "rb") as f:
        s3_client.upload_fileobj(f, the_script_bucket, the_script_key)

    jobs = []
    f = open('parameters.json',)
    data = json.load(f)
    for parameters in data['job_parameters']:
        # First Read all mandatory parameters
        source_raw_job_type = parameters['source_raw_job_type']
        type_of_pull = parameters['type_of_pull']
        source_schema_name = parameters['source_schema_name']
        source_table_name = parameters['source_table_name']
        nature_of_data = parameters['nature_of_data']
        target_db_name = parameters['target_db_name']
        target_partition_columns = parameters['target_partition_columns']
        compute_capacity_source_raw = parameters['compute_capacity_source_raw']
        compute_capacity_raw_curated = parameters['compute_capacity_raw_curated']
        raw_curated_worker_type = parameters['raw_curated_worker_type']
        # Now the optional parameters
        try:
            bookmark_key = parameters['bookmark_key']
        except:
            bookmark_key = ''

        try:
            source_db_type = parameters['source_db_type']
        except:
            source_db_type = ''

        try:
            parallel_pull = parameters['parallel_pull']
        except:
            parallel_pull = ''

        try:
            glue_catalog_db_name = parameters['glue_catalog_db_name']
        except:
            glue_catalog_db_name = ''

        try:
            glue_catalog_table_name = parameters['glue_catalog_table_name']
        except:
            glue_catalog_table_name = ''

        try:
            hashfield = parameters['hashfield']
        except:
            hashfield = ''

        try:
            hashpartitions = parameters['hashpartitions']
        except:
            hashpartitions = ''

        try:
            bookmark_key_order = parameters['bookmark_key_order']
        except:
            bookmark_key_order = ''

        try:
            primary_key = parameters['primary_key']
        except:
            primary_key = ''

        try:
            source_raw_worker_type = parameters['source_raw_worker_type']
        except:
            source_raw_worker_type = ''

        # If source to raw job type is python-shell
        if source_raw_job_type == 'python-shell':
            file_name = "source-raw-python-shell.py"
            the_script_key = f"{workflow_name}/{file_name}"
            the_script_location = f"s3://{the_script_bucket}/{the_script_key}"
            command = {
                "Name": "pythonshell",
                "ScriptLocation": the_script_location,
                "PythonVersion": "3"
            }
            arguments = {
                "--job-bookmark-option": "job-bookmark-disable",
                "--source_db_type": source_db_type,
                "--type_of_pull": type_of_pull,
                "--source_table_name": source_table_name,
                "--source_schema_name": source_schema_name,
                "--connection_name": connection_name,
                "--target_s3_location": raw_s3_location,
                "--bookmark_key": bookmark_key,
                "--bookmark_key_order": bookmark_key_order,
                "--dynamodb_control_table_name": dynamodb_control_table_name,
                "--extra-py-files": aws_wrangler_whl_file_location,
                "--workflow_name": workflow_name
            }

            source_raw_job = Job(
                Name=workflow_name + "_source_raw_" + source_schema_name + "_" + source_table_name,
                Command=command,
                Role=iam_role,
                DefaultArguments=arguments,
                GlueVersion="1.0",
                MaxCapacity=float(compute_capacity_source_raw),
                Connections={"Connections": [connection_name]}
            )

        else:
            file_name = "source-raw-pyspark.py"
            the_script_key = f"{workflow_name}/{file_name}"
            the_script_location = f"s3://{the_script_bucket}/{the_script_key}"
            command = {
                "Name": "glueetl",
                "ScriptLocation": the_script_location,
                "PythonVersion": "3"
            }
            arguments = {
                "--job-bookmark-option": "job-bookmark-enable",
                "--parallel_pull": parallel_pull,
                "--glue_catalog_db_name": glue_catalog_db_name,
                "--glue_catalog_table_name": glue_catalog_table_name,
                "--hashfield": hashfield,
                "--hashpartitions": hashpartitions,
                "--type_of_pull": type_of_pull,
                "--source_table_name": source_table_name,
                "--source_schema_name": source_schema_name,
                "--connection_name": connection_name,
                "--target_s3_location": raw_s3_location,
                "--bookmark_key": bookmark_key,
                "--bookmark_key_order": bookmark_key_order,
                "--TempDir": the_temp_location,
                "--enable-continuous-cloudwatch-log": "true"
            }

            source_raw_job = Job(
                Name=workflow_name + "_source_raw_" + source_schema_name + "_" + source_table_name,
                Command=command,
                Role=iam_role,
                DefaultArguments=arguments,
                GlueVersion="2.0",
                WorkerType=source_raw_worker_type,
                NumberOfWorkers=int(compute_capacity_source_raw),
                Connections={"Connections": [connection_name]}
            )

        jobs.append(source_raw_job)

        # Now Add the Raw to Curated Job
        file_name = "raw-curated-pyspark.py"
        the_script_key = f"{workflow_name}/{file_name}"
        the_script_location = f"s3://{the_script_bucket}/{the_script_key}"
        command = {
            "Name": "glueetl",
            "ScriptLocation": the_script_location,
            "PythonVersion": "3"
        }
        arguments = {
            "--job-bookmark-option": "job-bookmark-enable",
            "--enable-glue-datacatalog": "",
            "--nature_of_data": nature_of_data,
            "--target_db_name": target_db_name,
            "--target_partition_columns": target_partition_columns,
            "--source_s3_location": raw_s3_location,
            "--type_of_pull": type_of_pull,
            "--source_table_name": source_table_name,
            "--source_schema_name": source_schema_name,
            "--target_s3_location": curated_s3_location,
            "--primary_key": primary_key,
            "--TempDir": the_temp_location,
            "--enable-continuous-cloudwatch-log": "true"
        }
        raw_curated_job = Job(
            Name=workflow_name + "_raw_curated_" + source_schema_name + "_" + source_table_name,
            Command=command,
            Role=iam_role,
            DefaultArguments=arguments,
            GlueVersion="2.0",
            WorkerType=raw_curated_worker_type,
            NumberOfWorkers=int(compute_capacity_raw_curated),
            DependsOn={source_raw_job: "SUCCEEDED"}
        )
        jobs.append(raw_curated_job)
        # For Loop Ends Here
    try:
        if user_params['Frequency']:
            if user_params['Frequency'] == 'Custom':
                schedule = user_params['FrequencyCronFormat']
            else:
                schedule = generate_schedule(user_params['Frequency'])
        else:
            schedule = None
    except:
        schedule = None
    workflow = Workflow(Name=workflow_name, Entities=Entities(Jobs=jobs), OnSchedule=schedule)
    return workflow
