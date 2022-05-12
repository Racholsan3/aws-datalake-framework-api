from genericpath import exists
from sqlite3 import connect
import boto3
import os
import json
from datetime import datetime
from connector import Connector


def getGlobalParams():
    with open('globalConfig.json', "r") as json_file:
        json_config = json.load(json_file)
        return json_config


def get_database():
    db_secret = os.environ['secret_name']
    db_region = os.environ['secret_region']
    conn = Connector(secret=db_secret, region=db_region, autocommit=False)
    return conn


def create_src_s3_dir_str(asset_id, message_body, config):

    region = config["primary_region"]
    src_sys_id = message_body["asset_info"]["src_sys_id"]
    bucket_name = f"{config['fm_prefix']}-{str(src_sys_id)}-{region}"
    print(
        "Creating directory structure in {} bucket".format(bucket_name)
    )
    os.system(
        'aws s3api put-object --bucket "{}" --key "{}/init/dummy"'.format(
            bucket_name, asset_id
        )
    )
    os.system(
        'aws s3api put-object --bucket "{}" --key "{}/error/dummy"'.format(
            bucket_name, asset_id
        )
    )
    os.system(
        'aws s3api put-object --bucket "{}" --key "{}/masked/dummy"'.format(
            bucket_name, asset_id
        )
    )
    os.system(
        'aws s3api put-object --bucket "{}" --key "{}/error/dummy"'.format(
            bucket_name, asset_id
        )
    )
    os.system(
        'aws s3api put-object --bucket "{}" --key "{}/logs/dummy"'.format(
            bucket_name, asset_id
        )
    )


def set_bucket_event_notification(asset_id, message_body, config):
    """
    Utility method to set the bucket event notification by getting prior event settings
    :param asset_id:
    :param asset_json_file: A json formatted document that contains details about the asset
    :param region:
    :return:
    """

    region = config["primary_region"]
    src_sys_id = message_body["asset_info"]["src_sys_id"]

    bucket_name = (
        config["fm_prefix"]
        + "-"
        + str(src_sys_id)
        + "-"
        + region
    )
    key_prefix = str(asset_id) + "/init/"
    if message_body["asset_info"]["multipartition"] == "false":
        key_suffix = message_body["asset_info"]["file_type"]
    else:
        key_suffix = message_body["asset_info"]["trigger_file_pattern"]
    s3_event_name = str(asset_id) + "-createObject"
    sns_name = (
        config["fm_prefix"]
        + "-"
        + str(src_sys_id)
        + "-init-file-creation"
    )
    sns_arn = (
        "arn:aws:sns:"
        + region
        + ":"
        + os.environ['aws_account']
        + ":"
        + sns_name
    )
    s3Client = boto3.client("s3")
    print(
        "Creating putObject event notification to {} bucket".format(
            bucket_name
        )
    )
    new_config = {
        "Id": s3_event_name,
        "TopicArn": sns_arn,
        "Events": ["s3:ObjectCreated:*"],
        "Filter": {
            "Key": {
                "FilterRules": [
                    {"Name": "prefix", "Value": key_prefix},
                    {"Name": "suffix", "Value": key_suffix},
                ]
            }
        },
    }
    response = s3Client.get_bucket_notification_configuration(
        Bucket=bucket_name
    )
    if "TopicConfigurations" not in response.keys():
        # If the bucket doesn't have event notification configured
        # Attach a new Topic Config
        s3Client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                "TopicConfigurations": [new_config]
            },
        )
    else:
        # The bucket has event notif configured.
        # Get the initial configs and append the new config
        bucket_config = response["TopicConfigurations"]
        bucket_config.append(new_config)
        s3Client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                "TopicConfigurations": bucket_config
            },
        )


def insert_event_to_dynamoDb(event, context, api_call_type, status="success", op_type="insert"):
    cur_time = datetime.now()
    aws_request_id = context.aws_request_id
    log_group_name = context.log_group_name
    log_stream_name = context.log_stream_name
    function_name = context.function_name
    method_name = event["context"]["resource-path"]
    query_string = event["params"]["querystring"]
    payload = event["body-json"]

    client = boto3.resource("dynamodb")
    table = client.Table("aws-dl-fmwrk-api-events")

    if op_type == "insert":
        response = table.put_item(
            Item={
                "aws_request_id": aws_request_id,
                "method_name": method_name,
                "log_group_name": log_group_name,
                "log_stream_name": log_stream_name,
                "function_name": function_name,
                "query_string": query_string,
                "payload": payload,
                "api_call_type": api_call_type,
                "modified ts": str(cur_time),
                "status": status,
            })
    else:
        response = table.update_item(
            Key={
                'aws_request_id': aws_request_id,
                'method_name': method_name,
            },
            ConditionExpression="attribute_exists(aws_request_id)",
            UpdateExpression='SET status = :val1',
            ExpressionAttributeValues={
                ':val1': status,
            }
        )

    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        body = "Insert/Update of the event with aws_request_id=" + \
            aws_request_id + " completed successfully"
    else:
        body = "Insert/Update of the event with aws_request_id=" + aws_request_id + " failed"

    return {
        "statusCode": response["ResponseMetadata"]["HTTPStatusCode"],
        "body": body,
    }


def create_asset(event, context, config, database):
    message_body = event["body-json"]
    api_call_type = "synchronous"

    # API logic here
    # -----------
    asset_id = message_body["asset_id"]
    data_dataAsset = message_body["asset_info"]
    data_dataAssetAttributes = message_body["asset_attributes"]

    colData_dataAsset = {
        "asset_id": asset_id,
        "src_sys_id": data_dataAsset["src_sys_id"],
        "target_id": data_dataAsset["target_id"],
        "file_header": True if data_dataAsset["file_header"] is "true" else False,
        "multipartition": True if data_dataAsset["multipartition"] is "true" else False,
        "file_type": data_dataAsset["file_type"],
        "asset_nm": data_dataAsset["asset_nm"],
        "source_path": data_dataAsset["source_path"],
        "target_path": data_dataAsset["target_path"],
        "file_delim": data_dataAsset["file_delim"],
        "file_encryption_ind": True if data_dataAsset["file_encryption_ind"] is "true" else False,
        "athena_table_name": data_dataAsset["athena_table_name"],
        "asset_owner": data_dataAsset["asset_owner"],
        "support_cntct": data_dataAsset["support_cntct"],
        # "modified_ts": "CURRENT_TIMESTAMP"
    }
    colData_dataAssetAttributes = list(data_dataAssetAttributes.values())
    for i in colData_dataAssetAttributes:
        # i["modified_ts"] = "CURRENT_TIMESTAMP"
        i["asset_id"] = asset_id
        i["req_tokenization"] = True if i["req_tokenization"] is "true" else False
        i["pk_ind"] = True if i["pk_ind"] is "true" else False
        i["null_ind"] = True if i["null_ind"] is "true" else False
        i["tgt_col_nm"] = i["col_nm"]
        i["tgt_data_type"] = i["data_type"]
    try:
        database.insert(
            table="data_asset",
            data=colData_dataAsset
        )
        database.insert_many(
            table="data_asset_attributes",
            data=colData_dataAssetAttributes
        )
        status = "200"
        body = {
            "assetId_inserted": asset_id
        }

    except Exception as e:
        print(e)
        status = "404"
        database.rollback()
        body = {
            "error": f"{e}"
        }

    if status == "200":
        create_src_s3_dir_str(
            asset_id=asset_id, message_body=message_body, config=config)
        set_bucket_event_notification(
            asset_id=asset_id, message_body=message_body, config=config)

    # -----------

    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    return{
        "statusCode": status,
        "sourcePayload": message_body,
        "sourceCodeDynamoDb": response["statusCode"],
        "body": body,
    }


def read_asset(event, context, database):
    message_body = event["body-json"]
    api_call_type = "synchronous"

    # API logic here
    # -----------

    # Getting the column info
    if message_body["asset_info"]["columns"] != "*":
        column_dict = message_body["asset_info"]["columns"]
        assetInfo_columns = list(column_dict.values())
    else:
        assetInfo_columns = message_body["asset_info"]["columns"]
    if message_body["asset_attributes"]["columns"] != "*":
        column_dict = message_body["asset_attributes"]["columns"]
        assetAttributes_columns = list(column_dict.values())
    else:
        assetAttributes_columns = message_body["asset_attributes"]["columns"]
    # Getting the asset id
    asset_id = message_body["asset_id"]
    # Getting the limit
    assetAttributes_limit = int(message_body["asset_attributes"]["limit"])
    # Where clause
    where_clause = ("asset_id=%s", [asset_id])

    try:

        dict_dataAsset = database.retrieve_dict(
            table="data_asset",
            cols=assetInfo_columns,
            where=where_clause
        )
        if dict_dataAsset:
            dict_dataAssetAttributes = database.retrieve_dict(
                table="data_asset_attributes",
                cols=assetAttributes_columns,
                where=where_clause,
                limit=assetAttributes_limit
            )
        status = "200"
        body = {
            "asset_info": dict_dataAsset,
            "asset_attributes": dict_dataAssetAttributes
        }

    except Exception as e:
        print(e)
        status = "404"
        body = {
            "error": f"{e}"
        }

    # -----------
    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    return{
        "statusCode": status,
        "sourcePayload": message_body,
        "sourceCodeDynamoDb": response["statusCode"],
        "body": body
    }


def update_asset(event, context, database):
    message_body = event["body-json"]
    api_call_type = "synchronous"

    # API logic here
    # -----------

    asset_id = message_body["asset_id"]
    data_dataAsset = message_body["asset_info"]
    data_dataAssetAttributes = message_body["asset_attributes"]
    dataAsset_where = ("asset_id=%s", [asset_id])

    try:
        database.update(
            table="data_asset",
            data=data_dataAsset,
            where=dataAsset_where
        )
        for col in data_dataAssetAttributes.keys():
            col_id = data_dataAssetAttributes[col]["column_id"]
            col_data = {
                k: v for k, v in data_dataAssetAttributes[col].items() if k != "column_id"
            }
            dataAssetAttributes_where = (
                "asset_id=%s and col_id=%s", [asset_id, col_id])
            database.update(
                table="data_asset_attributes",
                data=col_data,
                where=dataAssetAttributes_where
            )
        status = "200"
        body = {
            "updated": {
                "dataAsset": data_dataAsset,
                "dataAssetAttributes": data_dataAssetAttributes
            }
        }

    except Exception as e:
        print(e)
        database.rollback()
        status = "404"
        body = {
            "error": f"{e}"
        }

    # -----------

    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    return{
        "statusCode": status,
        "sourcePayload": message_body,
        "sourceCodeDynamoDb": response["statusCode"],
        "body": body
    }


def delete_asset(event, context, database):
    message_body = event["body-json"]
    api_call_type = "synchronous"

    # API logic here
    # -----------

    asset_id = message_body["asset_id"]
    where_clause = ("asset_id=%s", [asset_id])

    try:
        database.delete(
            table="data_asset_attributes",
            where=where_clause
        )
        database.delete(
            table="data_asset",
            where=where_clause
        )
        status = "200"
        body = {
            "deleted_asset": asset_id
        }

    except Exception as e:
        print(e)
        status = "404"
        body = {
            "error": f"{e}"
        }
        database.rollback()

    # -----------

    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    return{
        "statusCode": status,
        "sourcePayload": message_body,
        "sourceCodeDynamoDb": response["statusCode"],
        "body": body
    }


def lambda_handler(event, context):
    resource = event["context"]["resource-path"][1:]
    taskType = resource.split("/")[0]
    method = resource.split("/")[1]

    print(event)
    print(taskType)
    print(method)

    if event:
        if method == "health":
            return {"statusCode": "200", "body": "API Health is good"}

        elif method == "create":
            global_config = getGlobalParams()
            db = get_database()
            response = create_asset(
                event, context, config=global_config, database=db)
            db.close()
            return response

        elif method == "read":
            db = get_database()
            response = read_asset(event, context, database=db)
            db.close()
            return response

        elif method == "update":
            db = get_database()
            response = update_asset(event, context, database=db)
            db.close()
            return response

        elif method == "delete":
            db = get_database()
            response = delete_asset(event, context, database=db)
            db.close()
            return response

        else:
            return {"statusCode": "404", "body": "Not found"}
