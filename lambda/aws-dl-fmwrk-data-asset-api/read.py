from utils import *


def read_asset(event, context, database):
    message_body = event["body-json"]
    api_call_type = "synchronous"

    # API logic here
    # -----------

    asset_columns = [
        "asset_id",
        "src_sys_id",
        "target_id",
        "file_header",
        "multipartition",
        "file_type",
        "asset_nm",
        "source_path",
        "target_path",
        "trigger_file_pattern",
        "file_delim",
        "file_encryption_ind",
        "athena_table_name",
        "asset_owner",
        "support_cntct",
        "rs_load_ind",
        "rs_stg_table_nm"
    ]
    attributes_columns = [
        "col_id",
        "asset_id",
        "col_nm",
        "col_desc",
        "data_classification",
        "col_length",
        "req_tokenization",
        "pk_ind",
        "null_ind",
        "data_type",
        "tgt_col_nm",
        "tgt_data_type"
    ]
    ingestion_columns = [
        "asset_id",
        "src_sys_id",
        "src_table_name",
        "src_sql_query",
        "ingstn_src_path",
        "trigger_mechanism",
        "frequency"
    ]
    dq_columns = [
        'dq_rule_id',
        'asset_id',
        'dq_rule'
    ]
    # Getting the asset id and source system id
    asset_id = message_body["asset_id"]
    src_sys_id = message_body["src_sys_id"]
    # Where clause
    where_clause = ("asset_id=%s", [asset_id])

    try:
        dict_asset = database.retrieve_dict(
            table="data_asset",
            cols=asset_columns,
            where=where_clause
        )[0]
        if dict_asset:
            dict_attributes = database.retrieve_dict(
                table="data_asset_attributes",
                cols=attributes_columns,
                where=where_clause
            )
            dict_ingestion = database.retrieve_dict(
                table="data_asset_ingstn_atrbts",
                cols=ingestion_columns,
                where=(
                    "asset_id=%s and src_sys_id=%s",
                    [asset_id, src_sys_id]
                )
            )[0]
            dict_dq = database.retrieve_dict(
                table="adv_dq_rules",
                cols=dq_columns,
                where=("asset_id=%s", [asset_id])
            )
            status_code = 202
            status = True
            body = {
                "asset_info": dict_asset,
                "asset_attributes": dict_attributes,
                "ingestion_attributes": dict_ingestion,
                "adv_dq_rules": dict_dq
            }
        else:
            status = False
            status_code = 402
            body = {}
        database.close()

    except Exception as e:
        database.close()
        print(e)
        status = False
        status_code = 402
        body = str(e)
        message_body = event["body-json"]

    # -----------
    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    return{
        "statusCode": status_code,
        "status": status,
        "sourceCodeDynamoDb": response["statusCode"],
        "body": body,
        "payload": message_body
    }
