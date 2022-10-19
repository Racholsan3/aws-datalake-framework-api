# Currently, the values are hardcoded
import json
import pandas as pd
import boto3
import io
import psycopg2
import pyarrow as pa
from pyarrow import csv, parquet, orc
from pyarrow import json as js
from io import StringIO
from connector import Connector

# pyarrow_to_spark_mapper
pyarrow_to_spark_mapper = {
    "int32": "IntegerType",
    "int64": "LongType",
    "string": "StringType",
    "double": "DoubleType",
    "bool": "BooleanType",
    "timestamp[ns]": "TimestampType",
    "date32[day]": "DateType",
    "null": "NullType"

}


def lambda_handler(event, context):
    client = boto3.client('s3')
    response = client.get_object(
        Bucket='2194-datastore',
        Key='part-00000-e4890e43-d85f-4351-b5ae-fc9ec834ba51-c000.snappy.orc'
    )

    ############### Parquet ###############

    file = io.BytesIO(response['Body'].read())
    df = parquet.read_table(file)
    names_list = df.schema.names
    types_list = df.schema.types
    output_dict = dict()
    for name, types in zip(names_list, types_list):
        output_dict[name] = pyarrow_to_spark_mapper[str(types)]
    print(output_dict)

    ############### CSV ###############

    file = io.BytesIO(response['Body'].read())
    df = csv.read_csv(file)
    names_list = df.schema.names
    types_list = df.schema.types
    output_dict = dict()
    for name, types in zip(names_list, types_list):
        output_dict[name] = pyarrow_to_spark_mapper[str(types)]
    print(output_dict)

    ############### JSON ###############

    file = str(response['Body'].read(), 'utf-8')
    file_pr = file.replace("},{", "}\n{")
    file_pr = file_pr[1:-1]
    file_bytes = str.encode(file_pr)
    file1 = io.BytesIO(file_bytes)
    df = js.read_json(file1)
    names_list = df.schema.names
    types_list = df.schema.types
    output_dict = dict()
    for name, types in zip(names_list, types_list):
        output_dict[name] = pyarrow_to_spark_mapper[str(types)]
    print(output_dict)

    ############### ORC ###############

    file = io.BytesIO(response['Body'].read())
    df = orc.read_table(file)
    names_list = df.schema.names
    types_list = df.schema.types
    output_dict = dict()
    for name, types in zip(names_list, types_list):
        output_dict[name] = pyarrow_to_spark_mapper[str(types)]
    print(output_dict)

    ############### Postgres ###############

    conn = Connector("dl-fmwrk-metadata-dev", "us-east-2", schema="test")
    host = conn.credentials['host']
    port = conn.credentials['port']
    dbname = conn.credentials['dbname']
    username = conn.credentials['username']
    pwd = conn.credentials['password']
    with psycopg2.connect(
            '''host='{}' port={} dbname='{}' user={} password={}'''.format(host, port, dbname, username,
                                                                           pwd)) as connection:
        sql = "SELECT * FROM test.extraction_test;"
        df = pd.read_sql(sql, connection)
        table = pa.Table.from_pandas(df)
        names_list = table.schema.names
        types_list = table.schema.types
        output_dict = dict()
        for name, types in zip(names_list, types_list):
            output_dict[name] = pyarrow_to_spark_mapper[str(types)]
        print(output_dict)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
