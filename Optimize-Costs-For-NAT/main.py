#!/usr/bin/python3

import boto3
import pprint
import time

ec2_client = boto3.client("ec2")
s3_client = boto3.client("s3")
athena_client = boto3.client("athena")

S3_BUCKET_FOR_VPCFLOWLOG = "vpcflowlog-nat"
S3_BUCKET_FOR_ATHENA_RESULTS = "athena-queries-array"
ATHENA_RESULT_OUTPUT_LOCATION = "s3://%s/" % S3_BUCKET_FOR_ATHENA_RESULTS
ATHENA_TIMEOUT = 5 # Times of Exponential Backoff

NAT2AWSSVC = True
AWSSVC2NAT = False

def _get_nat_gateway_enis():
    # {NatGatewayId: {NetworkInterfaceId: eni-xxx, VpcId: vpc-xxx}}
    natGatewayENIs = []
    for natGatewayInfo in ec2_client.describe_nat_gateways()["NatGateways"]:
        if natGatewayInfo["State"] == "available" and natGatewayInfo["ConnectivityType"] == "public":
            natGatewayENIs.append(natGatewayInfo["NatGatewayAddresses"][0]["NetworkInterfaceId"])
    return natGatewayENIs

# idempotent
def create_s3_bucket_for_vpcflowlog():
    # For vpcflowlog
    s3_client.create_bucket(Bucket=S3_BUCKET_FOR_VPCFLOWLOG)
    s3_client.create_bucket(Bucket=S3_BUCKET_FOR_ATHENA_RESULTS)

def create_flowlogs():
    natGatewayENIs = _get_nat_gateway_enis()
    pprint.pprint(natGatewayENIs)

    response = ec2_client.create_flow_logs(
        ResourceType='NetworkInterface',
        ResourceIds=natGatewayENIs,
        #ResourceIds=["eni-0d6f291e4807b81a2"],
        TrafficType='ALL',
        LogDestinationType='s3',
        LogDestination="arn:aws:s3:::%s/" % S3_BUCKET_FOR_VPCFLOWLOG,
        MaxAggregationInterval=60,
        LogFormat='${interface-id} ${srcaddr} ${srcport} ${pkt-src-aws-service} ${dstaddr} ${dstport} ${pkt-dst-aws-service} ${bytes}',
    )
    pprint.pprint(response)

def init_athena():
    DATABASE = "vpcflowlogdb"
    TABLE_DDL = "athena_vpcflowlogtable.ddl"

    # CREATE DATABASE
    response = athena_client.start_query_execution(
        QueryString="create database %s" % DATABASE,
        ResultConfiguration={"OutputLocation": ATHENA_RESULT_OUTPUT_LOCATION}
    )

    # CREATE TABLE
    with open(TABLE_DDL) as ddl:
        response = athena_client.start_query_execution(
            QueryString=ddl.read(),
            ResultConfiguration={"OutputLocation": ATHENA_RESULT_OUTPUT_LOCATION}
        )

def _get_top_flow(flow_direction, sql_path):
    with open(sql_path) as f:
        response = athena_client.start_query_execution(
            QueryString=f.read(),
            ResultConfiguration={"OutputLocation": ATHENA_RESULT_OUTPUT_LOCATION}
        )

        wait_time = 0
        while wait_time < ATHENA_TIMEOUT:
            try:
                results = athena_client.get_query_results(
                    QueryExecutionId=response["QueryExecutionId"]
                )
                print("---")
                for row in results["ResultSet"]["Rows"]:
                    aws_service = row["Data"][0]
                    data_transferred = row["Data"][1]
                    if "VarCharValue" in aws_service and "VarCharValue" in data_transferred and data_transferred["VarCharValue"].isnumeric():
                        if flow_direction == NAT2AWSSVC:
                            print("NAT-->%s" % aws_service["VarCharValue"],"%s bytes" % data_transferred["VarCharValue"])
                        else:
                            print("%s-->NAT" % aws_service["VarCharValue"],"%s bytes" % data_transferred["VarCharValue"])
                print()
                break
            except Exception as errMsg:
                if "Current state: RUNNING" or "Current state: QUEUED" in str(errMsg):
                    sleep_time = 2 ** wait_time
                    print("Athena query not finished, wait %d seconds" % sleep_time)
                    time.sleep(sleep_time)
                    wait_time += 1
                else:
                    print(errMsg)
                    break


def get_top_flows():
    _get_top_flow(NAT2AWSSVC, "nat-to-aws-service.sql")
    _get_top_flow(AWSSVC2NAT, "aws-service-to-nat.sql")

if __name__ == '__main__':
    # create_s3_bucket_for_vpcflowlog()
    # create_flowlogs()
    # init_athena()
    get_top_flows()


