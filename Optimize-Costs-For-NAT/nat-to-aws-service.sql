SELECT "interface-id", "pkt-dst-aws-service", SUM("bytes") as DataTransferred
FROM "vpcflowlogdb"."vpcflowlogtable"
WHERE "pkt-dst-aws-service" != '-'
GROUP BY "pkt-dst-aws-service", "interface-id"
ORDER BY "DataTransferred" DESC
