SELECT "pkt-src-aws-service", SUM("bytes") as DataTransferred
FROM "vpcflowlogdb"."vpcflowlogtable"
WHERE "pkt-src-aws-service" != '-'
GROUP BY "pkt-src-aws-service"
ORDER BY "DataTransferred" DESC
