#!/bin/bash

echo "======================================"
echo " OMS SPARK BIG DATA CLUSTER"
echo "======================================"

docker compose up -d

sleep 20

echo ""
echo "Cluster Started"
echo ""

echo "Spark Master UI"
echo "http://localhost:8080"

echo ""
echo "Spark History"
echo "http://localhost:18080"

echo ""
echo "Workers:"
docker ps | grep spark_worker

echo ""
echo "Submitting Job..."

docker exec spark_master spark-submit \
--class OMSBigData \
--master spark://spark-master:7077 \
/opt/spark/jobs/oms.jar

echo ""
echo "Streaming Running..."