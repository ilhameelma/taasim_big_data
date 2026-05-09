#!/bin/bash
pip3 install --user -r /tmp/requirements.txt
/opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077