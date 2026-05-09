#!/bin/bash
pip3 install --user -r /tmp/requirements.txt
/opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
