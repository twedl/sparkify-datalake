#!/usr/bin/bash
# -*- coding: utf-8 -*-

aws \
--profile sparkadmin \
--region us-west-2 \
emr create-cluster \
--name spark-cluster \
--use-default-roles \
--release-label emr-5.28.0 \
--log-uri s3://twedl-sparkify/logs/ \
--applications Name=Spark Name=Zeppelin \
--bootstrap-actions Path="s3://twedl-bootstrap-west/bootstrap-emr.sh" \
--ec2-attributes KeyName=spark-cluster-us-west-2,SubnetId=subnet-0027bfb78d4dbacc6 \
--instance-type m5.xlarge \
--instance-count 4 
