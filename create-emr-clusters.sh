#!/usr/bin/bash
# -*- coding: utf-8 -*-

aws \
--profile sparkadmin \
emr create-cluster \
--name spark-cluster \
--use-default-roles \
--release-label emr-5.20.0 \
--instance-count 3 \
--applications Name=Spark Name=Zeppelin \
--bootstrap-actions Path="s3://udac-bootstrap-sh/bootstrap-emr.sh" \
--ec2-attributes KeyName=spark-cluster,SubnetId=subnet-0554f7f11c8ee5e8f \
--instance-type m5.xlarge
