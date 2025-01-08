#!/bin/bash

set -e

docker build -t spark-base:latest ./docker-images/base
docker build -t spark-master:latest ./docker-images/spark-master
docker build -t spark-worker:latest ./docker-images/spark-worker
docker build -t spark-submit:latest ./docker-images/spark-submit
