#!/bin/bash

# stop all running containers
docker stop $(docker ps -aq)

# remove all containers
docker rm $(docker ps -aq)
