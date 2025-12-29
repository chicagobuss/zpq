#!/bin/bash
# Wrapper to trace S3 calls on the local MinIO instance
docker-compose exec mc mc admin trace local -a -v --insecure

