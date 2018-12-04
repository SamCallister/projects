#!/usr/bin/env bash
docker run -p 8888:8888 -v /Users/samc/school/big-data-healthcare/project:/home/jovyan/work -v /Users/samc/data:/home/jovyan/work/data --name bigdata jupyter/pyspark-notebook