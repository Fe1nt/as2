#!/bin/bash

spark-submit \
	--driver-memory 8G \
	--master local[4] \
	Stage2.py
	

