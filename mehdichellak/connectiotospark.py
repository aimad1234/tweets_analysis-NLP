# -*- coding: utf-8 -*-
"""
Created on Tue Jun 29 21:38:41 2021

@author: ASUS
"""

from pyspark.sql import SparkSession


df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
  .option("subscribe", "topic1") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")