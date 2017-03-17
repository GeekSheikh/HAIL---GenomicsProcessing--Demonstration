import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import seaborn
from math import log, isnan
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
spark = SparkSession\
    .builder\
    .config("spark.jars", "/home/sense/hail-all-spark.jar")\
    .config("spark.submit.pyFiles", "/home/sense/hail-python.zip")\
    .config("spark.hadoop.io.compression.codecs", "org.apache.hadoop.io.compress.DefaultCodec,is.hail.io.compress.BGzipCodec,org.apache.hadoop.io.compress.GzipCodec")\
    .config("spark.sql.files.openCostInBytes", "1099511627776")\
    .config("spark.sql.files.maxPartitionBytes", "1099511627776")\
    .config("spark.hadoop.mapreduce.input.fileinputformat.split.minsize", "1099511627776")\
    .config("spark.hadoop.parquet.block.size", "1099511627776")\
    .appName("Hail")\
    .getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)
sc.addPyFile('metadata.py')
import sys
sys.path.append('/home/sense/hail-python.zip')

from hail import *
hc = HailContext(sc)

# Here we'll import the metadata module from the file metadata.py
import metadata

#! wget https://storage.googleapis.com/hail-tutorial/Hail_Tutorial_Data-v2.tgz

#! tar -xvzf Hail_Tutorial_Data-v2.tgz

#! hadoop fs -put Hail_Tutorial-v2 .

vds = hc.import_vcf('Hail_Tutorial-v2/1000Genomes_248samples_coreExome10K.vcf.bgz')

vds = vds.split_multi()

vds = vds.annotate_samples_table('Hail_Tutorial-v2/1000Genomes.sample_annotations',
                                 root='sa.pheno', 
                                 sample_expr='Sample', 
                                 config=TextTableConfig(impute=True))

! hadoop fs -cat Hail_Tutorial-v2/1000Genomes.sample_annotations | head -n 200

out_path = '1kg.vds'
vds.write(out_path, parquet_genotypes=True)

# Here the path needs to be fully qualified, not relative path
# Then we call "build_parquet_metadata" to build the metadata table.
vds_path = '/user/tomesd/1kg.vds'
metadata.build_parquet_metadata(vds_path, sc, spark, create_tables=True, database='hail_vds_tomesd')