import json
import os
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
    .config("master", "yarn")\
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

vds_path = '/choa/gnomad/vds/gnomad.genomes.r2.0.1.sites.autosomes.vds'
out_path = '/choa/gnomad/vds/filtered_gnomad.genomes.r2.0.1.sites.autosomes.vds'

vds = hc.read(vds_path)

# Splitting the variant alleles where more than one exists in a single site.
vds = vds.split_multi()
# Take a look at the schema to build the list of variant annotation you want
# The total list results in a schema that is too big to create a table on top of it.
# The max string length for the complex schema is 4000 characters
print(vds.variant_schema)
# Filter the vds to only include the desired variant annotations
filtered_vds = vds.annotate_variants_expr('va = {aIndex: va.aIndex, filters: va.filters, AC: va.info.AC, AF: va.info.AF, AN: va.info.AN,AC_AFR: va.info.AC_AFR, AC_AMR: va.info.AC_AMR, AC_ASJ: va.info.AC_ASJ, AC_EAS: va.info.AC_EAS, AC_FIN: va.info.AC_FIN, AC_NFE: va.info.AC_NFE, AC_OTH: va.info.AC_OTH, AC_Male: va.info.AC_Male, AC_Female: va.info.AC_Female, AF_AFR: va.info.AF_AFR, AF_AMR: va.info.AF_AMR, AF_ASJ: va.info.AF_ASJ, AF_EAS: va.info.AF_EAS, AF_FIN: va.info.AF_FIN, AF_NFE: va.info.AF_NFE, AF_OTH: va.info.AF_OTH, AF_Male: va.info.AF_Male, AF_Female: va.info.AF_Female, AC_raw: va.info.AC_raw, AF_raw: va.info.AF_raw}')
# Write out the VDS to storage. Be sure to add parquet_genotypes = True if you have sample data in the vds
filtered_vds.write(out_path, parquet_genotypes=True, overwrite=True)
##Notes

# Telling Queries for this Dataset

# Proves that AF (allele freq) is only a viable measure when the total AC (Allele Count) is known and statistially relevant
# select annotations.an as AN, annotations.ac[annotations.aindex -1] as AC,
# annotations.an*annotations.af[annotations.aindex -1] as AN_AF, annotations.af[annotations.aindex -1] as AF, variant.altalleles[0]
# from genomes_autosomes_vds 
# where variant.start = 118222020
# and annotations.filters is null
# limit 50