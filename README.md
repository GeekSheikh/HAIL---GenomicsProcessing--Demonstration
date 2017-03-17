# CHOA--HAIL-GeneProcessing
Gene Processing Pipeline for CHOA

Getting started with [HAIL](https://hail.is/index.html) and genomics data processing and Cloudera DSW.

[exploringVDS](http://github.mtv.cloudera.com/srfnmnk/CHOA--HAIL-GeneProcessing/blob/master/exploringVDS.py) is an example python file that demonstrates some of the basic HAIL examples.

The [metdata](http://github.mtv.cloudera.com/srfnmnk/CHOA--HAIL-GeneProcessing/blob/master/metadata.py) module is being worked back into HAIL via a PR so that the metadata.json file can be converted to a strongly typed parquet file such that Impala/Hive can easily join up the sample annotations with the sample/variants data etc. There's an additional, [more detailed document here](http://github.mtv.cloudera.com/srfnmnk/CHOA--HAIL-GeneProcessing/blob/master/metadata_README.md)

The [gnomad](http://github.mtv.cloudera.com/srfnmnk/CHOA--HAIL-GeneProcessing/blob/master/gnomad.py) file demonstratates how to filter the variant annotations from the gnomad VDS files and output a good VDS for Imapala/HIVE to query. By default, gnomad VCF files (as of 03/2017) cannot be converted due to some malformed segments. As such we must use the VDS files from the website and this illustrates the process and some basic queries to get started after you prune the data.

The following files are the actual HAIL files used to import HAIL into a project. Make sure they are accessible from your python project. Also make sure you have your DSW environment configured to point to the [spark-defaults.conf](http://github.mtv.cloudera.com/srfnmnk/CHOA--HAIL-GeneProcessing/blob/master/spark-defaults.conf) file.
* [hail-python.zip](http://github.mtv.cloudera.com/srfnmnk/CHOA--HAIL-GeneProcessing/blob/master/hail-python.zip)
* [hail-all-spark.jar](http://github.mtv.cloudera.com/srfnmnk/CHOA--HAIL-GeneProcessing/blob/master/hail-all-spark.jar)

Below is the python setup inside DSW and similar to the pyspark command that would be used to spin up the HAIL context inside DSW.

```
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
sc.addPyFile('metadata.py')
import sys
sys.path.append('/home/sense/hail-python.zip')

from hail import *
hc = HailContext(sc)
```

http://github.mtv.cloudera.com/srfnmnk/CHOA--HAIL-GeneProcessing/blob/master/metadata.py
