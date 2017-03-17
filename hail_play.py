from hail import *
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from math import log, isnan
import seaborn

hc = HailContext(sc)
vcf_path = '/Users/daniel.tomes/data_sets/gnomad_vcf/gnomad.genomes.r2.0.1.sites.21.vcf.bgz'
vds = hc.import_vcf(vcf_path)
vds = vds.split_multi()
vds = vds.annotate_samples_table(annotation_path,
                                 root='sa.pheno',
                                 sample_expr='Sample',
                                 config=TextTableConfig(impute=True))


pyspark2 --master local[4] --jars /home/daniel.tomes/Software/hail/build/libs/hail-all-spark.jar \
           --conf spark.hadoop.io.compression.codecs=org.apache.hadoop.io.compress.DefaultCodec,is.hail.io.compress.BGzipCodec,org.apache.hadoop.io.compress.GzipCodec \
           --conf spark.sql.files.openCostInBytes=1099511627776 \
           --conf spark.sql.files.maxPartitionBytes=1099511627776 \
           --conf spark.hadoop.mapreduce.input.fileinputformat.split.minsize=1099511627776 \
           --conf spark.hadoop.parquet.block.size=1099511627776


                      --py-files build/distributions/hail-python.zip \


export SPARK_HOME=/opt/cloudera/parcels/SPARK2/lib/spark2
export HAIL_HOME=/home/daniel.tomes/Software/hail
export PYTHONPATH="$PYTHONPATH:$HAIL_HOME/python:$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.3-src.zip"

from hail import *
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from math import log, isnan
hc = HailContext(sc)
vcf_path = '/user/daniel.tomes/choa/data/gnomad/sample/vcf/gnomad.genomes.r2.0.1.sites.22.vcf.bgz'
vds = hc.import_vcf(vcf_path)
vds = vds.split_multi()
vds.write('/user/daniel.tomes/choa/data/gnomad/sample/vcf/gnomad.genomes.r2.0.1.sites.22.vds')
