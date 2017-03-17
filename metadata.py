import os
from pyspark.sql import HiveContext

def create_metadata_table(sc, database, metadata_tablename, vds, df):
  hiveContext = HiveContext(sc)
  print 'Hive database is: %s' % database
  print 'Hive Table Name is: %s' % metadata_tablename
  print 'Database Table Location is: %s/metadata.parquet' % vds
  hiveContext.sql('create database if not exists ' + database + ' location \'' + vds + '/db\'')
  df.write.saveAsTable(database + '.' + metadata_tablename, path=vds + '/metadata.parquet')
  
def build_parquet_metadata(vds_path, sc, spark, create_tables=False, database='default',\
                           metadata_tablename='vds_metadata'):
  vds = vds_path
  print "Creating processing directory"
  os.system('hadoop fs -mkdir ' + vds + '/processing')
  print "Cloning metadata"
  os.system('hadoop fs -cp ' + vds + '/metadata.json.gz ' + vds + '/processing')
  rawJsonFile = sc.wholeTextFiles(vds + '/processing')
  print "Converting Metadata to Parquet"
  processedJsonFile = rawJsonFile.map(lambda line: line[1].replace('\n','').replace(' ', ''))
  df = spark.read.json(processedJsonFile)
  
  if create_tables:
    print "Creating Hive Metastore Table"
    create_metadata_table(sc, database, metadata_tablename, vds_path, df)
    
  else:
    df.coalesce(1).write.save(vds + '/processing/metadata.paruqet')
    os.system('hadoop fs -mkdir ' + vds + '/metadata')
    os.system('hadoop fs -cp ' + vds + '/processing/metadata.paruqet/part* ' + vds + '/metadata/metadata.snappy.parquet')  

  os.system('hadoop fs -rm -r ' + vds + '/processing')
  
    
#    
#select sa.id, gs.gq, gs.dp, gs.gt, sa.annotation.pheno.caffeineconsumption, sa.annotation.pheno.isfemale, sa.annotation.pheno.purplehair
#from 1kg_vds.gs gs
#inner join 1kg_vds_metadata.sample_annotations sa on 
#    sa.pos = gs.pos
