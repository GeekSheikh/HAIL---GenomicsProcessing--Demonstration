#Readme file for the [metadata](http://github.mtv.cloudera.com/srfnmnk/CHOA--HAIL-GeneProcessing/metadata.py) module

##Convert the VCF to VDS
First thing we need to do is convert the VCF to a VDS using the HAIL tool. While we do this the important parts to take a close look at are:

* The vcf must be block compressed such that it can be computed in parallel.
* The filename suffix of the vcf, it should be bgz
* When adding the annotations be sure to enable the “impute=True” flag. This allows the parquet file to be strongly typed which helps when building the table in the later steps.
* When writing the VDS ensure that you enable the “parquet_genotypes=True” flag so that the genotypes are present in the resulting VDS.

## Using Impala to create a table over the VDS
Now that we have a VDS it’s very helpful to be able to query the data but even more helpful to be able to join that with the VDS metatada. So first we will build the table over top of the VDS using Impala query. <br>

Let’s assume our root path to the vds is /tmp/1kg.vds and within that folder we have two files and a nested folder.

* metadata.json.gz - json formatted/compressed metadata about the VDS
* partitioner.json.gz - vds partitioning information for parallel computations
* rdd.parquet - Folder containing the VDS data as parquet files

Now to create the VDS database table in the vdsStore database we’ll run the query below. The <i>`like parquet` </i> command is providing Impala with a parquet file to use as an example from which to build the table schema. This should reference a single parquet file in the rdd.query directory where the <i> `location` </i> path should refernce the top loevel “rdd.parquet” directory within the VDS directory.

```
use vdsStore;
create external table 1kg_vds like parquet ‘/tmp/1kg.vds/rdd.parquet/part-r-00000-edb964aa-f2cb-42ec-9677-b300e8c54988.snappy.parquet'
stored as parquet
location ‘/tmp/1kg.vds/rdd.parquet'
```
## Building the Metadata in Parquet Format

As noted above, its very helpful to be able to join the genotype data with the metadata; however, HAIL provides the metadata in json format, so here we’ll use the `metadata` module written by Cloudera to convert the json file to parquet and wrap an Impala table around it if so desired.

Ensure the metadata.py file is visible to your code and then import it <br>
`import metadata` <br>
As you can see below in the ode there is really only one visible function which is the “build_parquet_metadata” funtion. It requires 3 parameters with an additional 3 used for creating and controlling the table creation.

Below are some details regarding the input parameters. Note that after successfully calling the build_parquet_metadata function a sub-directory within the vds root level directory will be created named “metadata”. This folder will have one file in it which is the parquet version of the original metadata.json.gz file. If you elected to create a table then a table will be tied to this is the selected database with the selected name but the path will remain the same.

###Input Parameters
* vds_path - root level path to the vds directory in hdfs
* sc - spark context variable
* spark - spark session variable
* create_tables - Bool - Whether or not to create the metadata table.
    * Default: False
* database - In which database to place the table
    * Default: default
* metadata_tablename - What to name the table
    * Default: vds_metadata

##Query Example
Below is an example of a resulting query one can write joining the VDS table and the metadata table.

```    
select sa.id, gs.gq, gs.dp, gs.gt, sa.annotation.pheno.caffeineconsumption, sa.annotation.pheno.isfemale, sa.annotation.pheno.purplehair
from 1kg_vds.gs gs
inner join 1kg_vds_metadata.sample_annotations sa on 
    sa.pos = gs.pos
```
