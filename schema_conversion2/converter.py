import simplejson as sjson
import json
from pprint import pprint

# if __name__ == "__main__":

# with open('data/metadata.json') as f:
#     metadata_json = sjson.load(f)
#     version = metadata_json.get('version')
#     isSplit = metadata_json.get('split')
#     isDosage = metadata_json.get('isDosage')
#     isGenericGenotype = metadata_json.get('isGenericGenotype')
#     parquetGenotypes = metadata_json.get('parquetGenotypes')
#     sample_annotation_schema = metadata_json.get('sample_annotation_schema')
#     variant_annotation_schema = metadata_json.get('variant_annotation_schema')
#     global_annotation_schema = metadata_json.get('global_annotation_schema')
#     genotype_schema = metadata_json.get('genotype_schema')
#     sample_annotations = metadata_json.get('sample_annotations')
#     global_annotation = metadata_json.get('global_annotation')

with open('data/metadata_two.json') as data_file:
    data = json.load(data_file)

# pprint(data)

# print(data['sample_annotations'])

annotations = {}

def traverse_layers(layer):
    fields = []
    for k,v in layer.iteritems():
        if type(v) is dict:
            print "type v was dict"
            print "the key was: %s" % k
            print "the value was: %s" % v
            fields.append()
            traverse_layers(v)
        else:
            print (k, v)


for sample_annotation in data['sample_annotations']:
    print(type(sample_annotation))
    print(sample_annotation.keys())
    print "ID is: %s" % sample_annotation['id']
    print "Full Annotation is: %s" % sample_annotation['annotation']
    print "sample_annotation type is: %s" % type(sample_annotation['annotation'])


# annotes_list = data['sample_annotations']
# print(len(annotes_list))
# annotes = data['sample_annotations'][0]
#
# pprint(annotes)

# for item in annotes:
#     print item['id']
#     print item['annotation']


    # for item in json:
    #     print(item)
#
# print "Version is: %s" % version
# print "Split is: %s" % isSplit
# print sample_annotations

#
# {
#   "version" : 4,
#   "split" : false,
#   "isDosage" : false,
#   "isGenericGenotype" : false,
#   "parquetGenotypes" : false,
#   "sample_annotation_schema" : "Struct{pheno:Struct{Sample:String,height:Double,weight:Int}}",
#   "variant_annotation_schema" : "Struct{rsid:String,qual:Double,filters:Set[String],info:Struct{OID:Array[String]@Description=\"List of original Hotspot IDs\"@Number=\".\"@Type=\"String\",OPOS:Array[Int]@Description=\"List of original allele positions\"@Number=\".\"@Type=\"Integer\",OREF:Array[String]@Description=\"List of original reference bases\"@Number=\".\"@Type=\"String\",OALT:Array[String]@Description=\"List of original variant bases\"@Number=\".\"@Type=\"String\",OMAPALT:Array[String]@Description=\"Maps OID,OPOS,OREF,OALT entries to specific ALT alleles\"@Number=\".\"@Type=\"String\",AO:Array[Int]@Description=\"Alternate allele observations\"@Number=\"A\"@Type=\"Integer\",DP:Int@Description=\"Total read depth at the locus\"@Number=\"1\"@Type=\"Integer\",FAO:Array[Int]@Description=\"Flow Evaluator Alternate allele observations\"@Number=\"A\"@Type=\"Integer\",FDP:Int@Description=\"Flow Evaluator read depth at the locus\"@Number=\"1\"@Type=\"Integer\",FR:Array[String]@Description=\"Reason why the variant was filtered.\"@Number=\".\"@Type=\"String\",FRO:Int@Description=\"Flow Evaluator Reference allele observations\"@Number=\"1\"@Type=\"Integer\",FSAF:Array[Int]@Description=\"Flow Evaluator Alternate allele observations on the forward strand\"@Number=\"A\"@Type=\"Integer\",FSAR:Array[Int]@Description=\"Flow Evaluator Alternate allele observations on the reverse strand\"@Number=\"A\"@Type=\"Integer\",FSRF:Int@Description=\"Flow Evaluator Reference observations on the forward strand\"@Number=\"1\"@Type=\"Integer\",FSRR:Int@Description=\"Flow Evaluator Reference observations on the reverse strand\"@Number=\"1\"@Type=\"Integer\",FWDB:Array[Double]@Description=\"Forward strand bias in prediction.\"@Number=\"A\"@Type=\"Float\",HRUN:Array[Int]@Description=\"Run length: the number of consecutive repeats of the alternate allele in the reference genome\"@Number=\"A\"@Type=\"Integer\",HS:Boolean@Description=\"Indicate it is at a hot spot\"@Number=\"0\"@Type=\"Flag\",LEN:Array[Int]@Description=\"allele length\"@Number=\"A\"@Type=\"Integer\",MLLD:Array[Double]@Description=\"Mean log-likelihood delta per read.\"@Number=\"A\"@Type=\"Float\",NR:String@Description=\"Reason why the variant is a No-Call.\"@Number=\"1\"@Type=\"String\",NS:Int@Description=\"Number of samples with data\"@Number=\"1\"@Type=\"Integer\",RBI:Array[Double]@Description=\"Distance of bias parameters from zero.\"@Number=\"A\"@Type=\"Float\",REFB:Array[Double]@Description=\"Reference Hypothesis bias in prediction.\"@Number=\"A\"@Type=\"Float\",REVB:Array[Double]@Description=\"Reverse strand bias in prediction.\"@Number=\"A\"@Type=\"Float\",RO:Int@Description=\"Reference allele observations\"@Number=\"1\"@Type=\"Integer\",SAF:Array[Int]@Description=\"Alternate allele observations on the forward strand\"@Number=\"A\"@Type=\"Integer\",SAR:Array[Int]@Description=\"Alternate allele observations on the reverse strand\"@Number=\"A\"@Type=\"Integer\",SRF:Int@Description=\"Number of reference observations on the forward strand\"@Number=\"1\"@Type=\"Integer\",SRR:Int@Description=\"Number of reference observations on the reverse strand\"@Number=\"1\"@Type=\"Integer\",SSEN:Double@Description=\"Strand-specific-error prediction on negative strand.\"@Number=\"1\"@Type=\"Float\",SSEP:Double@Description=\"Strand-specific-error prediction on positive strand.\"@Number=\"1\"@Type=\"Float\",STB:Double@Description=\"Strand bias in variant relative to reference.\"@Number=\"1\"@Type=\"Float\",SXB:Double@Description=\"Experimental strand bias based on approximate bayesian score for difference in frequency.\"@Number=\"1\"@Type=\"Float\",TYPE:Array[String]@Description=\"The type of allele, either snp, mnp, ins, del, or complex.\"@Number=\"A\"@Type=\"String\",VARB:Array[Double]@Description=\"Variant Hypothesis bias in prediction.\"@Number=\"A\"@Type=\"Float\",genes:Array[String]@Description=\"Overlapping gene name\"@Number=\".\"@Type=\"String\",cosmic:Array[String]@Description=\"Cosmic id\"@Number=\".\"@Type=\"String\",omim:Array[String]@Description=\"Omim id of the gene\"@Number=\".\"@Type=\"String\",dbsnp:Array[String]@Description=\"Dbsnp id of the gene\"@Number=\".\"@Type=\"String\"}}",
#   "global_annotation_schema" : "Empty",
#   "genotype_schema" : "Genotype",
#   "sample_annotations" : [ {
#     "id" : "E33331-pool54-L7013",
#     "annotation" : {
#       "pheno" : {
#         "Sample" : "E33331-pool54-L7013",
#         "height" : 5540.8,
#         "weight" : 27694
#       }
#     }
#   } ],
#   "global_annotation" : null
# }