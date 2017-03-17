import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.spi.JsonUtil;
import org.kitesdk.data.spi.filesystem.JSONFileReader;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;
import org.apache.hadoop.conf.Configuration;

import java.io.File;

/**
 * Created by daniel.tomes on 3/14/17.
 */
public class converter {

    public static void main(String[] args) throws java.io.IOException {

        FileSystem fs = FileSystem.get(new Configuration());
        Path source = new Path("/Users/daniel.tomes/Documents/Dev/gitProjects/CHOA--HAIL-GeneProcessing/schema_conversion/src/main/resources/metadata.json");
        String sourceStr = "/Users/daniel.tomes/Documents/Dev/gitProjects/CHOA--HAIL-GeneProcessing/schema_conversion/src/main/resources/metadata.json";
        Path outputPath = new Path("/Users/daniel.tomes/Documents/Dev/gitProjects/CHOA--HAIL-GeneProcessing/schema_conversion/src/main/resources/metadata.parquet");

//        Schema jsonSchema = JsonUtil.inferSchema(fs.open(source), "RecordName", 20);
//        try(JSONFileReader<GenericData.Record> reader = new JSONFileReader<GenericData.Record>(
//                fs.open(source), jsonSchema, GenericData.Record.class)) {
//
//            reader.initialize();
//
//            try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
//                    .<GenericData.Record>builder(outputPath)
//                    .withConf(new Configuration())
////                    .withCompressionCodec(CompressionCodecName.SNAPPY)
//                    .withSchema(jsonSchema)
//                    .build()) {
//                for (GenericData.Record record : reader) {
//                    writer.write(record);
//                }
//            }
//        }

        final Schema avroSchema = new Schema.Parser().parse(new File(sourceStr));
        System.out.print(avroSchema.toString());
    }
}
