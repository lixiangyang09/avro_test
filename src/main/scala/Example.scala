import java.io.ByteArrayOutputStream

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.apache.avro.io._

object Example {
  def encode(record: GenericRecord, schema: Schema) : Array[Byte] = {

    val writer = new SpecificDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(record, encoder)
    encoder.flush()
    out.close()
    out.toByteArray
  }
  def decode(binary: Array[Byte], schema: Schema) : GenericRecord = {
    // Deserializing
    val reader = new SpecificDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get().binaryDecoder(binary, null)
    reader.read(null, decoder)
  }
  def main(args: Array[String]): Unit = {

    val schema1 =
      """
        |{
        |    "namespace": "avro.test",
        |     "type": "record",
        |     "name": "user",
        |     "fields":[
        |         {  "name": "id", "type": "int"},
        |         {   "name": "name",  "type": "string"},
        |         {   "name": "email", "type": ["string", "null"]}
        |     ]
        |}
      """.stripMargin
    val parsed_schema1 = new Schema.Parser().parse(schema1)
    val genericRecord1 = new GenericData.Record(parsed_schema1)
    genericRecord1.put("id", 1)
    genericRecord1.put("name", "singh")
    genericRecord1.put("email", null)
    val encoded_binary1 = encode(genericRecord1, parsed_schema1)

    val decoded_record1 = decode(encoded_binary1, parsed_schema1)




    val schema2 =
      """
        |{
        |    "namespace": "avro.test",
        |     "type": "record",
        |     "name": "user",
        |     "fields":[
        |         {  "name": "id", "type": "int"},
        |         {   "name": "name",  "type": "string"}
        |     ]
        |}
      """.stripMargin
    val parsed_schema2 = new Schema.Parser().parse(schema2)
    val genericRecord2 = new GenericData.Record(parsed_schema1)
    genericRecord2.put("id", 1)
    genericRecord2.put("name", "singh")
    val encoded_binary2 = encode(genericRecord2, parsed_schema2)
    val decoded_record2 = decode(encoded_binary2, parsed_schema2)

    val schema3 =
      """
        |{
        |    "namespace": "avro.test",
        |     "type": "record",
        |     "name": "user",
        |     "fields":[
        |         {   "name": "name",  "type": "string"},
        |         {   "name": "email", "type": ["string", "null"]},
        |         {  "name": "id", "type": "int"}
        |     ]
        |}
      """.stripMargin
    val parsed_schema3 = new Schema.Parser().parse(schema3)
    val genericRecord3 = new GenericData.Record(parsed_schema3)
    genericRecord3.put("id", 3)
    genericRecord3.put("name", "singh")
    genericRecord3.put("email", null)
    val encoded_binary3 = encode(genericRecord3, parsed_schema3)

    val decoded_record3 = decode(encoded_binary3, parsed_schema3)
  }
}
