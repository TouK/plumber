package pl.touk.plk.rtm.load.nifi.flow.tester.mock

import org.apache.avro.{Schema, SchemaParseException}
import org.apache.nifi.avro.AvroTypeUtil
import org.apache.nifi.confluent.schemaregistry.ConfluentSchemaRegistry
import org.apache.nifi.controller.ConfigurationContext
import org.apache.nifi.schema.access.SchemaNotFoundException
import org.apache.nifi.serialization.record.{RecordSchema, SchemaIdentifier, StandardSchemaIdentifier}

class ControllerServiceMocks {

  def confluentSchemaRegistry(id: String, schemaMap: Map[String, String]): ConfluentSchemaRegistry =
    new ConfluentSchemaRegistryMock(id, schemaMap)

}

class ConfluentSchemaRegistryMock(id: String, schemaMap: Map[String, String]) extends ConfluentSchemaRegistry{

  override def getIdentifier: String = id

  override def onEnabled(context: ConfigurationContext): Unit = {}

  override def retrieveSchema(schemaIdentifier: SchemaIdentifier): RecordSchema = {
    val schemaText = schemaMap.getOrElse(schemaIdentifier.getName.orElse(""), "")

    try {
      val schemaName = schemaIdentifier.getName.orElse("")
      val avroSchema = new Schema.Parser().parse(schemaText)
      val schemaId = new StandardSchemaIdentifier.Builder().id(schemaName.hashCode().toLong).name(schemaName).version(0).build()
      val recordSchema = AvroTypeUtil.createSchema(avroSchema, schemaText, schemaId)
      recordSchema
    } catch {
      case _: SchemaParseException =>
        throw new SchemaNotFoundException("Obtained Schema with id " + schemaIdentifier.getIdentifier.orElse(0) + " and name " + schemaIdentifier.getName.orElse("") + " from Confluent Schema Registry but the Schema Text that was returned is not a valid Avro Schema")
    }
  }

}
