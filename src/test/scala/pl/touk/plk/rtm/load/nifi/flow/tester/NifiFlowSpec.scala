package pl.touk.plk.rtm.load.nifi.flow.tester

import org.apache.nifi.csv.{CSVReader, CSVUtils}
import org.apache.nifi.json.JsonRecordSetWriter
import org.apache.nifi.processors.standard.{ConvertRecord, EvaluateXPath, SplitJson, ValidateCsv}
import org.apache.nifi.schema.access.SchemaAccessUtils
import org.scalatest.{FunSuite, Matchers}

class NifiFlowSpec extends FunSuite with Matchers {

  val data: String = """
      |id,name,surname,age
      |1,Arek,Testowy,21
      |2,Bartek,Testowy,31
      |3,Czesio,Testowy,42
      |4,Daniel,Testowy,16
      |4,Duplicated,Record Id,123
      |5,Ania,Testowa,Not Valid Age""".stripMargin

  test("should test flow"){

    // simple flow consisting of three processors:
    // 1. CsvValidator - which checks if test data are valid csv records
    // 2. ConvertRecord - which converts data from csv to json
    // 3. SplitJson - to split flow file containing multiple json, into many flow files with only one json each

    val flow = new NifiFlowBuilder()
      //defining controller services which later will be used in processors
      .addControllerService("csvReader", new CSVReader(),
        Map(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY.getName -> "csv-header-derived",
          CSVUtils.VALUE_SEPARATOR.getName -> ",",
          CSVUtils.FIRST_LINE_IS_HEADER.getName -> "true",
          CSVUtils.CSV_FORMAT.getName -> CSVUtils.RFC_4180.getValue))
      .addControllerService("jsonWriter", new JsonRecordSetWriter(), Map("Schema Write Strategy" -> "no-schema"))
      // adding nodes to our flow which later will be connected to each other using given names
      .addNode("csvValidator", new ValidateCsv,
        Map(ValidateCsv.SCHEMA.getName -> "Unique(),StrNotNullOrEmpty(),StrNotNullOrEmpty(),ParseInt()",
          ValidateCsv.VALIDATION_STRATEGY.getName -> ValidateCsv.VALIDATE_LINES_INDIVIDUALLY.getValue
        ))
      .addNode("convert", new ConvertRecord, Map("record-reader" -> "csvReader", "record-writer" -> "jsonWriter"))
      .addNode("splitJson", new SplitJson, Map(SplitJson.ARRAY_JSON_PATH_EXPRESSION.getName -> "$.*"))
      // this is starting point - where test data are queued
      .addInputConnection("csvValidator")
      // next connection - from csvValidator processor's valid relation to convert processor
      .addConnection("csvValidator", "convert", ValidateCsv.REL_VALID)
      // from convert success relation to split
      .addConnection("convert", "splitJson","success")
      // from split to end
      .addOutputConnection("splitJson", "split")
      .build()

    flow.enqueue(data.getBytes, Map("input_filename"->"test.csv"))

    flow.run()

    // getFlowFiles returns flow files redirected to 'output'
    val results = flow.executionResult.outputFlowFiles
    results.foreach(r => println(new String(r.toByteArray)))
    results should have size 4
    results.head.assertContentEquals("""{"id":"1","name":"Arek","surname":"Testowy","age":"21"}""")
    results.foreach(_.assertAttributeEquals("input_filename", "test.csv"))
  }

  test("should use variables"){
    val flow = new NifiFlowBuilder()
      .addControllerService("csvReader", new CSVReader(),
      Map(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY.getName -> "csv-header-derived",
        CSVUtils.VALUE_SEPARATOR.getName -> "${separator}",
        CSVUtils.FIRST_LINE_IS_HEADER.getName -> "true",
        CSVUtils.CSV_FORMAT.getName -> CSVUtils.RFC_4180.getValue))
      .addControllerService("jsonWriter", new JsonRecordSetWriter(), Map("Schema Write Strategy" -> "no-schema"))
      .addNode("csvValidator", new ValidateCsv,
      Map(ValidateCsv.SCHEMA.getName -> "${schema}",
        ValidateCsv.VALIDATION_STRATEGY.getName -> ValidateCsv.VALIDATE_LINES_INDIVIDUALLY.getValue
      ))
      .addNode("convert", new ConvertRecord, Map("record-reader" -> "csvReader", "record-writer" -> "jsonWriter"))
      .addInputConnection("csvValidator")
      .addConnection("csvValidator", "convert", ValidateCsv.REL_VALID)
      .addOutputConnection("convert", "success")
      .setVariable("separator",",")
      .setVariable("schema","Unique(),StrNotNullOrEmpty(),StrNotNullOrEmpty(),ParseInt()")
      .build()

    flow.enqueue(data.getBytes(), Map("input_filename"->"test.csv"))

    flow.run()

    // getFlowFiles returns flow files redirected to 'output'
    val results = flow.executionResult.outputFlowFiles
    results.foreach(r => println(new String(r.toByteArray)))
    results should have size 1
    new String(results.head.toByteArray) should startWith("""[{"id":"1","name":"Arek","surname":"Testowy","age":"21"},{"id":"2",""")
    results.foreach(_.assertAttributeEquals("input_filename", "test.csv"))
  }

  test("should process each record only once when flow ran multiple times"){
    val xmlData = List(
      "<student><name>Foo</name></student>",
      "<student><name>Bar</name></student>",
      "<teacher><name>FooBar</name></teacher>"
    ).map(FlowFile(_))
    val flow = new NifiFlowBuilder()
      .addNode("EvaluateXPath", new EvaluateXPath,
        Map("rootElement" -> "name(/*)",
          "name" -> "/*/name",
          EvaluateXPath.DESTINATION.getName -> EvaluateXPath.DESTINATION_ATTRIBUTE))
      .addInputConnection("EvaluateXPath")
      .addOutputConnection("EvaluateXPath", EvaluateXPath.REL_MATCH)
      .build()

    flow.executionResult.outputFlowFiles should have size 0

    flow.enqueue(xmlData)
    flow.run(1)
    flow.run(2)
    flow.run(3)

    val files = flow.executionResult.outputFlowFiles
    files should have size 3
    files.foreach(_.assertAttributeExists("rootElement"))
    files.foreach(_.assertAttributeExists("name"))
    files.map(_.getAttribute("name")) shouldBe List("Foo", "Bar", "FooBar")

    flow.run()
    flow.executionResult.outputFlowFiles should have size 3
  }

}
