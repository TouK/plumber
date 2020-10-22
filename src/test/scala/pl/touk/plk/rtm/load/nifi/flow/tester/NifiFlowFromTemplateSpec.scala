package pl.touk.plk.rtm.load.nifi.flow.tester

import org.apache.nifi.persistence.TemplateDeserializer
import org.apache.nifi.processors.standard.{ReplaceText, TransformXml, ValidateXml}
import org.apache.nifi.web.api.dto.TemplateDTO
import org.scalatest.{FunSuite, Matchers}

class NifiFlowFromTemplateSpec extends FunSuite with Matchers {

  private val xmlData: List[FlowFile] = List(
    "<person><name>Foo</name><type>student</type><age>31</age></person>",
    "<person><name>Invalid age</name><type>student</type><age>-11</age></person>",
    "<person><name>Bar</name><type>student</type><age>12</age></person>",
    "<car><name>Optimus</name><type>heavy truck</type><age>1200</age></car>",
    "<person><name>FooBar</name><type>teacher</type><age>42</age></person>"
  ).map(FlowFile(_, Map.empty[String, String]))

  private val testResourceDir: String = this.getClass.getClassLoader.getResource("").getFile

  test("create valid flow from xml file") {
    val flow = NifiTemplateFlowFactory(template("convert-xml-to-json-flow.xml"))
      .setProcessorProperty("ValidateXml", ValidateXml.SCHEMA_FILE.getName, testResourceDir + "person/person-schema.xsd")
      .setProcessorProperty("TransformXml", TransformXml.XSLT_FILE_NAME.getName, testResourceDir + "xml-to-json.xsl")
      .create()

    flow.enqueueByName(xmlData, "input")
    flow.run(1)

    flow.resultsFromOutputPort("output") should have size 1
    flow.run(1)
    // still one, because second record is invalid
    flow.resultsFromOutputPort("output") should have size 1
    flow.run(1)
    flow.resultsFromOutputPort("output") should have size 2

    flow.run(5)

    val files = flow.resultsFromOutputPort("output")
    files should have size 3
    files.head.assertContentEquals("""{"person":{"name":"Foo","type":"student","age":31}}""")
  }


  test("create valid flow with process group from xml file") {
    val flowBuilder = NifiTemplateFlowFactory(template("convert-xml-to-json-as-group.xml"))

    flowBuilder.getProcessGroup("ValidateAndConvertXml")
      .setProcessorProperty("ValidateXml", ValidateXml.SCHEMA_FILE.getName, testResourceDir + "person/person-schema.xsd")
      .setProcessorProperty("TransformXml", TransformXml.XSLT_FILE_NAME.getName, testResourceDir + "xml-to-json.xsl")

    val flow = flowBuilder.create()

    flow.getProcessGroup("ValidateAndConvertXml").enqueueByName(xmlData, "input")
    flow.run(2)
    flow.run(2)
    flow.run(2)

    val files = flow.getProcessGroup("ValidateAndConvertXml").resultsFromOutputPort("output")
    files should have size 3
    files.head.assertContentEquals("""{"person":{"name":"Foo","type":"student","age":31}}""")
  }

  test("create valid flow with two nested groups") {
    val flow = NifiTemplateFlowFactory(template("outer_group.xml")).create()

    flow.getProcessGroup("OuterGroup")
      .getProcessGroup("FirstInnerGroup")
      .enqueueByName(List(FlowFile("empty", Map("foo" -> "bar")), FlowFile("empty2"), FlowFile("empty3")), "Input")
    flow.run(2)
    flow.run(2)
    flow.run(2)

    val files = flow.getProcessGroup("OuterGroup").getProcessGroup("SecondInnerGroup").resultsFromOutputPort("second output")
    files should have size 3
    files.head.assertContentEquals("empty")
    files.head.assertAttributeEquals("foo", "bar")
  }

  test("create valid flow with group to processor connection") {
    val flow = NifiTemplateFlowFactory(template("group-to-processor.xml")).create()

    flow.getProcessGroup("OuterGroup")
      .getProcessGroup("FirstInnerGroup")
      .enqueueByName(List(FlowFile("empty", Map("foo" -> "bar")), FlowFile("empty2"), FlowFile("empty3")), "Input")
    flow.run(2)
    flow.getProcessGroup("OuterGroup").resultsFromOutputPort("out") should have size 2
    flow.run(2)
    flow.getProcessGroup("OuterGroup").resultsFromOutputPort("out") should have size 3
    flow.run(2)

    val files = flow.getProcessGroup("OuterGroup").resultsFromOutputPort("out")
    files should have size 3
    files.head.assertContentEquals("empty")
    files.head.assertAttributeEquals("foo", "bar")

    flow.run(10)

    val filesAfterAnotherRun = flow.getProcessGroup("OuterGroup").resultsFromOutputPort("out")
    filesAfterAnotherRun should have size 3
  }

  test("create valid flow with processor that does not require input") {
    val flow = NifiTemplateFlowFactory(template("generate.xml")).create()

    flow.run(1)
    flow.getProcessGroup("GenerateFlowFileGroup").resultsFromOutputPort("out") should have size 1
    flow.run(1)
    flow.getProcessGroup("GenerateFlowFileGroup").resultsFromOutputPort("out") should have size 2
    flow.run(3)

    val files = flow.getProcessGroup("GenerateFlowFileGroup").resultsFromOutputPort("out")
    files should have size 5
    files.head.assertContentEquals("flow file")

  }

  test("create valid flow with processor that uses variable") {
    val flow = NifiTemplateFlowFactory(template("variable-flow.xml")).create()

    flow.getProcessGroup("flow").processors.find(_.processor.getName == "GenerateFlowFile").head
      .processor.getConfig.getProperties.get("generate-ff-custom-text") shouldBe "${fooBarVariable}"

    flow.run(1)
    flow.getProcessGroup("flow").resultsFromOutputPort("out") should have size 1

    val files = flow.getProcessGroup("flow").resultsFromOutputPort("out")
    files should have size 1
    files.head.assertContentEquals("properContent")

  }

  test("create valid flow with processor to process group to another processor") {
    val flow = NifiTemplateFlowFactory(template("flow-processor-to-group-to-processor.xml")).create()

    flow.run(1)

    val files = flow.getProcessGroup("flow").resultsFromProcessorRelation("UpdateAttribute", "success")
    files should have size 1
    files.head.assertContentEquals("text")
    files.head.assertAttributeEquals("innerUpdate", "win")
    files.head.assertAttributeEquals("outerUpdate", "win")

  }

  test("create valid flow with funnel") {
    val flow = NifiTemplateFlowFactory(template("funnel.xml")).create()

    flow.run(3)

    val files = flow.getProcessGroup("flow").resultsFromProcessorRelation("UpdateAttribute", "success")
    files should have size 3
    files.map(f => new String(f.toByteArray)).toSet should be eq Set(1, 2, 3).map("generate" + _)

  }

  test("disable validate expression usage by processor name") {
    val flow = NifiTemplateFlowFactory(template("replace-text-validate-expression.xml")).create()

    intercept[AssertionError] {
      flow.run(1)
    }

    flow.setValidateExpressionUsage("ReplaceText", false)

    flow.run()

    val files = flow.resultsFromProcessorRelation("UpdateAttribute", "success")
    files should have size 1
    files.head.assertContentEquals("generate1")
  }

  test("disable validate expression usage by processor class") {
    val flow = NifiTemplateFlowFactory(template("replace-text-validate-expression.xml")).create()

    intercept[AssertionError] {
      flow.run(1)
    }

    flow.setValidateExpressionUsageByType(classOf[ReplaceText], validate = false, recursive = true)

    flow.run()

    val files = flow.resultsFromProcessorRelation("UpdateAttribute", "success")
    files should have size 1
    files.head.assertContentEquals("generate1")
  }

  test("disable validate expression usage automatically") {
    val flow = NifiTemplateFlowFactory(template("validate-expression.xml")).create()

    intercept[AssertionError] {
      flow.run(1)
    }

    flow.automaticallyDisableValidateExpressionUsage()

    flow.run()

    val files = flow.getProcessGroupByPath("log").resultsFromProcessorRelation("ReplaceText", "success")
    files should have size 1
  }

  private def template(fileName: String): TemplateDTO = {
    TemplateDeserializer.deserialize(this.getClass.getClassLoader.getResourceAsStream(s"template/$fileName"))
  }
}
