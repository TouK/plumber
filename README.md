# Plumber

Plumber lets you tame Apache NiFi flows. It is a Scala library for easy NiFi flow testing. 
Flows can be created using builders where you specify nodes and connections between them 
or you can load entire flow exported directly from NiFi.

This library is under development so feel free to let us know what you think.

## Usage
```bash
git clone https://github.com/TouK/plumber.git
cd plumber
mvn install
```

## Example

Lets test simple linear flow with three processors:
1. CsvValidator - which checks if incoming data are valid csv records
2. ConvertRecord - which converts data from csv to json
3. SplitJson - to split flow file containing multiple json, into many flow files with only one json each

We will use `NifiFlowBuilder` to create this flow manually. 
```scala
// input csv data
val data: String = """
      |id,name,surname,age
      |1,Arek,Testowy,21
      |2,Bartek,Testowy,31
      |3,Czesio,Testowy,42
      |4,Daniel,Testowy,16
      |4,Duplicated,Record Id,123
      |5,Ania,Testowa,Not Valid Age""".stripMargin

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
  // from split to end - output
  .addOutputConnection("splitJson", "split")
  .build()

// enqueue some data to input connection - this goes directly to csvValidator node 
flow.enqueue(data.getBytes, Map("input_filename"->"test.csv"))

// we run this flow once
flow.run()

// getFlowFiles returns flow files redirected to 'output'
val results = flow.executionResult.outputFlowFiles
results.foreach(r => println(new String(r.toByteArray)))
results should have size 4
results.head.assertContentEquals("""{"id":"1","name":"Arek","surname":"Testowy","age":"21"}""")
results.foreach(_.assertAttributeEquals("input_filename", "test.csv"))
```

## License
```text
Copyright 2015 original author or authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```