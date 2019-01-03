package pl.touk.plk.rtm.load.nifi.flow.tester.component

import org.apache.nifi.processor.Relationship
import org.apache.nifi.util.{MockFlowFile, TestRunner}
import pl.touk.plk.rtm.load.nifi.flow.tester.FlowFile

import scala.collection.JavaConversions._

class ExecutionResults {

  private var relationToResultsMap: Map[String, List[MockFlowFile]] = Map()
  private var relationToAlreadyReturnedMap: Map[String, Set[Long]] = Map()

  def newFromRelationship(relationship: String): List[FlowFile] = {
    val all = relationToResultsMap.getOrElse(relationship, List.empty)
    val alreadyReturned = relationToAlreadyReturnedMap.getOrElse(relationship, Set.empty)
    val notReturnedYet = all.filterNot(e => alreadyReturned.contains(e.getId))
    relationToAlreadyReturnedMap = relationToAlreadyReturnedMap + (relationship -> all.map(_.getId).toSet)
    notReturnedYet.map(new FlowFile(_))
  }

  def allFromRelationship(relationship: String): List[MockFlowFile] = {
    relationToResultsMap.getOrElse(relationship, List.empty)
  }

  def setResultsForRelationship(relationship: Relationship, files: List[MockFlowFile]): Unit = {
    setResultsForRelationship(relationship.getName, files)
  }

  def setResultsForRelationship(relationship: String, files: List[MockFlowFile]): Unit = {
    relationToResultsMap += relationship -> files
  }

  def setResultsFromRunner(runner: TestRunner): Unit = {
    relationToResultsMap = runner.getProcessor.getRelationships
      .map(r => r.getName -> runner.getFlowFilesForRelationship(r).toList).toMap
  }


}
