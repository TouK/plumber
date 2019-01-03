package pl.touk.plk.rtm.load.nifi.flow.tester

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.apache.nifi.annotation.behavior.InputRequirement
import org.apache.nifi.controller.ControllerService
import org.apache.nifi.processor.Processor
import org.apache.nifi.util.TestRunners.newTestRunner
import org.apache.nifi.util.{MockFlowFile, TestRunner}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.control.NonFatal

object NifiFlow{
  private[tester] val INPUT_NODE: String = "_Input"
  private[tester] val OUTPUT_NODE: String = "_Output"
  private[tester] val LOG_ATTR_NODE: String = "_LogAttribute"
  private[tester] val UPD_FAILURE_CTX_NODE: String = "_UpdateFailureContext"

  val SUCCESS_RELATION: String = "success"
  val FAILURE_RELATION: String = "failure"
  val VALID_RELATION: String = "valid"
  val INVALID_RELATION: String = "invalid"
}

class NifiFlow(private val nodes: List[Node], private val connections: List[Connection], private val services: List[Service], private val variables: Map[String, String]) {

  private val queue: mutable.Queue[FlowFile] = new mutable.Queue()
  private var result: ExecutionResult = ExecutionResult(Map.empty, connections.find(_.to == NifiFlow.OUTPUT_NODE).map(_.id))
  private val fromConnectionsMap: Map[String, List[Connection]] = connections.groupBy(_.from)
  private val nodeMap: Map[String, Node] = nodes.groupBy(_.name).mapValues(_.head)

  initRunners()

  private def initRunners(): Unit = {
    nodes.foreach(n => {
      n.initRunner(services)
      variables.foreach{
        case (name, value) => n.setVariable(name, value)
      }
    })
  }

  def enqueue(data: String): Unit = {
    enqueue(data.getBytes, Map.empty[String, String])
  }

  def enqueue(data: String, attributes: Map[String, String]): Unit = {
    enqueue(data.getBytes, attributes)
  }

  def enqueue(data: Array[Byte], attributes: Map[String, String]): Unit = {
    queue.enqueue(new FlowFile(data, attributes))
  }

  def enqueue(data: List[FlowFile]): Unit = {
    queue.enqueue(data:_*)
  }

  def run(iterations: Int = 1): Unit = {
    // procesory, do których należy zakolejkować dane wejściowe
    nodes.foreach(_.reset())
    val inputNodes = connections.filter(_.from == NifiFlow.INPUT_NODE).map(_.to).flatMap(nodeMap.get)
    val elements = queue.dequeueAll(_ => true)
    inputNodes.foreach(node => node.enqueue(elements))
    while (nodes.exists(n => n.isRunnable)) {
      nodes.filter(_.isRunnable).foreach(n => {
        n.run(iterations)
        fromConnectionsMap.getOrElse(n.name, List()).groupBy(_.relation).foreach {
          case (relation, connectionsForRelation) =>
            val files = n.getFromRelation(relation)
            connectionsForRelation.foreach(c => {
              result = result.withFlowFiles(c.id, files)
              if (c.to != NifiFlow.OUTPUT_NODE) {
                nodeMap.get(c.to).foreach(n => files.foreach(file => n.enqueue(file.toByteArray, file.getAttributes.toMap)))
              }
            })
        }
      })
    }
  }

  def executionResult: ExecutionResult = {
    result
  }

  def printFlowStats(): Unit = {
    var toNodes = connections.filter(_.from == NifiFlow.INPUT_NODE).map(_.to)
    while(toNodes.nonEmpty){
      val nextNodes = toNodes.flatMap(nodeMap.get).distinct
      nextNodes.foreach(printNodeStats)
      toNodes = nextNodes.flatMap(n => fromConnectionsMap.getOrElse(n.name, List.empty)).map(_.to)
    }
  }

  private def printNodeStats(node: Node): Unit ={
    println(s"${node.name}\tin=${node.getInputCount} out=${node.processor.getRelationships.map(_.getName).map(node.getFromRelationCount).sum}")
  }

}

private[tester] final class Node(val name: String, val processor: Processor, var properties: Map[String, String], var validateExpressions: Boolean) {
  private var runner: TestRunner = _
  private val wasRan = new AtomicBoolean(false)
  private var alreadyReturned: Set[Long] = Set()
  private val queueCounter: AtomicInteger = new AtomicInteger(0)

  def setVariable(name: String, value: String): Unit = {
    runner.setVariable(name, value)
  }

  def setProperty(name: String, value: String): Unit = {
    properties = properties + (name -> value)
  }

  def setValidateExpressions(validateExpressions: Boolean): Unit = {
    this.validateExpressions = validateExpressions
  }

  def isQueueEmpty: Boolean = {
    runner.isQueueEmpty
  }

  def isRunnable: Boolean = {
    !wasRan.get() && (!isQueueEmpty || !requiresInput)
  }

  private def requiresInput: Boolean = {
    val clazz = runner.getProcessor.getClass
    val inputRequirementPresent = clazz.isAnnotationPresent(classOf[InputRequirement])
    val inputRequirement = if (inputRequirementPresent) {
      clazz.getAnnotation(classOf[InputRequirement]).value
    } else {
      InputRequirement.Requirement.INPUT_ALLOWED
    }
    inputRequirement == InputRequirement.Requirement.INPUT_REQUIRED
  }

  def reset(): Unit = {
    wasRan.set(false)
  }

  def enqueue(flowFiles: Seq[FlowFile]): Unit = {
    flowFiles.foreach(enqueue)
  }

  def enqueue(flowFile: FlowFile): Unit = {
    enqueue(flowFile.data, flowFile.attributes)
  }
  def enqueue(data: String, attributes: Map[String, String]): Unit = {
    enqueue(data.getBytes(StandardCharsets.UTF_8), attributes)
  }

  def enqueue(data: Array[Byte], attributes: Map[String, String]): Unit = {
    runner.enqueue(data, attributes)
    queueCounter.incrementAndGet()
  }

  def run(iterations: Int): Unit = {
    if(wasRan.compareAndSet(false, true)){
      try {
        runner.run(iterations)
      } catch {
        case NonFatal(ex) =>
          throw new Exception(
            s"Error running node: $name with processor: ${processor.getClass.getName} and properties: $properties",
            ex)
      }
    }
  }

  def getFromRelation(relation: String): List[MockFlowFile] = {
    val results = runner.getFlowFilesForRelationship(relation).filterNot(f => alreadyReturned.contains(f.getId))
    alreadyReturned = alreadyReturned ++ results.map(_.getId)
    results.toList
  }

  def getInputCount: Int = {
    queueCounter.intValue()
  }

  def getFromRelationCount(relation: String): Int = {
    runner.getFlowFilesForRelationship(relation).length
  }

  def initRunner(controllerServices: List[Service]): TestRunner ={
    this.runner = newTestRunner(processor)
    runner.setValidateExpressionUsage(validateExpressions)
    properties.foreach {
      case (key, null) =>
        runner.removeProperty(key)
      case (key, value) =>
        runner.setProperty(key, value)
    }
    controllerServices.foreach(c => {
      runner.addControllerService(c.name, c.service)
      // niektóre service'y mają metodę "onPropertyModified" która zapewne jest wołana kiedy z GUI użytkownik zmieni
      // wartość, więc ustawiamy je w ten sposób jak poniżej zamiast przekazać mapę parametrów do metody addControllerService
      c.properties.foreach{
        case (key, value) => runner.setProperty(c.service, key, value)
      }
      runner.enableControllerService(c.service)
    })
    runner
  }
}
private[tester] final class Connection(val id: String, val from: String, val to: String, val relation: String, var queue: List[MockFlowFile] = List())

private[tester] final class Service(val name: String, val service: ControllerService, val properties: Map[String, String])

object FlowFile{
  def apply(data: String, attributes: Map[String, String] = Map.empty[String, String]): FlowFile = new FlowFile(data, attributes)
}

// data field has to be an array of bytes because converting it to String might corrupt the data
// the other way around seems to be less problematic
final case class FlowFile(data: Array[Byte], attributes: Map[String, String]){
  def this(data: String, attributes: Map[String, String] = Map.empty[String, String]) = this(data.getBytes, attributes)

  def this(mockFlowFile: MockFlowFile) = this(mockFlowFile.toByteArray, mockFlowFile.getAttributes.toMap)

}

final case class ExecutionResult(private val flowFilesByConnectionId: Map[String, List[MockFlowFile]], outputConnectionId: Option[String]) {

  private[tester] def withFlowFiles(connectionId: String, flowFiles: List[MockFlowFile]) =
    copy(flowFilesByConnectionId + (connectionId -> flowFiles))

  def outputFlowFiles: List[MockFlowFile] = flowFiles(outputConnectionId.getOrElse(throw new IllegalStateException("Missing output connection id")))

  def flowFiles(connectionId: String): List[MockFlowFile] = flowFilesByConnectionId.getOrElse(connectionId, List.empty)

}