package pl.touk.plk.rtm.load.nifi.flow.tester

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.controller.ControllerService
import org.apache.nifi.processor.{Processor, Relationship}
import org.apache.nifi.processors.attributes.UpdateAttribute
import org.apache.nifi.processors.standard.LogAttribute

import scala.collection.mutable.ListBuffer

class NifiFlowBuilder {

  val connections: ListBuffer[Connection] = new ListBuffer[Connection]()
  val nodes: ListBuffer[Node] = new ListBuffer[Node]()
  val services: ListBuffer[Service] = new ListBuffer[Service]()
  var variables: Map[String, String] = Map()

  def setVariable(variable: String, value: String): NifiFlowBuilder = {
    if (variables.contains(variable)) {
      throw new IllegalArgumentException(s"Variable with name: $variable already declared")
    }
    variables += (variable -> value)
    this
  }

  private def addUpdateFailureContextAttributesNodeIfNotExists(fromNode: String, successToNode: String, successRelation: String): NifiFlowBuilder = {
    addNodeIfNotExists(NifiFlow.UPD_FAILURE_CTX_NODE, new UpdateAttribute, Map(
      "_fromNode" -> fromNode,
      "_successToNode" -> successToNode,
      "_successRelation" -> successRelation
    ))
  }

  private def addLogAttributeNodeIfNotExists(): NifiFlowBuilder = {
    addNodeIfNotExists(NifiFlow.LOG_ATTR_NODE, new LogAttribute, Map.empty)
  }

  private def addNodeIfNotExists(name: String, processor: Processor, properties: Map[String, String], validateExpressions: Boolean = true): NifiFlowBuilder = {
    if (nodes.exists(_.name == name)) {
      this
    } else {
      addNode(name, processor, properties, validateExpressions)
    }
  }

  def addDummyNode(name: String): NifiFlowBuilder = {
    addNode(name, new DummyProcessor, Map(), false)
  }

  def addNode(name: String, processor: Processor, properties: Map[String, String], validateExpressions: Boolean = true): NifiFlowBuilder = {
    if (nodes.exists(_.name == name)){
      throw new IllegalArgumentException(s"Node with name: $name already exists")
    }
    nodes += new Node(name, processor, properties, validateExpressions)
    this
  }

  def setNodeProperty(nodeName: String, property: PropertyDescriptor, value: String): NifiFlowBuilder = {
    setNodeProperty(nodeName, property.getName, value)
  }

  def setNodeProperty(nodeName: String, property: String, value: String): NifiFlowBuilder = {
    val node = getNodeOrThrowExceptionIfNotExists(nodeName)
    node.setProperty(property, value)
    this
  }

  def setNodeValidateExpressions(nodeName: String, validateExpressions: Boolean): NifiFlowBuilder = {
    val node = getNodeOrThrowExceptionIfNotExists(nodeName)
    node.setValidateExpressions(validateExpressions)
    this
  }

  def addConnection(from: String, to: String, relation: Relationship): NifiFlowBuilder = {
    addConnection(from, to, relation.getName)
  }

  def addSuccessAndLogFailureConnection(from: String, to: String): NifiFlowBuilder = {
    addSuccessAndLogFailureConnection(generateId(from, to, NifiFlow.SUCCESS_RELATION), from, to, NifiFlow.SUCCESS_RELATION)
  }

  def addSuccessAndLogFailureConnection(id: String, from: String, to: String, successRelation: String): NifiFlowBuilder = {
    addConnection(id, from, to, successRelation)
      .addLogFailurePart(from, to, successRelation)
  }

  private def addConnectionIfNotExists(from: String, to: String, relation: String): NifiFlowBuilder = {
    if (connections.exists(c => c.from == from && c.to == to && c.relation == relation)){
      this
    } else{
      addConnection(from, to, relation)
    }
  }


  def addConnection(from: String, to: String, relation: String): NifiFlowBuilder = {
    addConnection(generateId(from, to, relation), from, to, relation)
  }

  def addInputConnection(to: String): NifiFlowBuilder = {
    addInputConnection(generateId(NifiFlow.INPUT_NODE, to, ""), to)
  }

  private[tester] def addInputConnection(id: String, to: String): NifiFlowBuilder = {
    addConnection(id, NifiFlow.INPUT_NODE, to, "")
  }

  def addSuccessOutputAndLogFailureConnection(from: String, successRelation: String = "success"): NifiFlowBuilder = {
    addOutputConnection(from, successRelation)
      .addLogFailurePart(from, NifiFlow.OUTPUT_NODE, successRelation)
  }

  private def addLogFailurePart(from: String, to: String, successRelation: String) = {
    addLogAttributeNodeIfNotExists()
      .addUpdateFailureContextAttributesNodeIfNotExists(from, to, successRelation)
      .addConnection(from, NifiFlow.UPD_FAILURE_CTX_NODE, NifiFlow.FAILURE_RELATION)
      .addConnectionIfNotExists(NifiFlow.UPD_FAILURE_CTX_NODE, NifiFlow.LOG_ATTR_NODE, NifiFlow.SUCCESS_RELATION)
  }

  def addOutputConnection(from: String, relation: Relationship): NifiFlowBuilder = {
    addOutputConnection(from, relation.getName)
  }

  def addOutputConnection(from: String, relation: String): NifiFlowBuilder = {
    addOutputConnection(generateId(from, NifiFlow.OUTPUT_NODE, relation), from, relation)
  }

  private def generateId(from: String, to: String, relation: String) =
    s"_${from}_${to}_$relation"

  private[tester] def addOutputConnection(id: String, from: String, relation: String): NifiFlowBuilder = {
    addConnection(id, from, NifiFlow.OUTPUT_NODE, relation)
  }

  def addConnection(id: String, from: String, to: String, relation: String): NifiFlowBuilder = {
    if (connections.exists(_.id == id)) {
      throw new IllegalArgumentException(s"Connection with id: $id already exists")
    }
    if (from != NifiFlow.INPUT_NODE) {
      getNodeOrThrowExceptionIfNotExists(from)
    }
    if (to != NifiFlow.OUTPUT_NODE) {
      getNodeOrThrowExceptionIfNotExists(to)
    }
    connections += new Connection(id, from, to, relation)
    this
  }

  private def getNodeOrThrowExceptionIfNotExists(name: String): Node = {
    nodes.find(_.name == name).getOrElse {
      throw new IllegalArgumentException(s"Node with name: $name not exists")
    }
  }

  def addControllerService(name: String, controllerService: ControllerService, properties: Map[String, String]): NifiFlowBuilder = {
    if (services.exists(_.name == name)) {
      throw new IllegalArgumentException(s"Service with name: $name already exists")
    }
    services += new Service(name, controllerService, properties)
    this
  }

  def build(): NifiFlow = {
    new NifiFlow(nodes.toList, connections.toList, services.toList, variables)
  }
}
