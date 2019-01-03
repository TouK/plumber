package pl.touk.plk.rtm.load.nifi.flow.tester

import java.util

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.controller.ControllerService
import org.apache.nifi.processor.{Processor, Relationship}
import org.apache.nifi.processors.standard.{JoltTransformJSON, LogMessage, ReplaceText}
import org.apache.nifi.util.MockFlowFile
import org.apache.nifi.web.api.dto._
import pl.touk.plk.rtm.load.nifi.flow.tester.NifiTemplateFlowFactory._
import pl.touk.plk.rtm.load.nifi.flow.tester.component._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

object NifiTemplateFlowFactory {

  def apply(template: TemplateDTO): NifiTemplateFlowFactory = {
    new NifiTemplateFlowFactory(template.getSnippet, template.getName)
  }

  def apply(snippet: FlowSnippetDTO, name: String): NifiTemplateFlowFactory = {
    new NifiTemplateFlowFactory(snippet, name)
  }

  private[tester] def findExactlyOneByName[T](elements: Iterable[T], nameExtractor: T => String, name: String, elementType: String): T = {
    val found = elements.filter(nameExtractor(_) == name)
    if (found.size > 1) {
      throw new IllegalArgumentException(s"Found more than one ${elementType.toLowerCase()} with name '$name' - change ${elementType.toLowerCase()} names in your flow or use `ById` method")
    } else if (found.isEmpty) {
      throw new IllegalArgumentException(s"$elementType with name '$name' not found")
    }
    found.head
  }
}

class NifiTemplateFlowFactory(val snippet: FlowSnippetDTO, val name: String) {

  var variables: util.Map[String, String] = new mutable.HashMap[String, String]()
  var services: List[Service] = List.empty

  def addVariable(name: String, value: String): NifiTemplateFlowFactory = {
    variables = variables + (name -> value)
    this
  }

  def addVariables(variables: Map[String, String]): NifiTemplateFlowFactory = {
    this.variables = this.variables ++ variables
    this
  }

  def addControllerService(name: String, service: ControllerService, parameters: Map[String, String]): NifiTemplateFlowFactory = {
    services = new Service(name, service, parameters) :: services
    this
  }

  def addControllerServices(services: List[Service]): NifiTemplateFlowFactory = {
    this.services = services ++ this.services
    this
  }

  def replaceControllerService(name: String, service: ControllerService, parameters: Map[String, String]): NifiTemplateFlowFactory = {
    val found = findExactlyOneByName[ControllerServiceDTO](snippet.getControllerServices, _.getName, name, "Controller service")
    services = new Service(name, service, found.getProperties.toMap) :: services
    snippet.setControllerServices(snippet.getControllerServices.filter(_.getId != found.getId).toSet.asJava)
    this
  }

  def replaceProcessorType(name: String, newType: Class[_]): NifiTemplateFlowFactory = {
    replaceProcessorType(name, newType.getName)
  }

  def replaceProcessorType(name: String, newType: String): NifiTemplateFlowFactory = {
    val found = findExactlyOneByName[ProcessorDTO](snippet.getProcessors, _.getName, name, "Processor")
    found.setType(newType)
    this
  }

  def setProcessorProperty(processorName: String, property: PropertyDescriptor, value: String): NifiTemplateFlowFactory = {
    setProcessorProperty(processorName, property.getName, value)
  }

  def setProcessorProperty(processorName: String, property: String, value: String): NifiTemplateFlowFactory = {
    findExactlyOneByName[ProcessorDTO](snippet.getProcessors, _.getName, processorName, "Processor")
      .getConfig.getProperties.put(property, value)
    this
  }

  def getProcessGroup(name: String): NifiTemplateFlowFactory = {
    val processGroup = findExactlyOneByName[ProcessGroupDTO](snippet.getProcessGroups, _.getName, name, "Process group")
    val builder = NifiTemplateFlowFactory(processGroup.getContents, processGroup.getName)
    builder.variables = processGroup.getVariables
    builder
  }

  def getProcessGroupByPath(path: String): NifiTemplateFlowFactory = {
    val elements = path.split("/")
    var current = this
    for (name <- elements) {
      current = current.getProcessGroup(name)
    }
    current
  }

  def create(): NifiTemplateFlow = {
    val controllers = services ++ snippet.getControllerServices.toList
      .map(c => new Service(c.getId, Class.forName(c.getType).newInstance().asInstanceOf[ControllerService], c.getProperties.toMap))
    new NifiTemplateFlow(name, snippet.getConnections.toSet, snippet.getInputPorts.map(new InputPortWithQueue(_)).toSet,
      snippet.getOutputPorts.map(new OutputPortWithQueue(_)).toSet,
      snippet.getFunnels.map(new FunnelWithQueue(_)).toSet,
      snippet.getProcessors.map(new ProcessorWithQueue(_, variables.toMap, controllers)).toSet,
      snippet.getProcessGroups.map(new ProcessGroupWithQueue(_, variables.toMap, controllers)).toSet)
  }

}

class NifiTemplateFlow(val name: String, val connections: Set[ConnectionDTO], val inputPorts: Set[InputPortWithQueue],
                       val outputPorts: Set[OutputPortWithQueue], val funnels: Set[FunnelWithQueue],
                       val processors: Set[ProcessorWithQueue], val processGroups: Set[ProcessGroupWithQueue]) {
  private[tester] val components: Set[ComponentWithQueue] = inputPorts ++ outputPorts ++ funnels ++ processors ++ processGroups
  private val componentIdToComponentMap: Map[String, ComponentWithQueue] = initComponentMap()

  //TODO: change this to enqueueToInputPortByName and add enqueueToProcessorByName ?
  // or maybe allow this method do enqueue data to any node (input/output/processor/group)
  def enqueueByName(data: String, inputPortName: String): Unit = {
    enqueueByName(data, Map(), inputPortName)
  }

  def enqueueByName(data: String, attributes: Map[String, String], inputPortName: String): Unit = {
    enqueueByName(new FlowFile(data, attributes), inputPortName)
  }

  def enqueueByName(flowFile: FlowFile, inputPortName: String): Unit = {
    enqueueByName(List(flowFile), inputPortName)
  }

  def enqueueByName(flowFiles: Iterable[FlowFile], inputPortName: String): Unit = {
    val inputPort = findExactlyOneByName[InputPortWithQueue](inputPorts, _.port.getName, inputPortName, "Input port")
    enqueueById(flowFiles, inputPort.port.getId)
  }

  def enqueueById(flowFiles: Iterable[FlowFile], nodeId: String): Unit = {
    componentIdToComponentMap.get(nodeId).foreach(c => flowFiles.foreach(f => c.enqueue(f)))
  }

  def setProcessorProperty(processorName: String, property: PropertyDescriptor, value: String): NifiTemplateFlow = {
    setProcessorProperty(processorName, property.getName, value)
  }

  def setProcessorProperty(processorName: String, property: String, value: String): NifiTemplateFlow = {
    findExactlyOneByName[ProcessorWithQueue](processors, _.processor.getName, processorName, "Processor")
      .setProperty(property, value)
    this
  }

  // TODO - setValidateExpressionUsage methods should be implemented in builder class, not here
  def setValidateExpressionUsage(processorName: String, validate: Boolean): NifiTemplateFlow = {
    findExactlyOneByName[ProcessorWithQueue](processors, _.processor.getName, processorName, "Processor")
      .setValidateExpressionUsage(validate)
    this
  }

  def setValidateExpressionUsageByType(processorType: Class[_ <: Processor], validate: Boolean, recursive: Boolean): NifiTemplateFlow = {
    setValidateExpressionUsageByType(processorType.getName, validate, recursive)
  }

  def setValidateExpressionUsageByType(processorType: String, validate: Boolean, recursive: Boolean): NifiTemplateFlow = {
    processors.filter(_.processor.getType == processorType).foreach(_.setValidateExpressionUsage(validate))
    if (recursive) {
      processGroups.map(_.nifiTemplateFlow)
        .foreach(_.setValidateExpressionUsageByType(processorType, validate, recursive))
    }
    this
  }

  // this method searches for all problematic processors and disables their's validation
  // maybe name could be more expressive?
  def automaticallyDisableValidateExpressionUsage(): NifiTemplateFlow = {
    val classes: List[Class[_ <: Processor]] = List(classOf[LogMessage], classOf[ReplaceText], classOf[JoltTransformJSON])
    classes.foreach(setValidateExpressionUsageByType(_, false, true))
    this
  }

  private def initComponentMap(): Map[String, ComponentWithQueue] = {
    components.map(c => c.component.getId -> c).toMap
  }

  def run(iterations: Int = 1): Unit = {
    components.foreach(_.reset())
    while (existsRunnableNode) {
      components.filter(_.isRunnable).foreach(c => {
        c.run(iterations)
        c.enqueueResultsTo(connections, components)
      })
    }
  }

  def getProcessGroup(name: String): NifiTemplateFlow = {
    findExactlyOneByName[ProcessGroupWithQueue](processGroups, _.processGroup.getName, name, "Process group").nifiTemplateFlow
  }

  def getProcessGroupByPath(path: String): NifiTemplateFlow = {
    val elements = path.split("/")
    var current = this
    for (name <- elements) {
      current = current.getProcessGroup(name)
    }
    current
  }

  def resultsFromOutputPort(outputPortName: String): List[MockFlowFile] = {
    findExactlyOneByName[OutputPortWithQueue](outputPorts, _.port.getName, outputPortName, "Output port")
      .executionResult.allFromRelationship(Relationship.ANONYMOUS.getName)
  }

  def resultsFromProcessorRelation(processorName: String, relation: String): List[MockFlowFile] = {
    findExactlyOneByName[ProcessorWithQueue](processors, _.processor.getName, processorName, "Processor")
      .executionResult.allFromRelationship(relation)
  }

  private def existsRunnableNode: Boolean = {
    components.exists(_.isRunnable)
  }

  override def toString = s"NifiTemplateFlow($name)"
}

