package pl.touk.plk.rtm.load.nifi.flow.tester.component

import org.apache.nifi.web.api.dto.{ConnectionDTO, ProcessGroupDTO}
import pl.touk.plk.rtm.load.nifi.flow.tester.{FlowFile, NifiTemplateFlowFactory, Service}

import scala.collection.JavaConversions._

private[tester] class ProcessGroupWithQueue(val processGroup: ProcessGroupDTO, val parentVariables: Map[String, String], val controllerServices: List[Service]) extends  ComponentWithQueue(processGroup){
  private[tester] val nifiTemplateFlow = NifiTemplateFlowFactory(processGroup.getContents,processGroup.getName)
    .addVariables(parentVariables)
    .addVariables(processGroup.getVariables.toMap)
    .addControllerServices(controllerServices).create()

  def enqueueByName(flowFiles: Iterable[FlowFile], inputPortName: String): Unit = {
    nifiTemplateFlow.enqueueByName(flowFiles, inputPortName)
  }

  def enqueueById(flowFiles: Iterable[FlowFile], inputPortId: String): Unit = {
    nifiTemplateFlow.enqueueById(flowFiles, inputPortId)
  }

  override def isRunnable: Boolean = {
    nifiTemplateFlow.components.exists(_.isRunnable)
  }

  override def reset(): Unit = {
    nifiTemplateFlow.components.foreach(_.reset())
  }

  override def isInnerQueueEmpty: Boolean = {
    nifiTemplateFlow.components.forall(_.isQueueEmpty)
  }

  override protected def internalRun(i: Int): Unit = {
    nifiTemplateFlow.run(i)

    nifiTemplateFlow.outputPorts.toList
      .foreach(p => executionResult.setResultsForRelationship(p.port.getId, nifiTemplateFlow.resultsFromOutputPort(p.port.getName)))
  }

  // this method is overridden because process groups have input/output port id as destination/source instead of
  // own group id
  override protected def isSourceOf(connection: ConnectionDTO): Boolean = {
    processGroup.getId == connection.getSource.getGroupId
  }

  override protected def isDestinationOf(connection: ConnectionDTO): Boolean = {
    processGroup.getId == connection.getDestination.getGroupId
  }

  override protected def enqueueFromConnection(flowFiles: Iterable[FlowFile], connection: ConnectionDTO): Unit = {
    enqueueById(flowFiles, connection.getDestination.getId)
  }

  override protected def getRelationsFromConnection(connection: ConnectionDTO): List[String] = {
    List(connection.getSource.getId)
  }

  override def requiresInput: Boolean = {
    nifiTemplateFlow.components.forall(_.requiresInput)
  }

  override def toString = s"ProcessGroupWithQueue($nifiTemplateFlow)"
}