package pl.touk.plk.rtm.load.nifi.flow.tester.component

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.nifi.processor.Relationship
import org.apache.nifi.util.{MockFlowFile, TestRunner}
import org.apache.nifi.web.api.dto._
import pl.touk.plk.rtm.load.nifi.flow.tester._

import scala.collection.JavaConversions._
import scala.collection.mutable

private[tester] abstract class ComponentWithQueue(val component: ComponentDTO){

  protected[tester] val queue: mutable.Queue[FlowFile] = new mutable.Queue[FlowFile]()
  protected[tester] val executionResult: ExecutionResults = new ExecutionResults
  private val wasRan = new AtomicBoolean(false)

  def isQueueEmpty: Boolean = {
    queue.isEmpty && isInnerQueueEmpty
  }

  def requiresInput: Boolean
  protected def isInnerQueueEmpty: Boolean

  def isRunnable: Boolean = {
    !wasRan.get() && (!requiresInput || (requiresInput && !isQueueEmpty))
  }

  def reset(): Unit = {
    wasRan.set(false)
  }

  def enqueue(flowFile: MockFlowFile): Unit = {
    queue.enqueue(new FlowFile(flowFile))
  }

  def enqueue(flowFile: FlowFile): Unit ={
    queue.enqueue(flowFile)
  }

  def enqueue(flowFiles: Iterable[FlowFile]): Unit ={
    flowFiles.foreach(enqueue)
  }

  final def run(iterations: Int = 1): Unit = {
    if(isRunnable) {
      wasRan.set(true)
      internalRun(iterations)
    }
  }

  protected def internalRun(i: Int): Unit

  protected def isSourceOf(connection: ConnectionDTO): Boolean = {
    component.getId == connection.getSource.getId
  }

  protected def isDestinationOf(connection: ConnectionDTO): Boolean = {
    component.getId == connection.getDestination.getId
  }

  protected def enqueueFromConnection(flowFiles: Iterable[FlowFile], connection: ConnectionDTO): Unit = {
    enqueue(flowFiles)
  }

  def enqueueResultsTo(connections: Set[ConnectionDTO], components: Set[ComponentWithQueue]): Unit = {
    for{
      connection <- connections.filter(isSourceOf)
      componentToEnqueue <- components.find(_.isDestinationOf(connection))
    } yield {
      componentToEnqueue.enqueueFromConnection(getResultsToEnqueue(connection), connection)
    }
  }

  protected def getRelationsFromConnection(connection: ConnectionDTO): List[String] = {
    Option(connection.getSelectedRelationships).map(_.toList).getOrElse(List(""))
  }

  protected def getResultsToEnqueue(connection: ConnectionDTO): Iterable[FlowFile] = {
    getRelationsFromConnection(connection)
      .flatMap(executionResult.newFromRelationship)
  }

}
