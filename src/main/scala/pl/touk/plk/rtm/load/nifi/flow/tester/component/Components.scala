package pl.touk.plk.rtm.load.nifi.flow.tester.component

import org.apache.nifi.processor.Relationship
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners.newTestRunner
import org.apache.nifi.web.api.dto.{ComponentDTO, FunnelDTO, PortDTO}
import pl.touk.plk.rtm.load.nifi.flow.tester.DummyProcessor

import scala.collection.JavaConversions._

private[tester] class DummyComponentWithQueue(component: ComponentDTO) extends ComponentWithQueue(component){
  val runner: TestRunner = newTestRunner(new DummyProcessor)
  override protected def internalRun(i: Int): Unit = {
    queue.dequeueAll(_ => true).foreach(e => runner.enqueue(e.data, e.attributes))
    runner.run(i)

    executionResult.setResultsForRelationship(Relationship.ANONYMOUS,
      runner.getFlowFilesForRelationship(Relationship.ANONYMOUS).toList)
  }

  override protected def isInnerQueueEmpty: Boolean = runner.isQueueEmpty
  override def requiresInput: Boolean = true
}

private[tester] class FunnelWithQueue(val funnel: FunnelDTO) extends DummyComponentWithQueue(funnel){
  override def toString = s"FunnelWithQueue(${funnel.getId})"
}

private[tester] class InputPortWithQueue(val port: PortDTO) extends DummyComponentWithQueue(port){
  override def toString = s"InputPortWithQueue(${port.getName})"
}

private[tester] class OutputPortWithQueue(val port: PortDTO) extends DummyComponentWithQueue(port){
  override def toString = s"OutputPortWithQueue(${port.getName})"
}



