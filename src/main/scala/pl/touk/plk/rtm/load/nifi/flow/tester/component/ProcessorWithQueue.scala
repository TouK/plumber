package pl.touk.plk.rtm.load.nifi.flow.tester.component

import org.apache.nifi.annotation.behavior.InputRequirement
import org.apache.nifi.processor.Processor
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners.newTestRunner
import org.apache.nifi.web.api.dto.ProcessorDTO
import pl.touk.plk.rtm.load.nifi.flow.tester.Service

import scala.collection.JavaConversions._

private[tester] class ProcessorWithQueue(val processor: ProcessorDTO, val variables: Map[String, String], val controllerServices: List[Service]) extends  ComponentWithQueue(processor){
  private var runner: TestRunner = initRunner(controllerServices)

  private def initRunner(controllerServices: List[Service]): TestRunner ={
    this.runner = newTestRunner(Class.forName(processor.getType).newInstance().asInstanceOf[Processor])
    variables.foreach{
      case (key, value) => runner.setVariable(key, value)
    }
    processor.getConfig.getProperties.foreach {
      case (key, null) =>
        runner.removeProperty(key)
      case (key, value) =>
        runner.setProperty(key, value)
    }
    controllerServices.foreach(c => runner.addControllerService(c.name, c.service, c.properties))
    controllerServices.foreach(c => runner.enableControllerService(c.service))
    runner
  }

  def setProperty(property: String, value: String):Unit = {
    if(value == null){
      removeProperty(property)
    }else{
      runner.setProperty(property, value)
      processor.getConfig.getProperties.put(property, value)
    }
  }

  def removeProperty(property: String): Unit = {
    runner.removeProperty(property)
    processor.getConfig.getProperties.remove(property)
  }

  def setValidateExpressionUsage(validate: Boolean): Unit = {
    runner.setValidateExpressionUsage(validate)
  }

  override def internalRun(iterations: Int): Unit = {
    queue.dequeueAll(_ => true).foreach(e => runner.enqueue(e.data, e.attributes))
    runner.run(iterations)
    executionResult.setResultsFromRunner(runner)
  }

  override protected def isInnerQueueEmpty: Boolean = runner.isQueueEmpty

  override def requiresInput: Boolean = {
    val clazz = runner.getProcessor.getClass
    val inputRequirementPresent = clazz.isAnnotationPresent(classOf[InputRequirement])
    val inputRequirement = if (inputRequirementPresent) {
      clazz.getAnnotation(classOf[InputRequirement]).value
    } else {
      InputRequirement.Requirement.INPUT_ALLOWED
    }
    inputRequirement == InputRequirement.Requirement.INPUT_REQUIRED
  }


  override def toString = s"ProcessorWithQueue(${processor.getName})"
}
