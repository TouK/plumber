package pl.touk.plk.rtm.load.nifi.flow.tester.mock

import java.util

import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.processor._
import org.apache.nifi.processors.hadoop.PutHDFS
import org.apache.nifi.processors.hive.PutHiveQL
import scala.collection.JavaConversions._

// TODO add more mocks
class PutHDFSMock extends ProcessorMock(new PutHDFS)
class PutHiveQLMock extends ProcessorMock(new PutHiveQL)

class ProcessorMock(val instance: Processor) extends Processor{

  override def initialize(context: ProcessorInitializationContext): Unit = instance.initialize(context)

  override def getRelationships: util.Set[Relationship] = instance.getRelationships

  override def onTrigger(context: ProcessContext, sessionFactory: ProcessSessionFactory): Unit = {}

  override def validate(context: ValidationContext): util.Collection[ValidationResult] = List.empty[ValidationResult]

  override def getPropertyDescriptor(name: String): PropertyDescriptor = instance.getPropertyDescriptor(name)

  override def onPropertyModified(descriptor: PropertyDescriptor, oldValue: String, newValue: String): Unit = {}

  override def getPropertyDescriptors: util.List[PropertyDescriptor] = instance.getPropertyDescriptors

  override def getIdentifier: String = instance.getIdentifier
}