package pl.touk.plk.rtm.load.nifi.flow.tester

import java.io.{InputStream, OutputStream}
import java.util

import org.apache.commons.io.IOUtils
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.processor.{AbstractProcessor, ProcessContext, ProcessSession, Relationship}

import scala.collection.JavaConversions._

// simple dummy processor which only job is to pass flow file without changing it
private[tester] class DummyProcessor extends AbstractProcessor {

  override def getRelationships: util.Set[Relationship] = Set(Relationship.ANONYMOUS)

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    var flowFile = session.get
    if (flowFile == null) return

    flowFile = session.write(flowFile, new StreamCallback() {
      override def process(in: InputStream, out: OutputStream): Unit = {
        IOUtils.copy(in, out)
      }
    })

    flowFile = session.putAllAttributes(flowFile, flowFile.getAttributes)
    session.transfer(flowFile, Relationship.ANONYMOUS)

  }
}