package pl.touk.plk.rtm.load.nifi.flow.tester.component

import org.apache.nifi.processor.Relationship
import org.apache.nifi.util.{MockFlowFile, TestRunners}
import org.scalatest.{FunSuite, Matchers}
import pl.touk.plk.rtm.load.nifi.flow.tester.{DummyProcessor, FlowFile}

class ExecutionResultsSpec extends FunSuite with Matchers {

  val relationship: String = "test123"
  val notExistingRelationship: String = "these are not the relationships you are looking for"
  val flowFile42 = new MockFlowFile(42)
  val flowFile33 = new MockFlowFile(33)

  test("return all results"){
    // given
    val results = executionResults()

    // expect
    results.allFromRelationship(relationship) shouldBe List(flowFile42)
    results.allFromRelationship(relationship) shouldBe List(flowFile42)
  }

  test("return empty list for not existing relationship"){
    val results = executionResults()

    results.allFromRelationship(notExistingRelationship) shouldBe List()
    results.allFromRelationship(notExistingRelationship) shouldBe List()
  }


  test("return only new results"){
    val results = executionResults()

    results.newFromRelationship(relationship).map(_.attributes.get("uuid")) shouldBe List(new FlowFile(flowFile42).attributes.get("uuid"))
    results.newFromRelationship(relationship) shouldBe List()

    results.setResultsForRelationship(relationship, List(flowFile42, flowFile33))
    results.newFromRelationship(relationship).map(_.attributes.get("uuid")) shouldBe List(new FlowFile(flowFile33).attributes.get("uuid"))
    results.newFromRelationship(relationship) shouldBe List()

  }

  test("set results from runner"){
    // given
    val runner = TestRunners.newTestRunner(new DummyProcessor)
    runner.enqueue(flowFile42)
    runner.run(1)
    val results = executionResults()

    // when
    results.setResultsFromRunner(runner)

    // then
    results.allFromRelationship(Relationship.ANONYMOUS.getName) shouldBe List(flowFile42)
  }


  private def executionResults(): ExecutionResults = {
    val results = new ExecutionResults
    results.setResultsForRelationship(relationship, List(flowFile42))
    results
  }
}
