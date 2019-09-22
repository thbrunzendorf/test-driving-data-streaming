package de.thbrunzendorf.scio.characters

import com.spotify.scio.testing.{PipelineSpec, TextIO}

class CountingPipelineSpec extends PipelineSpec {

  val inputPath = "input.txt"
  val outputPath = "output.txt"

  "HelloPipeline" should "work" in {
    val input: Seq[String] = Seq(
      "Hello ",
      "World!")
    val expected: Seq[String] = Seq("(H,1)", "(E,1)", "(L,3)", "(O,2)", "(W,1)", "(R,1)", "(D,1)")
    JobTest[CountingPipeline.type]
      .args(s"--inputPath=$inputPath", s"--outputPath=$outputPath")
      .input(TextIO(inputPath), input)
      .output(TextIO(outputPath)) { actual => actual should (haveSize(7) and containInAnyOrder(expected)) }
      .run()
  }

  "Transform" should "work" in {
    val input: Seq[String] = Seq(
      "Hello ",
      "World!")
    val expected: Seq[(Char, Long)] = Seq(('H',1), ('E',1), ('L',3), ('O',2), ('W',1), ('R',1), ('D',1))
    runWithData(input) { p =>
      CountingPipeline.transform(p)
    } should (have length(7) and contain theSameElementsAs(expected))
  }

}
