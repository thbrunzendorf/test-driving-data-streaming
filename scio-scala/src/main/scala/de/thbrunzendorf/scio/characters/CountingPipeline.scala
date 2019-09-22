package de.thbrunzendorf.scio.characters

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.options.{Description, PipelineOptions, PipelineOptionsFactory, Validation}

object CountingPipeline {

  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val options = getOptions(cmdlineArgs)
    val inputPath = options.getInputPath
    val outputPath = options.getOutputPath

    val inputs: SCollection[String] = sc.textFile(inputPath)
    val outputs: SCollection[(Char, Long)] = transform(inputs)
    outputs.saveAsTextFile(outputPath)

    sc.close()
  }

  def transform(inputs: SCollection[String]): SCollection[(Char, Long)] = {
    inputs.flatMap(x => x.toCharArray())
      .filter(x => x.isLetter)
      .map(x => x.toUpper)
      .countByValue
  }

  trait Options extends PipelineOptions {

    @Description("Input File path")
    @Validation.Required def getInputPath: String
    def setInputPath(inputPath: String): Unit

    @Description("Output File path")
    @Validation.Required def getOutputPath: String
    def setOutputPath(outputPath: String): Unit
  }

  def getOptions(args: Array[String]): CountingPipeline.Options = {
    PipelineOptionsFactory.fromArgs(args:_*).withValidation.as(classOf[CountingPipeline.Options])
  }
}
