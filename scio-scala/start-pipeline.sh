#!/usr/bin/env bash

if [ "$#" -ge 1 ]
then
  streaming=true
  echo "Start pipeline in streaming modus"
else
  streaming=false
  echo "Start pipeline in batch modus"
fi

sbt "runMain de.thbrunzendorf.scio.characters.CountingPipeline \
 --runner=DirectRunner \
 --inputPath=target/scala-2.12/test-classes/input.txt \
 --outputPath=target/scala-2.12/test-classes/output.txt \
 --streaming=$streaming"
