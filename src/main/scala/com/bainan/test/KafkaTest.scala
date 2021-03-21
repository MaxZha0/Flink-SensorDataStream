package com.bainan.test

import java.util.Properties

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

import scala.sys.process._

object KafkaTest {
  def main(args: Array[String]): Unit = {
    val processName = "flinkKafkaTemplate"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.registerCachedFile("file:///home/bigdata/packages/flink/flink-1.10.1/test.py","test_python")

    val kafkaBrokers = "n001:9092,n002:9092,n003:9092"
    val consumerGroup = "flinkKafkaTest"
    val listenerTopic = "picCvProcessTopic"
    val targetTopic = "testTargetTopic"

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaBrokers)
    properties.setProperty("group.id", consumerGroup)

    // id|name => {id:'',name:''}
    val splitChar = '|'
    val dataStream = env.addSource(new FlinkKafkaConsumer011[String](listenerTopic, new SimpleStringSchema(), properties))
    val resultStream = dataStream.map(_.split(splitChar))
      .filter(_.length == 2)
      .map(new CustomMapTest)
      .map(array => s"{id:'${array(0)}', name:'${array(1)}'}")

    resultStream.addSink(new FlinkKafkaProducer011[String](kafkaBrokers, targetTopic, new SimpleStringSchema()))
    env.execute(processName)
  }

  class CustomMapTest extends RichMapFunction[Array[String], Array[String]] {
    var filePth:String = ""
    override def open(parameters: Configuration): Unit = {
      val pythonFile = getRuntimeContext.getDistributedCache.getFile("test_python")
      filePth = pythonFile.getAbsolutePath
    }

    override def map(in: Array[String]): Array[String] = {
      val exec = s"python3 $filePth"
      val process_test = exec.!!
      Array(in(0), s"name=${in(1)}-path:${filePth}-python3:$process_test")
    }
  }
}
