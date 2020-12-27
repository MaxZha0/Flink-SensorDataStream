package com.bainan.test

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaTest {
  def main(args: Array[String]): Unit = {
    val processName = "flinkKafkaTemplate"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val kafkaBrokers = "n001:9092,n002:9092,n003:9092"
    val consumerGroup = "flinkKafkaTest"
    val listenerTopic = "testtopic"
    val targetTopic = "testTargetTopic"

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaBrokers)
    properties.setProperty("group.id", consumerGroup)

    // id|name => {id:'',name:''}
    val splitChar = '|'
    val dataStream = env.addSource(new FlinkKafkaConsumer011[String](listenerTopic, new SimpleStringSchema(), properties))
    val resultStream = dataStream.map(_.split(splitChar)).filter(_.length == 2).map(array => s"{id:'${array(0)}', name:'${array(1)}-'}")

    resultStream.addSink(new FlinkKafkaProducer011[String](kafkaBrokers, targetTopic, new SimpleStringSchema()))
    env.execute(processName)
  }
}
