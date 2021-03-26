package com.bainan.test

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import java.util.Properties


object KfkVolt {
  implicit val formats = Serialization.formats(NoTypeHints)
  //消息样例类
  case class Message(time : String , name : String , value : Double)
  case class ResultJson(info : String, value: Double, warningTime : String)

  def main(args: Array[String]): Unit = {

    val processName = "kafka_read_vol"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaBrokers = "n000:9092,n001:9092,n002:9092,n003:9092"
    val consumerGroup = "read_vol"
    val listenerTopic = "DataGenerationTopic"
    val targetTopic = "VoltAlertTopic"

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaBrokers)
    properties.setProperty("group.id", consumerGroup)

    // Source
    val consumer = new FlinkKafkaConsumer011[String](listenerTopic, new SimpleStringSchema(), properties)
    val dataStream = env.addSource(consumer)

    //分割stream 转换成 Message 格式Stream
    val messageStream = dataStream.map(data =>{
      val dataArray = data.split("\\+")
      Message(dataArray(0), dataArray(1), dataArray(2).toDouble)
    })

    //分流，进行判断后分流
    val splitStream = messageStream.split( data =>{
      if(data.name == "volt" && data.value >= 5.00)
        Seq("alertStream")
      else
        Seq("othersStream")
    })

//    val othersStream = splitStream.select("othersStream")
    //分离出来的流 开窗
    val alertStream = splitStream.select("alertStream")
      .keyBy(0)
      .timeWindow(Time.seconds(30))  //定义一个30秒的翻滚窗口
      .reduce((_, y) => Message(y.time, y.name, y.value)) //不做修改直接聚合
      .map(data => ResultJson("警告！电压值超过阈值！",data.value, data.time)) //修改格式

    //变成JsonString流
    val jsonStrStream = alertStream.map(data =>{
      Serialization.write(data)
    })

    jsonStrStream.addSink(new FlinkKafkaProducer011[String](kafkaBrokers, targetTopic, new SimpleStringSchema()))

    env.execute(processName)

  }
}

/**
 * ./bin/flink run -m yarn-cluster  -yjm 1024 -ytm 1024 -c com.bainan.test.KfkVolt /home/bigdata/flinkApp/flinkAlert/flink-alert-1.0-SNAPSHOT-jar-with-dependencies.jar
将其中的jar包替换成自身jar包名称， -c 后跟着的是Main函数的全类名也需要替换
 */

