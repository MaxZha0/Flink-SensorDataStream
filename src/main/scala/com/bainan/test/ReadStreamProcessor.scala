package com.bainan.test

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import java.util.Properties

/**
 * ./bin/flink run -m yarn-cluster  -yjm 1024 -ytm 1024 -c com.bainan.test.ReadStreamProcessor /home/bigdata/flinkApp/flinkAlert/flink-alert-1.0-SNAPSHOT-jar-with-dependencies.jar
 * @author Max
 */
object ReadStreamProcessor {
  implicit val formats = Serialization.formats(NoTypeHints)

  def main(args: Array[String]): Unit = {
    val processName = "kafka_read_stream_processor"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaBrokers = "222.25.172.64:9092,n001:9092,n002:9092,n003:9092"
    val consumerGroup = "readData"
    val voltListenerTopic = "VoltDataTopic"
    val currListenerTopic = "CurrDataTopic"
    val tempListenerTopic = "TempDataTopic"
    val amplListenerTopic = "AmplDataTopic"
    val frepListenerTopic = "FreqDataTopic"
    //接受topic
    val targetTopic = "plcStatusTopic"
    //检测阈值
    val thresholdValue = 5.97
    //窗口时间
    val windowTime = 30
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaBrokers)
    properties.setProperty("group.id", consumerGroup)
    //sink
    val sink = new FlinkKafkaProducer011[String](kafkaBrokers, targetTopic, new SimpleStringSchema())
    env.setParallelism(3)
    //source数据流
    val voltDataStream = env.addSource(new FlinkKafkaConsumer011[String](voltListenerTopic, new SimpleStringSchema(), properties))
      .map(data =>{val dataArray = data.split("\t")
        Message(dataArray(0), dataArray(1), dataArray(2), dataArray(3), dataArray(4).toDouble)})
    val currDataStream = env.addSource(new FlinkKafkaConsumer011[String](currListenerTopic, new SimpleStringSchema(), properties))
      .map(data =>{val dataArray = data.split("\t")
        Message(dataArray(0), dataArray(1), dataArray(2), dataArray(3), dataArray(4).toDouble)})
    val tempDataStream = env.addSource(new FlinkKafkaConsumer011[String](tempListenerTopic, new SimpleStringSchema(), properties))
      .map(data =>{val dataArray = data.split("\t")
        Message(dataArray(0), dataArray(1), dataArray(2), dataArray(3), dataArray(4).toDouble)})
    val amplDataStream = env.addSource(new FlinkKafkaConsumer011[String](amplListenerTopic, new SimpleStringSchema(), properties))
      .map(data =>{val dataArray = data.split("\t")
        Message(dataArray(0), dataArray(1), dataArray(2), dataArray(3), dataArray(4).toDouble)})
    val freqDataStream = env.addSource(new FlinkKafkaConsumer011[String](frepListenerTopic, new SimpleStringSchema(), properties))
      .map(data =>{val dataArray = data.split("\t")
        Message(dataArray(0), dataArray(1), dataArray(2), dataArray(3), dataArray(4).toDouble)})

    //分流，进行判断后分流
    val voltSplitStream = voltDataStream.split( data =>{
      data.machineType match {
        case "动力机械臂" => Seq("d-Volt-Stream")
        case "轴承电动机" => Seq("z-Volt-Stream")
        case "AGV运载车" => Seq("a-Volt-Stream")
      }
    })
    val currSplitStream = currDataStream.split( data =>{
      data.machineType match {
        case "动力机械臂" => Seq("d-Curr-Stream")
        case "轴承电动机" => Seq("z-Curr-Stream")
        case "AGV运载车" => Seq("a-Curr-Stream")
      }
    })
    val tempSplitStream = tempDataStream.split( data =>{
      data.machineType match {
        case "轴承电动机" => Seq("z-Temp-Stream")
      }
    })
    val amplSplitStream = amplDataStream.split( data =>{
      data.machineType match {
        case "轴承电动机" => Seq("z-Ampl-Stream")
        case "AGV运载车" => Seq("a-Ampl-Stream")
      }
    })
    val freqSplitStream = freqDataStream.split( data =>{
      data.machineType match {
        case "轴承电动机" => Seq("z-Freq-Stream")
        case "AGV运载车" => Seq("a-Freq-Stream")
      }
    })

    dataStreamProcessor("d-Volt-Stream", voltSplitStream, thresholdValue, sink, windowTime)
    dataStreamProcessor("z-Volt-Stream", voltSplitStream, thresholdValue, sink, windowTime)
    dataStreamProcessor("a-Volt-Stream", voltSplitStream, thresholdValue, sink, windowTime)
    dataStreamProcessor("d-Curr-Stream", currSplitStream, thresholdValue, sink, windowTime)
    dataStreamProcessor("z-Curr-Stream", currSplitStream, thresholdValue, sink, windowTime)
    dataStreamProcessor("a-Curr-Stream", currSplitStream, thresholdValue, sink, windowTime)
    dataStreamProcessor("z-Temp-Stream", tempSplitStream, thresholdValue, sink, windowTime)
    dataStreamProcessor("z-Ampl-Stream", amplSplitStream, thresholdValue, sink, windowTime)
    dataStreamProcessor("a-Ampl-Stream", amplSplitStream, thresholdValue, sink, windowTime)
    dataStreamProcessor("z-Freq-Stream", freqSplitStream, thresholdValue, sink, windowTime)
    dataStreamProcessor("a-Freq-Stream", freqSplitStream, thresholdValue, sink, windowTime)


    env.execute(processName)
  }

  /**
   * 逻辑代码函数
   * @param name
   * @param consumer source
   * @param env 环境
   * @param thresholdValue 检测临界值变量
   * @param sink  sink
   * @author Max
   */
  def dataStreamProcessor(name : String,
                          dataStream: SplitStream[Message],
                          thresholdValue : Double,
                          sink : FlinkKafkaProducer011[String],
                          windowTime : Int) : Unit= {
    //开窗
    val alertStream = dataStream
      .select(name)
      .keyBy(data => data.machineNumber)
      .timeWindow(Time.seconds(windowTime))  //定义一个30秒的翻滚窗口
      .trigger(new MyTrigger(thresholdValue))  //检测阈值为5
      .process(new MyProcessor(name))

    //变成JsonString流
    val jsonStrStream = alertStream
      .map(data => Serialization.write(data) ) //包装json

    jsonStrStream.addSink(sink).setParallelism(1)
    jsonStrStream.print()

  }
}

