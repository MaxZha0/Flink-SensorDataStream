package com.bainan.test

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import java.util.Properties

/**
 * ./bin/flink run -m yarn-cluster  -yjm 1024 -ytm 1024 -c com.bainan.test.KfkVolt /home/bigdata/flinkApp/flinkAlert/flink-alert-1.0-SNAPSHOT-jar-with-dependencies.jar
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
    val thresholdValue = 5.00
    //窗口时间
    val windowTime = 30
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaBrokers)
    properties.setProperty("group.id", consumerGroup)
    //sink
    val sink = new FlinkKafkaProducer011[String](kafkaBrokers, targetTopic, new SimpleStringSchema())
    //source数据流
    val voltDataStream = env.addSource(new FlinkKafkaConsumer011[String](voltListenerTopic, new SimpleStringSchema(), properties))
    val currDataStream = env.addSource(new FlinkKafkaConsumer011[String](currListenerTopic, new SimpleStringSchema(), properties))
    val tempDataStream = env.addSource(new FlinkKafkaConsumer011[String](tempListenerTopic, new SimpleStringSchema(), properties))
    val amplDataStream = env.addSource(new FlinkKafkaConsumer011[String](amplListenerTopic, new SimpleStringSchema(), properties))
    val freqDataStream = env.addSource(new FlinkKafkaConsumer011[String](frepListenerTopic, new SimpleStringSchema(), properties))

    dataStreamProcessor("volt", voltDataStream, thresholdValue, sink, windowTime)
    dataStreamProcessor("curr", currDataStream, thresholdValue, sink, windowTime)
    dataStreamProcessor("temp", tempDataStream, thresholdValue, sink, windowTime)
    dataStreamProcessor("ampl", amplDataStream, thresholdValue, sink, windowTime)
    dataStreamProcessor("freq", freqDataStream, thresholdValue, sink, windowTime)

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
                          //                          consumer : FlinkKafkaConsumer011[String],
                          //                          env :  StreamExecutionEnvironment,
                         dataStream: DataStream[String],
                          thresholdValue : Double,
                          sink : FlinkKafkaProducer011[String],
                          windowTime : Int) : Unit= {

    //分割stream 转换成 Message 格式Stream
    val messageStream = dataStream.map(data =>{
      val dataArray = data.split("\\+")
      Message(dataArray(0), dataArray(1), dataArray(2), dataArray(3), dataArray(4).toDouble)
    })

    //开窗
    val alertStream = messageStream
      .keyBy(data => data.name)
      .timeWindow(Time.seconds(windowTime))  //定义一个30秒的翻滚窗口
      .trigger(new MyTrigger(thresholdValue))  //检测阈值为5
      .reduce((_, y) => Message(y.time, y.machineNumber, y.machineType, y.name, y.value)) //直接聚合


    //变成JsonString流
    val jsonStrStream = alertStream
      .map(data => ResultJson(data.machineNumber, data.machineType, data.name,"警告！"+data.name+"值跳变过大，超过阈值！", data.time)) //修改格式
      .map(data => Serialization.write(data)) //包装json

    jsonStrStream.addSink(sink)
    jsonStrStream.print()

  }
}

