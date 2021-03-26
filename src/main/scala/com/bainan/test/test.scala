package com.bainan.test


import com.bainan.test.KfkVolt.{Message, ResultJson}
import com.bainan.test.PicCv.CustomMapTest

import java.util.{Properties, UUID}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.json4s.NoTypeHints
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization
import scalaj.http.Http






object test {
  implicit val formats = Serialization.formats(NoTypeHints)
  //消息样例类
  case class Message(time : String , name : String , value : Double)
  case class ResultJson(info :String, value: Double, warningTime : String)

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "file.txt"
    val dataStream = env.readTextFile(inputPath)

    //分割stream 转换成 Message 格式Stream
    val messageStream= dataStream.map(data =>{
      val dataArray = data.split("\\+")
      Message(dataArray(0), dataArray(1), dataArray(2).toDouble)
    })

    //分流，进行判断后分流


    //    val othersStream = splitStream.select("othersStream")
    //分离出来的流换成准备输出的格式
    val alertStream = messageStream.map(data =>{
      ResultJson("警报！电压值超出阈值",data.value, data.time)
    })

    //变成JsonString流
    val jsonStrStream = alertStream.map(data =>{
      Serialization.write(data)
    })

    jsonStrStream.print()


  }
}

/**
 * ./bin/flink run -m yarn-cluster  -yjm 1024 -ytm 1024 -c com.bainan.test.KfkVolt /home/bigdata/flinkApp/flinkAlert/flink-alert-1.0-SNAPSHOT-jar-with-dependencies.jar
将其中的jar包替换成自身jar包名称， -c 后跟着的是Main函数的全类名也需要替换
 */

