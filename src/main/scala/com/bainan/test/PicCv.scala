package com.bainan.test

import java.io.PrintWriter
import java.nio.file.{Files, Paths}
import java.util.{Properties, UUID}

import io.minio.MinioClient
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.json4s.NoTypeHints
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import scalaj.http.Http

import scala.io.Source
import scala.sys.process._

object PicCv {

  implicit val formats = Serialization.formats(NoTypeHints)

  case class ImageJson(img_path: String, templateImage_path: String, output_path: String, trainData_path: String, result_file_path: String)

  case class ResultJson(result: String, resultImageUrl: String)

  case class PythonJson(result: String, angle: String, var output_image: String, location: String)

  val mainPython = "Detection1228.py"
  val angleFile = "angle.txt"
  val inputJsonFile = "jsonfile.json"
  val template_pic = "template_pic.jpg"
  val source_path = "source_pic.jpg"
  val target_path = "target"
  val result_path = "result.json"

  def main(args: Array[String]): Unit = {
    val processName = "pic_cv_process"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.registerCachedFile("hdfs:///user/root/pic_cv/code/Detection1228.py", mainPython)
    env.registerCachedFile("hdfs:///user/root/pic_cv/code/angle.txt", angleFile)
    env.registerCachedFile("hdfs:///user/root/pic_cv/code/jsonfile.json", inputJsonFile)
    env.registerCachedFile("hdfs:///user/root/pic_cv/code/template_pic.jpg", template_pic)
    env.registerCachedFile("hdfs:///user/root/pic_cv/code/source_pic.jpg", source_path)

    val kafkaBrokers = "n000:9092,n001:9092,n002:9092,n003:9092"
    val consumerGroup = "pic_cv_process"
    val listenerTopic = "picCvProcessTopic"
    val targetTopic = "testTargetTopic"

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaBrokers)
    properties.setProperty("group.id", consumerGroup)

    // {"image_path": "http://n000/cloudera/pic_cv/save105.jpg"}
    val dataStream = env.addSource(new FlinkKafkaConsumer011[String](listenerTopic, new SimpleStringSchema(), properties))
    val resultStream = dataStream.map(parse(_).extract[(String, String)])
      .map(new CustomMapTest)

    resultStream.addSink(new FlinkKafkaProducer011[String](kafkaBrokers, targetTopic, new SimpleStringSchema()))
    env.execute(processName)
  }

  class CustomMapTest() extends RichMapFunction[(String, String), String] {
    var fileDir: String = ""
    var minioClient:MinioClient = _

    override def open(parameters: Configuration): Unit = {
      val pythonFile = getRuntimeContext.getDistributedCache.getFile(PicCv.mainPython)
      fileDir = pythonFile.getAbsoluteFile.getParent + "/"

      minioClient = MinioClient.builder()
        .endpoint("http://n002:9000")
        .credentials("root", "root_password")
        .build()
    }

    def uploadOutputImage(output_image: String, objectName:String) = {
      import io.minio.UploadObjectArgs
      minioClient.uploadObject(UploadObjectArgs.builder.bucket("processed-images")
          .contentType("image/jpeg")
        .`object`(objectName)
        .filename(output_image).build)
    }

    override def map(in: (String, String)): String = {
      val image_url = in._2
      var randomDir = UUID.randomUUID().toString
      Files.createDirectories(Paths.get(fileDir + randomDir))
      randomDir = randomDir + "/"
      var source:Source = null
      try {
        val sourceImageFile = fileDir + randomDir + PicCv.source_path
        writeImage(image_url, sourceImageFile)
        val resultPath = fileDir + randomDir + PicCv.result_path
        val jsonPath = fileDir + randomDir + PicCv.inputJsonFile
        val outputPath = fileDir + randomDir

        val imageJson = ImageJson(sourceImageFile, fileDir + PicCv.template_pic, outputPath, fileDir + PicCv.angleFile, resultPath)
        val jsonStr = Serialization.write(imageJson)
        writeJson(jsonStr, jsonPath)

        s"python3 ${fileDir + PicCv.mainPython} jsonPath=$jsonPath".!

        source = Source.fromFile(resultPath)
        val result = source.mkString
        val resultJsonArray = parse(result).extract[Array[PythonJson]]
        val resultJson = resultJsonArray(0)
        val imageName = UUID.randomUUID().toString + "_output.jpeg"
        uploadOutputImage(resultJson.output_image, imageName)
        resultJson.output_image = "http://n000/oss/processed-images/"+imageName
        Serialization.write(resultJsonArray)
      } catch {
        case ex: Exception => s"path :${fileDir + randomDir},image_url:${image_url}, ex: ${ex.getMessage}"
      } finally {
        if (source != null){
          source.close()
        }
      }


      //      val pythonResultJson = parse(result).extract[PythonJson]
      //      val outImagePath = pythonResultJson.output_image
      //      val imageName = outImagePath.substring(outImagePath.lastIndexOf("/") + 1)

      //      write(ResultJson(result))
      //      fileDir + randomDir
    }

    def writeImage(image_url: String, sourceImageFile: String): Unit = {
      val imageData = Http(image_url).asBytes.body
      Files.write(Paths.get(sourceImageFile), imageData)
    }

    def writeJson(jsonStr: String, path: String): Unit = {
      val print = new PrintWriter(path)
      print.print(jsonStr)
      print.close()
    }
  }

}



