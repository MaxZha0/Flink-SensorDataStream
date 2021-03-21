package com.bainan.test

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.bainan.test.PicCv.{ImageJson, PythonJson}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization._
import scalaj.http.Http

import scala.io.Source

object jsonTest {
  implicit val formats = Serialization.formats(NoTypeHints)

  def readUrl(): Unit = {
    val imageData = Http("http://n000/cloudera/pic_cv/save1.jpg").asBytes.body
    Files.createDirectories(Paths.get("/Users/linan.bai/work/java/work/flink-template/src/main/resources/test"))
    println("http://n000/cloudera/pic_cv/save1.jpg".substring("http://n000/cloudera/pic_cv/save1.jpg".lastIndexOf("/")))
//    Files.write(Paths.get("/Users/linan.bai/work/java/work/flink-template/src/main/resources/a.jpg"), imageData)
  }

  def str2json(): Unit = {
    val filePath = "/Users/linan.bai/work/python/study/test/src/xidian/result.json"
    val fileContent = Source.fromFile(filePath).mkString
    val jsonObj = parse(fileContent).extract[Array[PythonJson]]
    jsonObj(0).output_image = "xxxx"
    println(write(jsonObj))
  }

  def main(args: Array[String]): Unit = {
//    readUrl()
    str2json()
//    val jsonStr = "{\"image_path\": \"http://n000/cloudera/pic_cv/save105.jpg\"}"
//    val a = parse(jsonStr).extract[(String, String)]
//    println(a)
//    val imageJson = ImageJson("a", "b", "c", "d", "e")
//    val jsonStr2 = write(imageJson)
//    println(jsonStr2)
  }

}
