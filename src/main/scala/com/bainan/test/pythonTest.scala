package com.bainan.test
import scala.sys.process._

object pythonTest {
  def main(args: Array[String]): Unit = {
    val proc1 = "python --version".!!
    println(proc1)
  }
}
