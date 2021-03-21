package com.bainan.test

import io.minio.{MinioClient, UploadObjectArgs}

object MinIOTest {
  def main(args: Array[String]): Unit = {
    val minioClient = MinioClient.builder()
      .endpoint("http://localhost:9000")
      .credentials("root", "root_password")
      .build()
    val result = minioClient.uploadObject(UploadObjectArgs.builder.bucket("processed-images")
      .`object`("test2.jpeg")
        .contentType("image/jpeg")
      .filename("/Users/linan.bai/work/python/study/test/src/xidian/20201228_213812.jpg").build)
    println("http://localhost:9000/processed-images/test2.jpeg")
  }
}
