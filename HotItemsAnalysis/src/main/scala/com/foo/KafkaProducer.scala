package com.foo

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * 将数据源更改为kafka
  */
object KafkaProducer {
  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }
  def writeToKafka(topic: String): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", "hadoop101:9092")
    props.setProperty("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("auto.offset.reset", "latest")
    val producer = new KafkaProducer[String,String](props)
    val bufferedSource = io.Source.fromFile("F:\\ideaWorkspace\\Big practical training\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for (line <- bufferedSource.getLines()) {
      val record = new ProducerRecord[String,String](topic,line)
      producer.send(record)

    }
    producer.close()
  }


}
