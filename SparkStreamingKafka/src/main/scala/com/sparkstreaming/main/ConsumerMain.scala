package com.sparkstreaming.main

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author litaoxiao
  *
  */
object ConsumerMain extends Serializable {

	def functionToCreateContext(): StreamingContext = {
		val sparkConf = new SparkConf().setAppName("WordFreqConsumer").setMaster("local[5]")
				.set("spark.local.dir", "/tmp")
				.set("spark.streaming.kafka.maxRatePerPartition", "10")
		val ssc = new StreamingContext(sparkConf, Seconds(10))

		// Create direct kafka stream with brokers and topics
		val topicsSet = "ssjt_test".split(",").toSet
		val kafkaParams = scala.collection.immutable.Map[String, String]("metadata.broker.list" -> "hadoop03:9092,hadoop04:9092,hadoop05:9092", "auto.offset.reset" -> "smallest", "group.id" -> "kafka_test")
		val km = new KafkaManager(kafkaParams)
		val kafkaDirectStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

		kafkaDirectStream.cache

		//do something......
		var offsetRanges = Array[OffsetRange]()
		kafkaDirectStream.transform(rdd => {
			offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
			println("- - - - - - - - - - - - - - - - - - ")
			rdd
		}).map(msg => msg._2)
				.foreachRDD(rdd => {
					rdd.foreachPartition(partition => {
						partition.foreach(record => {
							//处理数据的方法
							println(record)
						})
					})
				})
		//更新zk中的offset
		kafkaDirectStream.foreachRDD(rdd => {
			if (!rdd.isEmpty) {
				km.updateZKOffsets(rdd)
			}
		})
		ssc
	}


	def main(args: Array[String]) {

		val ssc = functionToCreateContext()

		// Start the computation
		ssc.start()
		ssc.awaitTermination()
		ssc.stop()
	}


}