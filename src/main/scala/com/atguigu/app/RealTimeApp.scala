package com.atguigu.app

import java.util.Properties

import com.atguigu.handler.{BlackListHandler, DateAreaCityAdCountHandler, LastHourAdCountHandler}
import com.atguigu.util.{MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Properties

/**
 * @author zhouyanjun
 * @create 2020-11-02 18:31
 */
object RealTimeApp {
  def main(args: Array[String]): Unit = {
    //2创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("RealTimeApp ").setMaster("local[*]")

    //1 创建StreamingContext对象
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //3 读取数据
    val properties: Properties = PropertiesUtil.load("config.properties")
    val topic: String = properties.getProperty("kafka.topic")

    //4 从kafka读取到的数据，抽象数据集
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc)

    val adsLogDStream: DStream[Ads_log] = kafkaDStream.map(kafkaData => {
      val value: String = kafkaData.value()
      val arr: Array[String] = value.split(" ")
      Ads_log(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
    })

    //5 TODO 需求1.0：根据MySQL中的黑名单过滤---> 然后得到过滤后数据集
    val filterAdsLogDStream: DStream[Ads_log] = BlackListHandler.filterByBlackList(adsLogDStream)

    //6.TODO 需求1.1：在需求1.0的基础上，把满足要求的用户写入黑名单
    BlackListHandler.addBlackList(filterAdsLogDStream)
    //测试打印
    filterAdsLogDStream.cache()
    filterAdsLogDStream.count().print()

    //7 TODO 需求2.0：统计每天各大区各个城市广告点击总数并保存至MySQL中
    DateAreaCityAdCountHandler.saveDateAreaCityAdCountToMysql(filterAdsLogDStream)

    //8 TODO 需求3.0：统计最近一小时(2分钟)广告分时点击总数
    val adToHmCountListDStream: DStream[(String, List[(String, Long)])] = LastHourAdCountHandler.getAdHourMintToCount(filterAdsLogDStream)

    adToHmCountListDStream.print()

    //启动任务
    ssc.start()
    ssc.awaitTermination()
  }
}

//使用样例类封装输入数据。
case class Ads_log(timestamp: Long, area: String, city: String, userid: String, adid: String)