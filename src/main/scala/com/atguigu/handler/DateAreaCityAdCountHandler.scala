package com.atguigu.handler

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.app.Ads_log
import com.atguigu.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

/**
 * 实时统计每天各地区各城市各广告的点击总流量，并将其存入	MySQL
 *
 * @author zhouyanjun
 * @create 2020-11-02 20:53
 */
object DateAreaCityAdCountHandler {
  // 时间格式化对象
  private val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def saveDateAreaCityAdCountToMysql(filterAdsLogDStream: DStream[Ads_log]): Unit = {
    val dateAreaCityAdToCount: DStream[((String, String, String, String), Long)] = filterAdsLogDStream.map(
      ads_log => {
        val timestamp: Long = ads_log.timestamp
        val dt: String = sdf.format(new Date(timestamp))

        ((dt, ads_log.area, ads_log.city, ads_log.adid), 1L)
      }
    ).reduceByKey(_ + _)

    //对每批次进行操作
    dateAreaCityAdToCount.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            //获取连接
            val connection: Connection = JDBCUtil.getConnection
            //写入数据库
            iter.foreach{
              case((dt,area,city,adid),ct)=>{
                JDBCUtil.executeUpdate(
                  connection,
                  """
                    |INSERT INTO area_city_ad_count (dt,area,city,adid,count)
                    |value(?,?,?,?,?)
                    |ON DUPLICATE KEY
                    |UPDATE count=count+?;
                    |""".stripMargin,
                  Array(dt,area,city,adid,ct,ct)
                )
              }
            }
            //c.释放连接
            connection.close()

          }
        )
      }
    )

  }
}
