package com.atguigu.handler

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.app.Ads_log
import com.atguigu.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

/**
 * @author zhouyanjun
 * @create 2020-11-02 19:07
 */
object BlackListHandler {
  private val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  // TODO 首先判断点击用户是否在黑名单内，在里面，就不能要。不在里面，要保留
  def filterByBlackList(adsLogDStream: DStream[Ads_log]): DStream[Ads_log] = {
    adsLogDStream.transform(
      rdd => {
        rdd.filter(
          adsLog => {
            val connection: Connection = JDBCUtil.getConnection

            val bool: Boolean = JDBCUtil.isExist(
              connection,
              """
                |select * from black_list where userid=?
                |""".stripMargin,
              Array(adsLog.userid)
            )
            // 关闭连接
            connection.close()
            !bool
          }
        )
      }
    )
  }

  //TODO 通过计算，往mysql的黑名单列表里添加符合要求的用户
  def addBlackList(filterAdsLogDStream: DStream[Ads_log]): Unit = {
    //得到数据集
    val dateUserAdToCount: DStream[((String, String, String), Long)] = filterAdsLogDStream.map(
      adsLog => {
        val date: String = sdf.format(new Date(adsLog.timestamp))

        ((date, adsLog.userid, adsLog.adid), 1L) //1L，long类型，是为了避免数据太大可能报错的问题
      }
    ).reduceByKey(_ + _)

    //TODO 如果没有超过阈值，那么需要将当天的广告点击数量进行更新
    dateUserAdToCount.foreachRDD( //操作每批次的数据
      rdd => {
        rdd.foreachPartition(
          iter => {
            val connection: Connection = JDBCUtil.getConnection
            iter.foreach {
              case ((dt, user, ad), count) => {

                // 向MySQL中user_ad_count表，更新or新增数据
                JDBCUtil.executeUpdate(
                  connection,
                  """
                    |INSERT INTO user_ad_count(dt,userid,adid,count)
                    |VALUES(?,?,?,?)
                    |ON DUPLICATE KEY
                    |UPDATE count=count+?
                    |""".stripMargin,
                  Array(dt, user, ad, count, count)
                )

                //TODO 如果发现统计数量超过点击阈值，那么将用户拉入到黑名单
                val ct: Long = JDBCUtil.getDataFromMysql(
                  connection,
                  """
                    |select count from user_ad_count
                    |where dt=? and userid=? and adid=?
                    |""".stripMargin,
                  Array(dt, user, ad)
                )

                // 点击次数>30次，加入黑名单
                if (ct >= 30) {
                  JDBCUtil.executeUpdate(
                    connection,
                    """
                      |INSERT INTO black_list (userid)
                      |VALUES (?)
                      |ON DUPLICATE KEY
                      |update userid=?
                      |""".stripMargin,
                    Array(user,user)
                  )
                }
              }
            }
            connection.close()


          }
        )
      }
    )

  }
}

