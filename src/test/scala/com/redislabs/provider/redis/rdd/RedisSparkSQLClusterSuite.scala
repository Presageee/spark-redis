package com.redislabs.provider.redis.rdd

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, FunSuite}
import org.apache.spark.sql.SQLContext
import com.redislabs.provider.redis._

class RedisSparkSQLClusterSuite extends FunSuite with ENV with BeforeAndAfterAll with ShouldMatchers {

  var sqlContext: SQLContext = null
  override def beforeAll() {
    super.beforeAll()

    sc = new SparkContext(new SparkConf()
      .setMaster("local[*]").setAppName(getClass.getName)
      .set("redis.host", "172.20.2.60")
      .set("redis.port", "7000")
    )
    redisConfig = new RedisConfig(new RedisEndpoint("172.20.2.60", 7000))

    // Flush all the hosts
    redisConfig.hosts.foreach( node => {
//      val conn = node.connect
//      conn.flushAll
//      conn.close
    })

    sqlContext = new SQLContext(sc)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY VIEW rl
                       |(name STRING, score STRING)
                       |USING com.redislabs.provider.redis.sql
                       |OPTIONS (table 'rl')
      """.stripMargin)
//    val dd = sqlContext.read
//      .options(Map("table"->"rl"))
//      .format("com.redislabs.provider.redis.sql")
//      .load()
//    dd.show()
//    dd.registerTempTable("rl")

//    (1 to 10000000).foreach{
//      index => {
//        sqlContext.sql(s"insert overwrite table rl select t.* from (select 'rl${index}'," +
//          s" '7qpXQpFlAACAAAAAH/Y5vVPnr73i43a9nDkRPgAiYD3Itjq9pZmhOzS+RL3cONk9tWbMPeMjLblv/Qo+ZEiIPSq0hjwM2vi99FRVPXfHsb21hoG9pT4ovUxVcjxb9Sw9k9mTvTRoFL68kTw+he0HvhGOYr1+SFW99jMXPRLgU73rDPm9QdysvH6qjT3JlxI+vIcavfGwPL13fJy9eOnSPRulob3WVaS9Br52vVm4Cr2KN+48mz3nPLg5kz1AdSu+SfUGvVEwtz311qg9otnuvHk3aj6tzVC9lo5+PkEZBj6oRa69bxcqvYaizL38JK+9mguFvTx7t70MA4I9haJQvag9NL56v6G9JlV2PWxPJL0uWfg95C2PPTLt1LxMJU+9k9TjPZkHkzuvZbw9K89LPWM4obuwxJC99GDTO/RAG75qvvo9TEOZPdpsDz7JPJ09PCcJOxyxgT2231i8KJxUPPRs3D3Z9Qu9ZdAPvklhy7xll5k6uzz4OzvL9DwWOaE9QLDLPQksXb21ey4+z4WnPFkawTwYZ429z7EOvgCerz3rY4I9vshavbAo0zuRWgq95ijavXCjIj5v0tY85S4bPZ/rqDwgp949qgBWvl1T771XQi+9LmSzPUxcajzuuTE9Y2mzvbXO872f7ty8i/S1OwXiEr3Im/i8SwFPPuOZh714Mra8jaugu4pcnz0=') t")
//      }
//    }
      //val dd = sqlContext.sql(s"select * from rl*")
  }

  test("RedisKVRDD - default(cluster)") {
    val df = sqlContext.sql(
      s"""
         |SELECT *
         |FROM rl
       """.stripMargin)
    //df.filter(df("score") > 10).count should be (54)
    //df.filter(df("score") > 10 and df("score") < 20).count should be (9)
    df.show(10)
    }

//  test("RedisKVRDD - cluster") {
//    implicit val c: RedisConfig = redisConfig
//    val df = sqlContext.sql(
//      s"""
//         |SELECT *
//         |FROM rl
//       """.stripMargin)
//    df.filter(df("score") > 10).count should be (54)
//    df.filter(df("score") > 10 and df("score") < 20).count should be (9)
//  }

  override def afterAll(): Unit = {
    sc.stop
    System.clearProperty("spark.driver.port")
  }

}

