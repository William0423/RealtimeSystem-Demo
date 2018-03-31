package com.rts.dao.hive

import org.apache.spark.sql.SparkSession

/**
  * 通过spark sql 访问hive；
  * 后台只需要启动 “bin/hive --service metastore &”服务即可
  */
object HiveConnectionDAO {

  def main(args: Array[String]): Unit = {
      val spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("HiveConnectionDAO")
//        .config("hive.metastore.uris", "thrift://{server ip}:9083") //需与hive-site.xml配置相同
        .config("hive.metastore.uris", "thrift://localhost:9083") //需与hive-site.xml配置相同
        .enableHiveSupport()
        .getOrCreate()

      import spark.sql
      sql("show tables").show()
      sql("select * from hive_bdtable").show()
      spark.stop()
    }

}
