package flinksql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.catalog.hive.HiveCatalog

object sqlConnectHive {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useOldPlanner().build()

    val streamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    val name            = "myhive"
    val defaultDatabase = "mydatabase"
    val hiveConfDir     = "/opt/hive-conf" // a local path
    val version         = "1.2.1"

    val hive: HiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version)
    streamTableEnv.registerCatalog("myhive",hive)

    // set the HiveCatalog as the current catalog of the session
    streamTableEnv.useCatalog("myhive")

    

    env.execute()
  }
}
