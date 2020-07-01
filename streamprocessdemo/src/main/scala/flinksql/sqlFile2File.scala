package flinksql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object sqlFile2File {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useOldPlanner()
      .build()

    val streamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    //创建一张表，用于读取数据
    streamTableEnv.connect(new FileSystem().path("D:\\IdeaProjects\\myflink\\streamprocessdemo\\src\\main\\resources\\sqlDemoCsv.txt"))
      .withSchema(new Schema()
          .field("id",DataTypes.STRING())
          .field("stamp",DataTypes.BIGINT())
          .field("temperature",DataTypes.DOUBLE())
      )
      .withFormat(new Csv())
      .createTemporaryTable("inputTable")

    //注册一张表，用于存储数据
    streamTableEnv.connect(new FileSystem().path("D:\\IdeaProjects\\myflink\\streamprocessdemo\\src\\main\\resources\\out\\out.txt"))
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("timestamp",DataTypes.BIGINT())
        .field("temperature",DataTypes.DOUBLE())
      )
      .withFormat(new Csv())
      .createTemporaryTable("outTable")

    streamTableEnv.sqlQuery("select id,stamp from inputTable").insertInto("outTable")


    env.execute()
  }
}
