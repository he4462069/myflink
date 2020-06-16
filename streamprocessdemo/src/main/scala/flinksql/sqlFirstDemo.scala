package flinksql

import java.util

import bean.SensorReading
import org.apache.flink.api.common.typeinfo.TypeInfo
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{ConnectorDescriptor, Csv, FileSystem, OldCsv, Schema}
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row

object sqlFirstDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val settings:EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val streamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    //根据文件系统的csv文件创建表
    streamTableEnv.connect( new FileSystem().path("D:\\IdeaProjects\\myflink\\streamprocessdemo\\src\\main\\resources\\sqlDemoCsv.txt"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("timestamp",DataTypes.BIGINT())
        .field("temperature",DataTypes.DOUBLE()))
      .createTemporaryTable("inputTable")

    val queryTable: Table = streamTableEnv.sqlQuery("select *  from inputTable")
    val datastream: DataStream[(String, Long, Double)] = queryTable.toAppendStream[(String, Long, Double)]

    val table: Table = streamTableEnv.fromDataStream(datastream, 'id, 'temperature,'processtime.proctime)



    //两种方式把Table转化为DataStream
    //val value1: DataStream[SensorReading] = queryTable.toAppendStream[SensorReading]
    //val value2: DataStream[SensorReading] = streamTableEnv.toAppendStream[SensorReading](queryTable)
    //
    //val value3: DataStream[(Boolean, (String, Long))] = streamTableEnv.toRetractStream[(String, Long)](queryTable)

    //value3.filter(_._1 == true).print().setParallelism(1)
    val value: DataStream[(String, Long)] = table.toAppendStream[(String, Long)]
    value.print().setParallelism(1)

    env.execute("sqlFirstDemo")


  }

}
