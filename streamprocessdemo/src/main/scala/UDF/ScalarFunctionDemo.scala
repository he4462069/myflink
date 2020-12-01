package UDF

import bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

class HashCode(factor:Int) extends ScalarFunction{
 def eval(str: String):Int={
   str.hashCode*factor
 }
}

object functionTest{
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useOldPlanner()
      .build()

    val streamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    val inputStream: DataStream[SensorReading] = env.readTextFile("D:\\IdeaProjects\\myflink\\streamprocessdemo\\src\\main\\resources\\sqlDemoCsv.txt")
      .filter(_.nonEmpty).map { data =>
      val strings: Array[String] = data.split(",")
      SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
    }

    val streamWithTs: DataStream[SensorReading] = inputStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(5)) {
      override def extractTimestamp(t: SensorReading): Long = {
        t.timestamp
      }
    })

   /* val table: Table = streamTableEnv.fromDataStream(streamWithTs, 'id, 'timestamp.rowtime as 'ts, 'temperature as 'temp)
    val hash = new HashCode(10)

    val resultTable: Table = table.select(hash('id))
    resultTable.toAppendStream[Row].print()*/

    //使用sql时先注册sql的函数
    streamTableEnv.createTemporaryView("inputTable", streamWithTs, 'id, 'timestamp.rowtime as 'ts, 'temperature as 'temp)

    val hash = new HashCode(10)
    streamTableEnv.registerFunction("hash",hash)

    val resultTable: Table = streamTableEnv.sqlQuery(
      """
        |select hash(id) from inputTable
        |""".stripMargin)

    resultTable.toAppendStream[Row].print()

    env.execute("functionTest")
  }
}
