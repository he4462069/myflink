package timeandwindow

import java.sql.Timestamp

import bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Over, Table}
import org.apache.flink.types.Row

/**
 * Over窗口
 * 需求：对每个传感器统计当前行与前两行的温度平均值
 */
object OverWindowTest {
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

/*    //table api 实现
    val table: Table = streamTableEnv.fromDataStream(streamWithTs, 'id, 'timestamp.rowtime as 'ts, 'temperature as 'temp)

    val resultTable: Table = table.window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'w)
      .select('id, 'ts, 'id.count over 'w, 'temp.avg over 'w)

    resultTable.toAppendStream[Row].print("table API print")*/

    //sql 实现
    streamTableEnv.createTemporaryView("inputTable", streamWithTs, 'id, 'timestamp.rowtime as 'ts, 'temperature as 'temp)

    val resultTable: Table = streamTableEnv.sqlQuery(
      """
        |select
        | id,
        | ts,
        | count(id) over w,
        | avg(temp) over w
        |from
        | inputTable
        |window w as(
        | partition by id
        | order by ts
        | rows between 2 preceding and current row
        |)
        |""".stripMargin)

    resultTable.toAppendStream[Row].print("SQL print")


     env.execute("OverWindowTest")
  }
}
