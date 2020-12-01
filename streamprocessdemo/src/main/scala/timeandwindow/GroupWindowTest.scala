package timeandwindow

import java.sql.Timestamp

import bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}

/**
 * group窗口
 */
object GroupWindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useOldPlanner()
      .build()

    val streamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    val streamWithTs: DataStream[SensorReading] = env.readTextFile("D:\\IdeaProjects\\myflink\\streamprocessdemo\\src\\main\\resources\\sqlDemoCsv.txt")
      .filter(_.nonEmpty).map { data =>
        val strings: Array[String] = data.split(",")
        SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
      }.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(5)) {
        override def extractTimestamp(t: SensorReading): Long = {
          t.timestamp
        }
      })

    streamTableEnv.createTemporaryView("inputTable",streamWithTs,'id, 'timestamp.rowtime as 'ts, 'temperature as 'temp)

    val table: Table = streamTableEnv.sqlQuery(
      """
        |select
        | id,
        | count(1),
        | tumble_end(ts, interval '10' second)
        |from inputTable
        |group by
        | id,
        | tumble(ts, interval '10' second)
        |""".stripMargin)
    val value: DataStream[(String, Long, Timestamp)] = table.toAppendStream[(String, Long, Timestamp)]

   /* val streamTable: Table = streamTableEnv.fromDataStream[SensorReading](streamWithTs, 'id, 'timestamp.rowtime as 'ts, 'temperature as 'temp)


    val resultTable: Table = streamTable.window(Tumble over 50.millis on 'ts as 'tw)
      .groupBy('id,'tw)
      .select('id, 'temp.sum)

    val value: DataStream[(String, Double)] = resultTable.toAppendStream[(String, Double)]
*/
    value.print("result")

     env.execute("WindowTest")

  }
}
