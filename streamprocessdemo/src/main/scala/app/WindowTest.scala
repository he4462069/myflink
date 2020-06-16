package app

import bean.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import source.MySource

/**
 * 根据processTime来划分窗口
 */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[SensorReading] = env.addSource(new MySource)

    val result: DataStream[(String, Double)] = stream.map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .reduce((x, y) => (x._1, x._2.min(y._2)))

    result.print().setParallelism(1)

    env.execute("WindowTest")

  }

}
