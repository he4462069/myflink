package app

import bean.SensorReading
import org.apache.flink.streaming.api.scala._
import source.MySource

/**
 * 用connect对两个流进行合并
 */
object ConnectTest {
  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[SensorReading] = env.addSource(new MySource())

    val splitedStream: SplitStream[SensorReading] = stream.split(data => if (data.temperature > 70) Seq("high") else Seq("low"))

    val highStream: DataStream[SensorReading] = splitedStream.select("high")
    val lowStream: DataStream[SensorReading] = splitedStream.select("low")
    val warnStream: DataStream[(String, Double)] = highStream.map(data => (data.id, data.temperature))

    val connectedStream: ConnectedStreams[(String, Double), SensorReading] = warnStream.connect(lowStream)

    val coMap: DataStream[Product] = connectedStream.map(
      warnStream => (warnStream._1, warnStream._2, "highWarning"),
      low => (low.id, "health")
    )

    coMap.print.setParallelism(1)

    env.execute("ConnectTest")

  }
}
