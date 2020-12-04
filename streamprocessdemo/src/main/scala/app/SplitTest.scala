package app

import bean.SensorReading
import org.apache.flink.streaming.api.scala._
import source.SensorSource

object SplitTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val input: DataStream[SensorReading] = env.addSource(new SensorSource)
    val splitStream: SplitStream[SensorReading] = input.split(data => if (data.temperature < 65) Seq("low") else Seq("high"))

    //val low: DataStream[SensorReading] = splitStream.select("low")
    val high: DataStream[SensorReading] = splitStream.select("high")
    //val all: DataStream[SensorReading] = splitStream.select("low", "high")

    //low.print("低温流：")
    high.print("高温流：")
    //all.print("全部流：")

    env.execute("split job")
  }
}
