package app

import bean.SensorReading
import org.apache.flink.streaming.api.scala._
import source.Mysource

object MySourceTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[SensorReading] = env.addSource(new Mysource())

    //stream.print().setParallelism(1)

    val splitedStream: SplitStream[SensorReading] = stream.split(data => if (data.temperature > 70) Seq("high") else Seq("low"))

    //选择名为high的流
    val highStream: DataStream[SensorReading] = splitedStream.select("high")
    val lowStream: DataStream[SensorReading] = splitedStream.select("low")



    highStream.print.setParallelism(1)

    env.execute("MySourceTest")
  }

}
