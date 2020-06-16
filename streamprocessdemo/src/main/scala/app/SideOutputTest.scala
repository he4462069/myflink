package app

import bean.SensorReading
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.executiongraph.failover.RestartAllStrategy
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import source.MySource

object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.setStateBackend(new FsStateBackend("file:///D:\\IdeaProjects\\myflink\\streamprocessdemo\\src\\main\\resources"))
    env.enableCheckpointing(10000,CheckpointingMode.AT_LEAST_ONCE)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,Time.seconds(10)))

    val stream: DataStream[SensorReading] = env.addSource(new MySource())

    val monitoredReading: DataStream[String] = stream.process(new FreezingMonitor(70))

    val sideStream: DataStream[String] = monitoredReading.getSideOutput(new OutputTag[String]("freezing-alarm"))

    sideStream.print()
    stream.print()

    env.execute("SideOutputTest")
  }

}

/**
 * 自定义函数，得到测输出流
 * @param point
 */
class FreezingMonitor(point :Double) extends ProcessFunction[SensorReading,String]{
  //定义一个测输出流标签
  lazy val freezingAlarmOutput:OutputTag[String] = new OutputTag[String]("freezing-alarm")

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, String]#Context, out: Collector[String]): Unit = {
    if(value.temperature < point) {
      ctx.output(freezingAlarmOutput,s"freezing-alarm for ${value.id},temperature is ${value.temperature}")
    }


    out.collect(value.toString)
  }
}
