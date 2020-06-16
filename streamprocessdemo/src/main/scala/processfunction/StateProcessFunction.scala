package processfunction

import bean.SensorReading
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import source.MySource

object StateProcessFunction {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.enableCheckpointing(1000)
    //默认就是EXACTLY_ONCE
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //保存checkpoint的超时时间
    env.getCheckpointConfig.setCheckpointInterval(3000L)
    //同时保存checkpoint的最大个数（当前一个checkpoint还没有保存完，又到下一个checkpoint保存的时间）
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    //
    //env.getCheckpointConfig.setMinPauseBetweenCheckpoints()
    //重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,Time.seconds(100)))


    val stream: DataStream[SensorReading] = env.addSource(new MySource).keyBy("id")

    val mapedStream: DataStream[String] = stream.map(new MyStateMap)

    mapedStream.print()

    env.execute("StateProcessFunction")
  }

}

class MyStateMap() extends RichMapFunction[SensorReading,String]{
  lazy val lastTempState:ValueState[Double] = getRuntimeContext.getState[Double]( new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))

  override def map(value: SensorReading): String = {
    val lastTemp = lastTempState.value()
    lastTempState.update(value.temperature)
    if ((value.temperature - lastTemp).abs > 5) {
      s"lastTemp = $lastTemp,currentTemp = ${value.temperature},温度跨度过大！"
    } else {
      s"lastTemp = $lastTemp,currentTemp = ${value.temperature},温度跨度在合理范围内。"
    }

  }
}
