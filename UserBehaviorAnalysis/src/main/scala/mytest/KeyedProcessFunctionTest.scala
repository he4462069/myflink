package mytest

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 需求：监控温度传感器的温度值，如果温度值在一秒钟之内连续上升，则报警。
 */
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object KeyedProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = tool.get("host")
    val port: Int = tool.getInt("port")

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)
    val streamWithTs: DataStream[SensorReading] = inputStream.map { text =>
      val strings: Array[String] = text.split(",")
      SensorReading(strings(0), strings(1).toLong, strings(2).toDouble)
    }.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp*1000
    })


    val value: DataStream[String] = streamWithTs.keyBy(_.id).process(new MyKeyedProcessFunction)
    value.print()


    env.execute("my job")
  }
}

class MyKeyedProcessFunction extends KeyedProcessFunction[String,SensorReading,String] {

  //定义一个状态，保存上一个传感器温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState[Double](new ValueStateDescriptor[Double]("lastTemp", classOf[Double],Double.MinValue))

  //保存注册的定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("timer", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    //获取上次温度值
    val lastTempValue: Double = lastTemp.value()
    //将本次温度存入状态
    lastTemp.update(value.temperature)

      //上次温度为第一个温度或者当前温度小于上一次温度则删除定时器
    if(lastTempValue == Double.MinValue || value.temperature < lastTempValue) {
      ctx.timerService().deleteEventTimeTimer(currentTimer.value())

      currentTimer.clear()
    }else if(currentTimer.value() == 0 && lastTempValue < value.temperature){  //温度上升且我们并没有设置定时器
      val timerTs = value.timestamp + 4
      ctx.timerService().registerEventTimeTimer(timerTs)
      currentTimer.update(timerTs)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("传感器id为: " + ctx.getCurrentKey + "的传感器温度值已经连续1s上升了。")
    currentTimer.clear()
  }
}
