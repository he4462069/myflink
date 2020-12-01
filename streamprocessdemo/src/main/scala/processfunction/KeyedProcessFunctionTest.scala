package processfunction

import bean.SensorReading
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.util.Collector

/**
 * 监控传感器的温度值，如果温度值在 1s 之内连续上升，则报警。
 */
class KeyedProcessFunctionTest extends KeyedProcessFunction[String,SensorReading,String]{

  //用来保存上一个传感器的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new  ValueStateDescriptor[Double]("lastTemp",classOf[Double]))

  //用来保存当前定时器的时间戳
  lazy val currentTimer:ValueState[Long] = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("timer", classOf[Long]))
  //getRuntimeContext.getMapState[String,String](new MapStateDescriptor[String,String]("adad",classOf[String],classOf[String]))

  override def processElement(element: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {

    //取出传感器上次的温度
    val preTemp: Double = lastTemp.value()

    //取出本次传感器的温度
    val curTemp: Double = element.temperature

    //将当前温度保存到状态中
    lastTemp.update(curTemp)

    //获取当前定时器的时间戳
    val curTimerTimestamp : Long = currentTimer.value()

    if (preTemp == 0||curTemp<preTemp) {
      //温度下降或者第一个温度值，则删除定时器
      ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)

      //清空状态变量
      currentTimer.clear()
    }else if(curTemp>preTemp&&curTimerTimestamp==0) {
      //温度上且没有设置定时器
      val timerTimestamp = ctx.timerService().currentProcessingTime()+1000
      ctx.timerService().registerProcessingTimeTimer(timerTimestamp)

      //将定时器的时间戳保存到currentTimer中
      currentTimer.update(timerTimestamp)

    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("传感器id为: " + ctx.getCurrentKey + "的传感器温度值已经连续1s上升了。")

    //定时器出触发之后，清空状态存储的定时器时间戳
    currentTimer.clear()
  }
}
