package CEP

import java.util

import bean.SensorReading
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**``
 * 温度在一定时间内连续上升就报警
 */
object TempIncreaseAlert {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val input: KeyedStream[SensorReading, String] = env.readTextFile("D:\\IdeaProjects\\myflink\\streamprocessdemo\\src\\main\\resources\\qqqq.csv")
      .map { data =>
        val strings: Array[String] = data.split(",")
        SensorReading(strings(0), strings(1).toLong, strings(2).toDouble)
      }.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp
    }).keyBy(_.id)

    val value: DataStream[String] = input.process(new TempIncreaseAlertFunction(5))


    value.print()

    env.execute("")
  }
}

class TempIncreaseAlertFunction(delayTime: Int) extends KeyedProcessFunction[String,SensorReading,String]{
  //上次存储的温度值
  lazy val lastTmpState: ValueState[Double] = getRuntimeContext.getState[Double](new ValueStateDescriptor[Double]("last-tmp", classOf[Double]))
  //上次存储的定时器时间戳
  lazy val lastTimeState: ValueState[Long] = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("last-time", classOf[Long]))

  lazy val sensorState: ListState[SensorReading] = getRuntimeContext.getListState[SensorReading](new ListStateDescriptor[SensorReading]("sensor", classOf[SensorReading]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    //取出上次温度
    val lastTemprature: Double = lastTmpState.value()

    lastTmpState.update(value.temperature)
    //获取本次温度
    val thisTemprature: Double = value.temperature
    //获取上次定时器的时间戳
    val lastTime: Long = lastTimeState.value()

    if (lastTemprature < thisTemprature ) {

      if (lastTime==0){
        //创建的定时器时间
        val time = ctx.timerService().currentWatermark() + delayTime*1000
        ctx.timerService().registerEventTimeTimer(time)
        //记录定时器时间
        lastTimeState.update(time)
      }
      sensorState.add(value)
    }

    if (lastTemprature > thisTemprature || lastTemprature == 0 ){
      ctx.timerService().deleteEventTimeTimer(lastTime)
      lastTimeState.clear()
      sensorState.clear()
    }



  }

  //触发定时器意味着温度一直在升高，如果温度降低则会清空定时器。在定时器中进行报警操作
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    val iter: util.Iterator[SensorReading] = sensorState.get().iterator()
    val list = new util.ArrayList[SensorReading]()
    while (iter.hasNext) {
      val reading: SensorReading = iter.next()
      list.add(reading)

    }
    out.collect(list.toString)
    lastTimeState.clear()
    lastTmpState.clear()
    sensorState.clear()

  }
}
