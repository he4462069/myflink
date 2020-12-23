package function

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 用ProcessFunction，自定义实现检测订单超时的功能。
 */
object OrderTimeoutWithoutCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orderEventStream = env.readTextFile("D:\\IdeaProjects\\myflink\\streamprocessdemo\\src\\main\\resources\\OrderLog.csv")
      .map( data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1),dataArray(2), dataArray(3).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.orderId)

    // 自定义一个 process function，进行order的超时检测，输出超时报警信息
    val timeoutWarningStream = orderEventStream
      .process(new OrderTimeoutAlert)

    timeoutWarningStream.print()

    env.execute()
  }

  class OrderTimeoutAlert extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
      val isPayed = isPayedState.value()

      if (value.eventType == "create" && !isPayed) {
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 15 * 60 * 1000L)
      } else if (value.eventType == "pay") {
        isPayedState.update(true)
      }
    }
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      val isPayed = isPayedState.value()
      if (!isPayed) {
        out.collect(OrderResult(ctx.getCurrentKey, "order timeout"))
      }
      isPayedState.clear()
    }
  }
}


case class OrderResult(orderId: Long, eventType: String)
