package function

import bean.SensorReading
import org.apache.flink.api.common.functions.{AbstractRichFunction, CoGroupFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, KeyedCoProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import source.MySource

/**
 * 用connect对两个流进行合并
 */
case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime: Long )

case class ReceiptEvent( txId: String, payChannel: String, eventTime: Long )


object TxMatch {

  val unmatchedOrder = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipt = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderEventStream = env.readTextFile("D:\\IdeaProjects\\myflink\\streamprocessdemo\\src\\main\\resources\\OrderLog.csv")
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    val receiptEventStream = env.readTextFile("D:\\IdeaProjects\\myflink\\streamprocessdemo\\src\\main\\resources\\ReceiptLog.csv")
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    //主流
    val processedStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream
      .connect(receiptEventStream)
      .process(new TxMatchDetection)

    //order没有join上
    val unmatchedOrders: DataStream[OrderEvent] = processedStream.getSideOutput(unmatchedOrder)
    //receipt没有join上
    val unmatchedReceipts: DataStream[ReceiptEvent] = processedStream.getSideOutput(unmatchedReceipt)

    processedStream.print("join成功：")
    unmatchedOrders.print("order没有join上")
    unmatchedReceipts.print("receipt没有join上")


    env.execute("ConnectTest")

  }



}


class TxMatchDetection extends KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent,(OrderEvent, ReceiptEvent)]{

  lazy  private val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]))
  lazy  private val ordertState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("order-state", classOf[OrderEvent]))

  override def processElement1(orderEvent: OrderEvent, ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val receiptEvent: ReceiptEvent = receiptState.value()
    if (receiptEvent != null) {
      receiptState.clear()
      out.collect((orderEvent,receiptEvent))
    }else{
      ordertState.update(orderEvent)
      ctx.timerService().registerEventTimeTimer(orderEvent.eventTime + 1000L)
    }


  }

  override def processElement2(receiptEvent: ReceiptEvent, ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

    val orderEvent: OrderEvent = ordertState.value()
    if (orderEvent != null) {
      out.collect((orderEvent,receiptEvent))
      ordertState.clear()
    }else{
      receiptState.update(receiptEvent)
      ctx.timerService().registerEventTimeTimer(receiptEvent.eventTime +1000L)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val unmatchedOrder = new OutputTag[OrderEvent]("unmatchedPays")
    val unmatchedReceipt = new OutputTag[ReceiptEvent]("unmatchedReceipts")

    if (ordertState.value() != null) {
      //ctx.output(unmatchedOrder,ordertState.value())
      out.collect((ordertState.value(),null))
    }

    if (receiptState.value() != null) {
      //ctx.output(unmatchedReceipt,receiptState.value())
      out.collect((null,receiptState.value()))
    }
    ordertState.clear()
    receiptState.clear()

  }
}
