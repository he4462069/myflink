package app.TxMatchDetect

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TxMatch {
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val ttlConfig=StateTtlConfig.newBuilder(Time.seconds(1))
//      .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
//      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//      .build()
//    val descripe = new ValueStateDescriptor("avgState", TypeInformation.of(new TypeHint[(Long, Long)] {}))
//    descripe.enableTimeToLive(ttlConfig)

//    RocksDB状态后端设置
//    val stateBackend:StateBackend=new RocksDBStateBackend("hdfs://namenode:9000/flink/checkpoint",true)
//    env.setStateBackend(stateBackend)
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)))


    val orderEventStream = env.readTextFile("YOUR_PATH\\resources\\OrderLog.csv")
      .map( data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    val receiptEventStream = env.readTextFile("YOUR_PATH\\resources\\ReceiptLog.csv")
      .map( data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    val processedStream = orderEventStream
      .connect(receiptEventStream)
      .process(new TxMatchDetection)

    processedStream.getSideOutput(unmatchedPays).print("unmatched pays")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatched receipts")

    processedStream.print("processed")

    env.execute()
  }

  class TxMatchDetection extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
    lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state",classOf[OrderEvent]) )
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]) )

    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val receipt = receiptState.value()

      if( receipt != null ){
        receiptState.clear()
        out.collect((pay, receipt))
      } else{
        payState.update(pay)
        ctx.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 5000L)
      }
    }

    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val payment = payState.value()

      if( payment != null ){
        payState.clear()
        out.collect((payment, receipt))
      } else{
        receiptState.update(receipt)
        ctx.timerService().registerEventTimeTimer(receipt.eventTime * 1000L + 5000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      if ( payState.value() != null ){
        ctx.output(unmatchedPays, payState.value())
      }
      if ( receiptState.value() != null ){
        ctx.output(unmatchedReceipts, receiptState.value())
      }
      payState.clear()
      receiptState.clear()
    }
  }



}

case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime: Long )

case class ReceiptEvent( txId: String, payChannel: String, eventTime: Long )

