package function

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orderEventStream : DataStream[OrderEvent] = env.readTextFile("D:\\IdeaProjects\\myflink\\streamprocessdemo\\src\\main\\resources\\OrderLog.csv")
      .map { data =>
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      }.assignAscendingTimestamps(_.eventTime * 1000)

    //下单后15分钟内不支付就取消订单
    val pattern: Pattern[OrderEvent, OrderEvent] = Pattern
      .begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.seconds(15))

    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream.keyBy(_.orderId), pattern)

    // 定义一个输出标签
    val orderTimeoutOutput = OutputTag[(Long,String)]("orderTimeout")

    val completedResult : DataStream[(Long, String)] = patternStream.select(orderTimeoutOutput, new MyPatternTimeoutFunction, new MyPatternSelectFunction)

    //拿到同一输出标签中的 timeout 匹配结果（流）
    val timeoutResult : DataStream[(Long, String)] = completedResult.getSideOutput(orderTimeoutOutput)

    completedResult.print()
    timeoutResult.print()

    env.execute("Order Timeout Detect Job")
  }
}

class MyPatternTimeoutFunction  extends PatternTimeoutFunction[OrderEvent,(Long,String)]{
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): (Long, String) = {
    val orderId: Long = pattern.get("begin").iterator().next().orderId
    (orderId,"timeout")
  }
}

class MyPatternSelectFunction extends PatternSelectFunction[OrderEvent,(Long,String)]{
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): (Long, String) = {
    val orderId: Long = pattern.get("follow").iterator().next().orderId
    (orderId,"success")
  }
}
