package CEP

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

object LoginFailWithCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginEventStream = env.readTextFile("D:\\IdeaProjects\\myflink\\streamprocessdemo\\src\\main\\resources\\LoginLog.csv")
      .map{ data =>
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      }
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.milliseconds(3000)) {
        override def extractTimestamp(element: LoginEvent): Long ={
          element.eventTime * 1000L
        }
      })

    //定义匹配模式
    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("firstFail").where(_.eventType == "fail")
      .next("secondFail").where(_.eventType == "fail")
      .within(Time.seconds(2))

    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), pattern)

    patternStream.select(new MySelectFunction()).print()




    env.execute()
  }
}

case class MySelectFunction() extends PatternSelectFunction[LoginEvent,(Long,String,String)] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): (Long, String, String) = {
    val event1: LoginEvent = map.get("firstFail").iterator().next()
    val event2: LoginEvent = map.get("secondFail").iterator().next()
    (event1.userId,event2.ip,event1.eventType)
  }
}
