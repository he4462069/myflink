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
    //  next():严格近邻
    //  followedBy():宽松近邻
    //  followedByAny()：可重复
    //  notNext() 如果不希望一个事件类型紧接着另一个类型出现。
    //  notFollowedBy() 不希望两个事件之间任何地方出现该事件。
    //  时间约束：pattern.within（Time.seconds(10)）方法定义模式应在10秒内发生。
    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("firstFail").where(_.eventType == "fail").times(2).consecutive()
      //.next("secondFail").where(_.eventType == "fail")
      .within(Time.seconds(2))

    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), pattern)

    patternStream.select(new MySelectFunction()).print()




    env.execute()
  }
}

case class MySelectFunction() extends PatternSelectFunction[LoginEvent,util.List[LoginEvent]] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): util.List[LoginEvent] = {
    val events: util.List[LoginEvent] = map.get("firstFail")
    events
  }
}
