package app.login

import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

/**
 * 同一用户（可以是不同IP）在2秒之内连续2次登录失败，就认为存在恶意登录的风险
 */
object LoginFail {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val streamWithTs: DataStream[LoginEvent] = env.readTextFile("D:\\IdeaProjects\\myflink\\UserBehaviorAnalysis\\src\\main\\resources\\Data\\LoginLog.csv")
      .map { data =>
        val strings: Array[String] = data.split(",")
        LoginEvent(strings(0).toLong, strings(1), strings(2), strings(3).toLong)
      }
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(1)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000
      })

    streamWithTs.keyBy(_.userId)
      .process(new MatchFunction())

  }
}

class MatchFunction extends KeyedProcessFunction[Long,LoginEvent,LoginEvent] {

  lazy val loginState = getRuntimeContext.getListState(new ListStateDescriptor[String]("saved login", classOf[String]))

  //lazy val currentTimerTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("saved login", classOf[Long]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context, out: Collector[LoginEvent]): Unit = {
     if (value.eventType == "fail") {
       loginState.add(value.eventType)

       ctx.timerService().registerEventTimeTimer(value.eventTime*1000+2000)
     }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext, out: Collector[LoginEvent]): Unit = {

  }
}
