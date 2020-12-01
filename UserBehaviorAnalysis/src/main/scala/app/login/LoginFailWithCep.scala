package app.login

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection._


case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)


object LoginFailWithCep {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginEventStream = env.readTextFile("YOUR_PATH\\resources\\LoginLog.csv")
      .map{ data =>
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      }
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.milliseconds(3000)) {
        override def extractTimestamp(element: LoginEvent): Long ={
          element.eventTime * 1000L
        }
      })

    // 定义匹配模式
    val loginFailPattern = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType=="fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))



    // 在数据流中匹配出定义好的模式
    val patternStream = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)

    // .select方法传入一个 pattern select function，当检测到定义好的模式序列时就会调用
    val loginFailDataStream = patternStream.select{pattern: Map[String, Iterable[LoginEvent]] =>
        val first = pattern.getOrElse("begin", null).iterator.next()
        val second = pattern.getOrElse("next", null).iterator.next()
        (second.userId, second.ip, second.eventType)
      }





    // 将匹配到的符合条件的事件打印出来
    loginFailDataStream.print()
    env.execute("Login Fail Detect Job")
  }
}

