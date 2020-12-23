package CEP

import java.util

import bean.SensorReading
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object TempIncreaseAlertCEP {
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


    val pattern: Pattern[SensorReading, SensorReading] = Pattern
      .begin[SensorReading]("begin")
      .next("next").where { (value, ctx) =>
      val begin: SensorReading = ctx.getEventsForPattern("begin").toIterator.next()
      value.temperature > begin.temperature
    }.within(Time.seconds(5))

    val patternStream: PatternStream[SensorReading] = CEP.pattern(input, pattern)

    val outputTag: OutputTag[String] = new OutputTag[String]("out")

    val output: DataStream[String] = patternStream.select(outputTag, new timeOutFunction, new selectFunction)

    val value: DataStream[String] = output.getSideOutput(outputTag)

    value.print()

    env.execute()

  }
}

class timeOutFunction extends PatternTimeoutFunction[SensorReading,String]{
  override def timeout(pattern: util.Map[String, util.List[SensorReading]], timeoutTimestamp: Long): String = {
    val reading: SensorReading = pattern.get("next").get(0)
    reading.toString
  }
}

class selectFunction extends PatternSelectFunction[SensorReading,String]{
  override def select(pattern: util.Map[String, util.List[SensorReading]]): String = {
    val reading: SensorReading = pattern.get("next").get(0)
    reading.toString
  }
}
