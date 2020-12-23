package app

import java.{lang, util}

import bean.SensorReading
import org.apache.flink.api.common.functions.{CoGroupFunction, JoinFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object StreamJoinDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val inputStream: DataStream[SensorReading] = env.readTextFile("D:\\IdeaProjects\\myflink\\streamprocessdemo\\src\\main\\resources\\sqlDemoCsv.txt")
      .map { data =>
        val strings: Array[String] = data.split(",")
        SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
      }
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(5)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp
      })

    val splited: SplitStream[SensorReading] = inputStream.split(data => if (data.temperature > 40) Seq("high") else Seq("low"))

    val high: DataStream[SensorReading] = splited.select("high")
    val low: DataStream[SensorReading] = splited.select("low")

    //join
    val joinStream: DataStream[String] = high.join(low)
      .where(_.id)
      .equalTo(_.id)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(30)))
      .apply(new MyJoinFunction())


    //Left Outer Join
    high.coGroup(low)
        .where(_.id)
        .equalTo(_.id)
        .window(TumblingEventTimeWindows.of(Time.milliseconds(30)))
        .apply(new MyCoGroupFunction())


    // Full Outer Join,这个也是基于coGroup()实现的



    joinStream.print("join")

    env.execute()
  }
}

class MyJoinFunction() extends JoinFunction[SensorReading,SensorReading,String] {
  override def join(first: SensorReading, second: SensorReading): String = {
    first.id+"  "+first.temperature+"  "+second.temperature
  }
}

class MyCoGroupFunction() extends CoGroupFunction[SensorReading,SensorReading,String] {
  override def coGroup(first: lang.Iterable[SensorReading], second: lang.Iterable[SensorReading], out: Collector[String]): Unit = {
    val leftIter: util.Iterator[SensorReading] = first.iterator()
    val rightIter: util.Iterator[SensorReading] = second.iterator()
    while (leftIter.hasNext) {
      var hadElement = false
      val leftEle: SensorReading = leftIter.next()
      while (rightIter.hasNext) {
        val ele: SensorReading = rightIter.next()
        out.collect(leftEle.id+" "+ele.temperature+" "+ ele.timestamp)
        hadElement = true
      }

      if (!hadElement){
        out.collect(leftEle.id+" "+ "empty")
      }
    }
  }
}
