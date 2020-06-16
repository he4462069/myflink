package app

import bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import source.MySource

/**
 * flink使用eventTime
 */
object EventTimeTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream: DataStream[SensorReading] = env.addSource(new MySource)

    //为stream分配事件时间
    val streamWithEventTime: DataStream[SensorReading] = stream.assignTimestampsAndWatermarks(
      //有界无序时间戳提取器
      new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = {
        element.timestamp
      }
    })

    streamWithEventTime



  }
}
