package app.HotItemAnalysis

import bean.{ItemViewCount, UserBehavior}
import org.apache.flink.api._
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector


/**
 * 每隔5分钟输出最近一小时内点击量最多的前N个商品。
 */
object HotItems {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    env.setStateBackend(new FsStateBackend("file:///D:\\IdeaProjects\\myflink\\UserBehaviorAnalysis\\src\\main\\resources\\ckpoint"))
    env.enableCheckpointing(1000*30)   //默认为CheckpointingMode.EXACTLY_ONCE
    //env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(3)
    env.getCheckpointConfig.setCheckpointTimeout(1000*60*3)
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,common.time.Time.seconds(5)))

    val inputStream: DataStream[String] = env.readTextFile("D:\\IdeaProjects\\myflink\\UserBehaviorAnalysis\\src\\main\\resources\\Data\\UserBehavior.csv")

    val streamWithTs: DataStream[UserBehavior] = inputStream.map { data =>
      val strings: Array[String] = data.split(",")
      UserBehavior(strings(0).trim.toLong, strings(1).trim.toLong, strings(2).trim.toInt, strings(3).trim, strings(4).trim.toLong)
    }.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(2)) {
      override def extractTimestamp(element: UserBehavior): Long = element.timestamp * 1000
    }).filter(_.behavior=="pv")

    val value: DataStream[ItemViewCount] = streamWithTs
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      //.process(new HotProcessWindowFunction())
        .aggregate(new CountAgg(),new WindowResultFunction())

    value.keyBy(_.windowEnd).process(new TopNHotItems(5)).print()


    env.execute("HotItems")
  }
}

class CountAgg() extends AggregateFunction[UserBehavior,Int,Int] {
  override def createAccumulator(): Int = 0

  override def add(value: UserBehavior, accumulator: Int): Int = accumulator+1

  override def getResult(accumulator: Int): Int = accumulator

  override def merge(a: Int, b: Int): Int = a+b
}

class WindowResultFunction() extends ProcessWindowFunction[Int,ItemViewCount,Long,TimeWindow] {
  override def process(key: Long, context: Context, elements: Iterable[Int], out: Collector[ItemViewCount]): Unit = {
    val windowEnd: Long = context.window.getEnd
    val cnt: Int = elements.toIterator.next()
    out.collect(ItemViewCount(key,cnt,windowEnd))
  }
}



