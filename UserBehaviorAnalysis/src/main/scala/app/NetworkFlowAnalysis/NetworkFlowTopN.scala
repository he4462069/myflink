package app.NetworkFlowAnalysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import bean.{ApacheLogEvent, UrlViewCount}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 每隔5秒，输出最近10分钟内访问量最多的前N个URL。
 */
object NetworkFlowTopN {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setStateBackend(new FsStateBackend("file:///D:\\IdeaProjects\\myflink\\UserBehaviorAnalysis\\src\\main\\resources\\ckpoint"))
    env.enableCheckpointing(1000*10)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
    env.getCheckpointConfig.setCheckpointTimeout(3*60*1000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(3)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3*60*1000))

    val format: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")

    val inputStream: DataStream[String] = env.readTextFile("D:\\IdeaProjects\\myflink\\UserBehaviorAnalysis\\src\\main\\resources\\Data\\apache.log")


    //为inputstream分配watermark和事件时间
    val streamWithTs: DataStream[ApacheLogEvent] = inputStream.map { data =>
      val strings: Array[String] = data.split(" ")
      val timestamp: Long = format.parse(strings(3).trim).getTime
      ApacheLogEvent(strings(0).trim, strings(2).trim, timestamp, strings(5).trim, strings(6).trim)
    }.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(10)) {
      override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
    })

    //把数据流url聚合并开窗，获取每个窗口各个url的点击量
    val groupedStream: DataStream[UrlViewCount] = streamWithTs
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      //.allowedLateness(Time.seconds(20))
      //.sideOutputLateData(new OutputTag[ApacheLogEvent]("lateData"))
      .aggregate(new AggCount(), new WindowResFunction())

    //对窗口各个url的点击量进行排序
    val result: DataStream[String] = groupedStream.keyBy(_.windowEnd).process(new UrlTopN(3))

    result.print("正常输出")
    //groupedStream.getSideOutput(new OutputTag[ApacheLogEvent]("lateData")).print("迟到的数据")

    env.execute(this.getClass.getSimpleName)
  }
}

//aggregate函数是增量计算的函数
class AggCount() extends AggregateFunction[ApacheLogEvent,Long,Long] {
  override def createAccumulator(): Long = 0

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator+1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a+b
}

class WindowResFunction() extends WindowFunction[Long,UrlViewCount,String,TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key,input.toIterator.next(),window.getEnd))
  }
}

class UrlTopN(topSize: Int) extends KeyedProcessFunction[Long,UrlViewCount,String] {

  //定义一个状态保存每一个UrlViewCount
  lazy val viewCountState:ListState[UrlViewCount] = getRuntimeContext.getListState[UrlViewCount](new ListStateDescriptor[UrlViewCount]("listState",classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {

    ctx.timerService().registerEventTimeTimer(value.windowEnd+1)
    viewCountState.add(value)
  }

  //触发定时器时执行的操作
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    val buffer:ListBuffer[UrlViewCount] = ListBuffer()

    val values: util.Iterator[UrlViewCount] = viewCountState.get().iterator()
    while (values.hasNext) {
      buffer += values.next()
    }
    //清除状态
    viewCountState.clear()

    val sortedItems = buffer.sortWith(_.count>_.count).take(topSize)

    // 将排名信息格式化成 String, 便于打印
    val result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

    for(i <- sortedItems.indices){
      val currentItem: UrlViewCount = sortedItems(i)

      result.append("No").append(i+1).append(":")
        .append("  url=").append(currentItem.url)
        .append("  浏览量=").append(currentItem.count).append("\n")
    }
    result.append("====================================\n\n")
    // 控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)
    out.collect(result.toString)
  }
}
