package app.MarketAnalysis

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


case class MarketingCountView(windowStart: Long, windowEnd: Long, channel: String, behavior: String, count: Long)

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    env.enableCheckpointing(60*1000)
    env.setStateBackend(new FsStateBackend("file:///D:\\IdeaProjects\\myflink\\UserBehaviorAnalysis\\src\\main\\resource\\ckpoint"))
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(3)
    env.getCheckpointConfig.setCheckpointTimeout(30*1000)
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,Time.minutes(2)))

    val stream: DataStream[MarketingUserBehavior] = env.addSource(new SimulatedEventSource).assignAscendingTimestamps(_.timestamp)

    stream.filter(_.behavior != "UNINSTALL")
      .map{data=>
        ((data.channel,data.behavior),1L)
      }
      .keyBy(_._1)
      .timeWindow(streaming.api.windowing.time.Time.seconds(10),streaming.api.windowing.time.Time.seconds(1))
      .process(new MarketingCountByChannel())
      .print()


    env.execute("AppMarketingByChannel")
  }

}

class MarketingCountByChannel() extends ProcessWindowFunction[((String,String),Long),MarketingCountView , (String,String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingCountView]): Unit = {
    val startTs = context.window.getStart
    val endTs = context.window.getEnd
    val channel = key._1
    val behaviorType = key._2
    val count = elements.size
    out.collect( MarketingCountView(startTs, endTs, channel, behaviorType, count) )
  }


 /* private def formatTs (ts: Long) = {
    val df = new SimpleDateFormat ("yyyy/MM/dd-HH:mm:ss")
    df.format (new Date (ts) )
  }*/

}
