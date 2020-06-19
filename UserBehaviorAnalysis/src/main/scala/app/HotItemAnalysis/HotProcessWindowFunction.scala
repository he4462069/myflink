package app.HotItemAnalysis

import bean.{ItemViewCount, UserBehavior}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class HotProcessWindowFunction extends ProcessWindowFunction[UserBehavior,ItemViewCount,Long,TimeWindow] {
  override def process(key: Long, context: Context, elements: Iterable[UserBehavior], out: Collector[ItemViewCount]): Unit = {
    var cnt = 0
    for (elem <- elements) {
      cnt += 1
    }
    out.collect(ItemViewCount(key,cnt,context.window.getEnd))
  }
}
