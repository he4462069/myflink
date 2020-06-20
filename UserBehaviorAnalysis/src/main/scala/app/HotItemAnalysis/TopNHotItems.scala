package app.HotItemAnalysis

import java.lang
import java.sql.Timestamp

import bean.ItemViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long,ItemViewCount,String] {

  lazy val itemState:ListState[ItemViewCount] = getRuntimeContext.getListState[ItemViewCount](new ListStateDescriptor[ItemViewCount]("itemState",classOf[ItemViewCount]))

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //注册定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd+1)

    itemState.add(value)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allItems: ListBuffer[ItemViewCount] = ListBuffer[ItemViewCount]()

    import scala.collection.JavaConversions._
    for (item <- itemState.get) {
      allItems += item
    }

    // 提前清除状态中的数据，释放空间
    itemState.clear()
    // 按照点击量从大到小排序
    val sortedItems = allItems.sortBy(_.count)(Ordering.Int.reverse).take(topSize)
    // 将排名信息格式化成 String, 便于打印
    val result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

    for(i <- sortedItems.indices){
      val currentItem: ItemViewCount = sortedItems(i)
      // e.g.  No1：  商品ID=12224  浏览量=2413
      result.append("No").append(i+1).append(":")
        .append("  商品ID=").append(currentItem.itemId)
        .append("  浏览量=").append(currentItem.count).append("\n")
    }
    result.append("====================================\n\n")
    // 控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)
    out.collect(result.toString)
  }
}
