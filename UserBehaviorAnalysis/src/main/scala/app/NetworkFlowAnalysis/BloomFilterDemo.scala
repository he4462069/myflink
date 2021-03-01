package app.NetworkFlowAnalysis

import java.nio.charset.Charset

import com.google.common.hash.{BloomFilter, Funnels}

/**
 * 布隆过滤器
 */
object BloomFilterDemo {
  def main(args: Array[String]): Unit = {

    val urls = Array("www.apigo.cn", "www.baidu.com", "www.apigo.cn")


    val bloomFilter: BloomFilter[CharSequence] = BloomFilter.create(Funnels.stringFunnel(),
      10, // 期望处理的元素数量
      0.01)// 期望的误报概率

    urls.foreach{url =>
      if (bloomFilter.mightContain(url)) { // 用重复的 URL
        println("URL 已存在了：" + url)
      } else { // 将 URL 存储在布隆过滤器中
        bloomFilter.put(url)
      }
    }
  }
}
