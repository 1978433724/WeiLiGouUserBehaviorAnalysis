package com.foo

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 实时流量统计模块
  * 分析热门页面
  *
  */
//输入数据格式
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

//输出数据格式
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkTraffic {
  def main(args: Array[String]): Unit = {
    //1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //显示地定义Time的类型,默认是Pro
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置并行度
    env.setParallelism(1)

    val stream = env
      .readTextFile("F:\\ideaWorkspace\\Big practical training\\UserBehaviorAnalysis\\NetworkTrafficAnalysis\\src\\main\\resources\\apache.log")
      .map(line=>{
        val linearray = line.split(" ")
        //定一个时间戳
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")

        val timestamp = simpleDateFormat.parse(linearray(3)).getTime
        ApacheLogEvent(linearray(0),linearray(1),timestamp,linearray(5),(linearray(6)))
      })

    //创建时间戳和水位    10延迟发车
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(10)) {
      override def extractTimestamp(element: ApacheLogEvent): Long = {
        element.eventTime
      }
    })
      .filter(_.method=="GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(1),Time.seconds(5))
      .aggregate(new CountAgg(),new WindowResultFunction())
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))
     stream.print()
    env.execute("Hot Items Job")

  }
  //自定义实现窗口聚合函数
  class CountAgg extends AggregateFunction[ApacheLogEvent,Long,Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a+b
  }

  //自定义实现windowFunction,输出的是ItemViewCount格式
  class WindowResultFunction extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      val url: String = key
      val count = input.iterator.next()

      out.collect(UrlViewCount(url, window.getEnd, count))
    }
  }


  //自定义processFunction,统计访问量最大的url
  class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long ,UrlViewCount, String] {

    //直接定义我们的状态变量,懒加载
    lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("urlState",classOf[UrlViewCount]))
    override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
      //将每条数据保存到状态中
      urlState.add(i)
      //注册定时器,windowEnd + 10
      context.timerService().registerEventTimeTimer(i.windowEnd+10*1000)
    }

    //实现onTimes
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allUrlViews: ListBuffer[UrlViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for(urlView<-urlState.get()){
        allUrlViews += urlView
      }

      //清空state
      urlState.clear()
      //按照访问量排序输出
      val sortedUrlViews = allUrlViews.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
      // 将排名信息格式化成 String, 便于打印
      val result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

      for (i <- sortedUrlViews.indices) {
        val currentItem: UrlViewCount = sortedUrlViews(i)
        // e.g.  No1 ：  URL =/blog/tags/firefox?flav=rss20  流 量 =55
        result.append("No").append(i+1).append(":")
          .append("  URL=").append(currentItem.url)
          .append("  流量=").append(currentItem.count).append("\n")
      }
      result.append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)

    }
  }
}
