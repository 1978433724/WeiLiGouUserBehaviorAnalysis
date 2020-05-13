package com.foo
import org.apache.flink.cep.scala.{CEP}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * 支持订单实时监控模块
  * CEP实现
  * 作业:状态编程留给大家的作业
  */

case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)
case class OrderResult(orderId: Long, eventType: String)
object OrderTimeoutWithCEP {
  def main(args: Array[String]): Unit = {
    //1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //显示地定义Time的类型,默认是Pro
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置并行度
    env.setParallelism(1)

    val orderEventStream = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(1, "pay", 1558436842),
      OrderEvent(2, "pay", 1558430844)
    )).assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.orderId)

    // 定义一个带匹配时间窗口的模式
    val orderPayPattern = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .followedBy("follow")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))
    //定义一个输出标签,用于标明侧输出流,因为我想要拿到的最重要的数据
    // 不是匹配成功的数据,更想拿到的是超时的数据
    val orderTimeoutOutput = OutputTag[OrderResult]("orderTimeout")

    //在keyBy之后的流中匹配出定义的pattern stream
    val patternStream = CEP.pattern(orderEventStream.keyBy("orderId"),orderPayPattern)

    import scala.collection.Map

    val completeResultDateStream = patternStream.select(orderTimeoutOutput){
      (pattern: Map[String,Iterable[OrderEvent]], timestamp: Long)=>{
        val timeoutOrderId = pattern.getOrElse("begin",null).iterator.next().orderId
        OrderResult(timeoutOrderId,"timeout")
      }
    }
    {
      //正常匹配的部分,调用pattern select function
       pattern: Map[String, Iterable[OrderEvent]] => {
        val payedOrderId = pattern.getOrElse("follow", null).iterator.next().orderId
        OrderResult(payedOrderId, "success")
      }

    }

    completeResultDateStream.print()

    val timeoutResultDataStream = completeResultDateStream.getSideOutput(orderTimeoutOutput)

    timeoutResultDataStream.print()
    env.execute("Order Timeout Detect Job")
  }
}
